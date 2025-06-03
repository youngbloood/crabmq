mod buffer;
mod flusher;

use super::Config as DiskConfig;
use super::fd_cache::FdWriterCacheAync;
use super::meta::{
    PartitionWriterPtr, TOPIC_META, TopicMeta, WRITER_PTR_FILENAME, gen_record_filename,
};
use crate::StorageWriter;
use anyhow::{Result, anyhow};
use buffer::PartitionWriterBuffer;
use bytes::Bytes;
use dashmap::DashMap;
use flusher::Flusher;
use log::error;
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

#[derive(Clone)]
struct TopicStorage {
    dir: PathBuf,
    // partition_id -> PartitionWriterBuffer
    // partition_id -> Sender
    partitions: Arc<DashMap<u32, mpsc::Sender<Bytes>>>,
    // 创建分区的互斥锁
    partition_create_locks: Arc<DashMap<u32, Mutex<()>>>,
    meta: TopicMeta,
}

#[derive(Clone)]
pub struct DiskStorageWriter {
    conf: DiskConfig,
    topics: Arc<DashMap<String, TopicStorage>>,
    topic_create_locks: Arc<DashMap<String, Mutex<()>>>,
    fd_cache: Arc<FdWriterCacheAync>,
    flusher: Flusher,

    flush_sender: mpsc::Sender<()>,
}

impl DiskStorageWriter {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        cfg.validate()?;
        let fd_cache = Arc::new(FdWriterCacheAync::new(
            cfg.max_size_per_file as usize,
            cfg.fd_cache_size as usize,
        ));

        // 启动刷盘守护任务
        let flusher = Flusher::new(
            cfg.flusher_partition_writer_buffer_tasks_num,
            cfg.flusher_partition_writer_ptr_tasks_num,
            Duration::from_millis(cfg.flusher_period),
            fd_cache.clone(),
        );
        let mut _flusher = flusher.clone();
        let (flush_sender, flusher_signal) = mpsc::channel(1);
        tokio::spawn(async move { _flusher.run(flusher_signal).await });

        Ok(Self {
            conf: cfg,
            topics: Arc::new(DashMap::new()),
            topic_create_locks: Arc::default(),
            fd_cache,
            flusher,
            flush_sender,
        })
    }

    /// get_topic_storage
    ///
    /// 先从内存 DashMap 中获取是否有该 topic
    ///
    /// 在检查本地存储中是否有 topic
    ///
    /// 都无，初始化 topic
    async fn get_topic_storage(&self, topic: &str) -> Result<TopicStorage> {
        // 第一重检查：快速路径
        if let Some(ts) = self.topics.get(topic) {
            return Ok(ts.clone());
        }

        // 先获取/创建 topic_meta
        let dir = self.conf.storage_dir.join(topic);
        tokio::fs::create_dir_all(&dir).await?;
        let meta_path = dir.join(TOPIC_META);
        let topic_meta = if meta_path.exists() {
            TopicMeta::load(&meta_path).await?
        } else {
            TopicMeta {
                keys: Arc::new(DashMap::new()),
            }
        };

        let topic_lock = self
            .topic_create_locks
            .entry(topic.to_string())
            .or_insert_with(|| Mutex::new(()));
        // 获取或创建topic级别的锁
        let _topic_lock = topic_lock.value().lock().await;

        // 第二重检查：在持有锁后再次检查
        if let Some(ts) = self.topics.get(topic) {
            return Ok(ts.clone());
        }

        let ts = TopicStorage {
            dir: dir.clone(),
            partitions: Arc::new(DashMap::new()),
            meta: topic_meta,
            partition_create_locks: Arc::new(DashMap::new()),
        };
        self.topics.insert(topic.to_string(), ts.clone());
        self.flusher.add_topic_meta(meta_path, ts.meta.clone());
        // 释放锁
        drop(_topic_lock);

        Ok(ts)
    }

    async fn get_partition_writer(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<mpsc::Sender<Bytes>> {
        let ts = self.get_topic_storage(topic).await?;

        // 第一重检查：快速路径
        if let Some(pwb) = ts.partitions.get(&partition_id) {
            return Ok(pwb.value().clone());
        }

        // 获取或创建partition级别的锁
        let partition_lock = ts
            .partition_create_locks
            .entry(partition_id)
            .or_insert_with(|| Mutex::new(()));
        let _partition_lock = partition_lock.value().lock().await;

        // 第二重检查：在持有锁后再次检查
        if let Some(pwb) = ts.partitions.get(&partition_id) {
            return Ok(pwb.value().clone());
        }

        let (tx_partition, mut rx_partition) = mpsc::channel(1);

        let _flusher = self.flusher.clone();
        let conf = self.conf.clone();
        let max_size_per_file = self.conf.max_size_per_file;
        tokio::spawn(async move {
            // 先加载/创建 PartitionWriterPtr
            let dir = ts.dir.join(format!("{}", partition_id));
            if let Err(e) = tokio::fs::create_dir_all(&dir).await {
                error!("get_partition_writer create_dir_all err: {e:?}");
                rx_partition.close();
                return;
            }

            let writer_ptr_filename = dir.join(WRITER_PTR_FILENAME);
            let pwp = if writer_ptr_filename.exists() {
                match PartitionWriterPtr::load(&writer_ptr_filename).await {
                    Ok(pwp) => pwp,
                    Err(e) => {
                        error!("get_partition_writer PartitionWriterPtr err: {e:?}");
                        rx_partition.close();
                        return;
                    }
                }
            } else {
                PartitionWriterPtr::new(writer_ptr_filename.clone())
            };

            match PartitionWriterBuffer::new(
                dir.clone(),
                &conf,
                Arc::new(FdWriterCacheAync::new(max_size_per_file as usize, 2)),
                pwp.get_inner(),
            ) {
                Ok(pwb) => {
                    _flusher.add_partition_writer(dir.clone(), pwb.clone());
                    _flusher.add_partition_meta(writer_ptr_filename, pwp.clone());

                    while let Some(data) = rx_partition.recv().await {
                        let _ = pwb.write(data).await;
                    }
                }
                Err(e) => {
                    error!("get_partition_writer PartitionWriterBuffer new err: {e:?}");
                    rx_partition.close();
                }
            };
        });
        ts.partitions.insert(partition_id, tx_partition.clone());
        Ok(tx_partition)
    }

    pub async fn write_to_partition(
        &self,
        topic: &str,
        partition_id: u32,
        data: Bytes,
    ) -> Result<()> {
        let sender = self.get_partition_writer(topic, partition_id).await?;
        sender.send(data).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriter {
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Err(anyhow!("can't store empty data"));
        }
        self.write_to_partition(topic, partition, data).await?;
        Ok(())
    }
}

fn filename_factor_next_record(filename: &Path) -> PathBuf {
    let filename = PathBuf::from(filename.file_name().unwrap());
    let filename_factor = filename
        .with_extension("")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    PathBuf::from(gen_record_filename(filename_factor + 1))
}

#[cfg(test)]
mod test {
    use super::{DiskConfig, DiskStorageWriter};
    use crate::{StorageWriter as _, disk::default_config};
    use anyhow::Result;
    use bytes::Bytes;
    use futures::future::join_all;
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    use tokio::{sync::mpsc, time};

    fn new_disk_storage() -> DiskStorageWriter {
        DiskStorageWriter::new(DiskConfig {
            storage_dir: PathBuf::from("./data"),
            flusher_period: 50,
            flusher_factor: 1024 * 1024 * 1, // 1M
            max_msg_num_per_file: 4000,
            max_size_per_file: 500,
            compress_type: 0,
            fd_cache_size: 20,
            create_next_record_file_threshold: 80,
            flusher_partition_writer_buffer_tasks_num: 10,
            flusher_partition_writer_ptr_tasks_num: 10,
        })
        .expect("error config")
    }

    #[tokio::test]
    async fn storage_store() -> Result<()> {
        let store = new_disk_storage();
        let datas = vec![
            "Apple",
            "Banana",
            "Cat",
            "Dog",
            "Elephant",
            "Fish",
            "Giraffe",
            "Horse",
            "Igloo",
            "Jaguar",
            "Kangaroo",
            "Lion",
            "Monkey",
            "Nest",
            "Ostrich",
            "Penguin",
            "Queen",
            "Rabbit",
            "Snake",
            "Tiger",
            "Umbrella",
            "Violin",
            "Whale",
            "Xylophone",
            "Yak",
            "Zebra",
        ];
        // for _ in 0..1000 {
        for d in &datas {
            if let Err(e) = store
                .store("topic111", 11, Bytes::copy_from_slice(d.as_bytes()))
                .await
            {
                eprintln!("e = {e:?}");
            }
        }
        // }
        time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    #[tokio::test]
    async fn storage_store_bench_multi_store() -> Result<()> {
        let partition_num = 100;
        let num_messages = 10_000; // 消息总量
        let num_producers = 100;
        let message_size = 10_240;
        let error_count = Arc::new(AtomicU64::new(0));

        let message_pool: Vec<Bytes> = (0..26)
            .map(|i| Bytes::from(vec![b'a' + i; message_size]))
            .collect();
        let message_content = Arc::new(message_pool);
        let mut txs: Vec<mpsc::Sender<Bytes>> = vec![];
        let mut handles = vec![];
        let partition_times = Arc::new(
            (0..partition_num)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>(),
        );

        let start_time = time::Instant::now();
        for p in 1..num_producers + 1 {
            let (tx, mut rx) = mpsc::channel::<Bytes>(10);
            txs.push(tx);
            let _message_content = message_content.clone();
            let _error_count = error_count.clone();
            let _partition_times = partition_times.clone();
            handles.push(tokio::spawn(async move {
                let store = DiskStorageWriter::new(default_config()).expect("error config");
                for i in 0..num_messages {
                    let partition = i % partition_num;
                    let start = time::Instant::now();
                    if let Err(e) = store
                        .store(
                            &format!("mytopic_{}", p),
                            partition,
                            _message_content[i as usize % _message_content.len()].clone(),
                        )
                        .await
                    {
                        _error_count.fetch_add(1, Ordering::Relaxed);
                        eprintln!("e = {e:?}");
                    }
                    let elapsed = start.elapsed().as_micros() as u64;
                    _partition_times[partition as usize].fetch_add(elapsed, Ordering::Relaxed);
                }
            }));
        }

        join_all(handles).await;

        let elapsed = start_time.elapsed();
        let total_messages = num_producers * num_messages;
        let rate = total_messages as f64 / elapsed.as_secs_f64();
        let errors = error_count.load(Ordering::Relaxed);

        println!("\nSummary:");
        println!("Test completed in {:.2?}", elapsed);
        println!("Total messages: {}", total_messages);
        println!("Errors: {}", errors);
        println!("Throughput: {:.2} msg/sec", rate);
        println!(
            "Data rate: {:.2} MB/sec",
            (rate * message_size as f64) / (1024.0 * 1024.0)
        );
        println!("Partition write times (microseconds):");
        for i in 0..partition_num {
            let total_time = partition_times[i as usize].load(Ordering::Relaxed);
            println!("Partition {}: {} us", i, total_time);
        }
        // time::sleep(Duration::from_secs(10)).await;
        Ok(())
    }

    #[tokio::test]
    async fn storage_store_bench_single_store() -> Result<()> {
        // 配置参数 - 根据您的硬件调整
        let partition_num = 100; // 分区数量
        let total_messages = 1_000_000; // 总消息量
        let message_size = 1024; // 每条消息大小(字节)
        let concurrency_level = 10000; // 并发生产者数量
        let warmup_messages = 10_000; // 预热消息量
        let topic_name = "single_bench_topic";

        // 创建存储实例
        let store = Arc::new(DiskStorageWriter::new(default_config())?);

        // 错误计数器
        let error_count = Arc::new(AtomicU64::new(0));
        // 消息计数器
        let msg_counter = Arc::new(AtomicU64::new(0));

        // 创建消息内容池 (避免重复分配内存)
        let message_pool: Vec<Bytes> = (0..26)
            .map(|i| Bytes::from(vec![b'a' + i; message_size]))
            .collect();
        let message_pool = Arc::new(message_pool);

        println!("=== 开始性能测试 ===");
        println!("分区数: {}", partition_num);
        println!("总消息量: {}", total_messages);
        println!("消息大小: {} bytes", message_size);
        println!("并发数: {}", concurrency_level);

        // 预热阶段
        println!("[预热] 发送 {} 条消息...", warmup_messages);
        for _ in 0..warmup_messages {
            let partition = rand::random::<u32>() % partition_num;
            let msg = message_pool[rand::random::<u32>() as usize % message_pool.len()].clone();
            store.store(topic_name.clone(), partition, msg).await?;
        }
        println!("[预热] 完成");

        // 创建生产者任务
        let start_time = time::Instant::now();
        let mut handles = Vec::new();

        for _ in 0..concurrency_level {
            let store = store.clone();
            let message_pool = message_pool.clone();
            let error_count = error_count.clone();
            let msg_counter = msg_counter.clone();

            handles.push(tokio::spawn(async move {
                while msg_counter.fetch_add(1, Ordering::Relaxed) < total_messages {
                    let partition = rand::random::<u32>() % partition_num;
                    let msg_idx = rand::random::<u32>() as usize % message_pool.len();
                    let msg = message_pool[msg_idx].clone();

                    if let Err(e) = store.store(topic_name.clone(), partition, msg).await {
                        error_count.fetch_add(1, Ordering::Relaxed);
                        log::error!("写入错误: {:?}", e);
                    }
                }
            }));
        }

        // 进度监控任务
        let progress_handle = tokio::spawn({
            let msg_counter = msg_counter.clone();
            let start_time = start_time.clone();
            let error_count = error_count.clone();
            async move {
                let mut last_count = 0;
                let mut last_time = time::Instant::now();

                while msg_counter.load(Ordering::Relaxed) < total_messages {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let current_count = msg_counter.load(Ordering::Relaxed);
                    let current_time = time::Instant::now();

                    let delta_count = current_count - last_count;
                    let delta_time = current_time.duration_since(last_time).as_secs_f64();

                    if delta_time > 0.0 {
                        let tps = delta_count as f64 / delta_time;
                        let mbps = (tps * message_size as f64) / (1024.0 * 1024.0);
                        let progress = (current_count as f64 / total_messages as f64) * 100.0;

                        println!(
                            "进度: {:.1}% | TPS: {:.0} | 吞吐量: {:.2} MB/s | 错误: {}",
                            progress,
                            tps,
                            mbps,
                            error_count.load(Ordering::Relaxed)
                        );
                    }

                    last_count = current_count;
                    last_time = current_time;
                }
            }
        });

        // 等待所有任务完成
        futures::future::join_all(handles).await;
        progress_handle.abort(); // 停止进度监控

        // 计算最终结果
        let elapsed = start_time.elapsed();
        let total_sent = msg_counter.load(Ordering::Relaxed).min(total_messages);
        let errors = error_count.load(Ordering::Relaxed);
        let success_rate = (total_sent - errors) as f64 / total_sent as f64 * 100.0;

        let tps = total_sent as f64 / elapsed.as_secs_f64();
        let mbps = (tps * message_size as f64) / (1024.0 * 1024.0);

        println!("\n=== 测试结果 ===");
        println!("总耗时: {:.2?}", elapsed);
        println!("发送消息: {} (目标: {})", total_sent, total_messages);
        println!("错误数: {} (成功率: {:.2}%)", errors, success_rate);
        println!("平均吞吐量: {:.2} 条消息/秒", tps);
        println!("数据速率: {:.2} MB/s", mbps);
        println!("并发数: {}", concurrency_level);

        Ok(())
    }
}
