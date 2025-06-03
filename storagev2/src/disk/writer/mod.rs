mod buffer;
// mod disk_bench;
mod flusher;

use super::Config as DiskConfig;
use super::fd_cache::FdWriterCacheAync;
use super::meta::{TOPIC_META, TopicMeta, WRITER_PTR_FILENAME, gen_record_filename};
use crate::StorageWriter;
use crate::disk::fd_cache::create_writer_fd;
use crate::disk::meta::WriterPositionPtr;
use crate::disk::writer::flusher::PartitionMetrics;
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
            let fd = create_writer_fd(&meta_path)?;
            TopicMeta::with(fd)
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

        let (tx_partition, mut rx_partition) =
            mpsc::channel(self.conf.partition_writer_buffer_size);

        let _flusher = self.flusher.clone();
        let conf = self.conf.clone();
        let max_size_per_file = self.conf.max_size_per_file;
        let _fd_cache = self.fd_cache.clone();
        tokio::spawn(async move {
            // 先加载/创建 PartitionWriterPtr
            let dir = ts.dir.join(format!("{}", partition_id));
            if let Err(e) = tokio::fs::create_dir_all(&dir).await {
                error!("get_partition_writer create_dir_all err: {e:?}");
                rx_partition.close();
                return;
            }

            // 加载写指针文件
            let writer_ptr_filename = dir.join(WRITER_PTR_FILENAME);
            let wpp = if writer_ptr_filename.exists() {
                match WriterPositionPtr::load(&writer_ptr_filename).await {
                    Ok(pwp) => pwp,
                    Err(e) => {
                        error!("get_partition_writer WriterPositionPtr load err: {e:?}");
                        rx_partition.close();
                        return;
                    }
                }
            } else {
                match WriterPositionPtr::new(
                    writer_ptr_filename.clone(),
                    dir.join(gen_record_filename(0)),
                ) {
                    Ok(ptr) => ptr,
                    Err(e) => {
                        error!("get_partition_writer WriterPositionPtr new err: {e:?}");
                        rx_partition.close();
                        return;
                    }
                }
            };

            // 加载
            let wpp = Arc::new(wpp);
            match PartitionWriterBuffer::new(dir.clone(), &conf, wpp.clone(), _flusher.clone())
                .await
            {
                Ok(pwb) => {
                    _flusher.add_partition_writer(dir.clone(), pwb.clone());
                    _flusher.add_partition_meta(writer_ptr_filename, wpp.clone());
                    let buffer_size_half = conf.partition_writer_buffer_size / 2 + 1;
                    let mut datas: Vec<Bytes> = Vec::with_capacity(buffer_size_half);
                    loop {
                        let s = rx_partition.recv_many(&mut datas, buffer_size_half).await;
                        let _ = pwb.write_batch(&datas[..s]).await;
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

    // 添加获取分区指标的方法
    pub fn get_partition_metrics(&self) -> Vec<PartitionMetrics> {
        self.flusher.get_partition_metrics()
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
    use crate::{
        StorageWriter as _,
        disk::{default_config, writer::flusher::PartitionMetrics},
    };
    use anyhow::Result;
    use bytes::Bytes;
    use defer::defer;
    use futures::future::join_all;
    use serde::Serialize;
    use std::{
        cell::RefCell,
        fs::File,
        io::Write,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        sync::{RwLock, mpsc},
        time,
    };

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
            partition_writer_buffer_size: 100,
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

    #[derive(Debug, Serialize)]
    struct BenchResult {
        elapsed: f32,        // 总耗时(s)
        total_sent: u64,     // 实际发送消息量
        total_messages: u64, // 目标发送消息量
        errors: u64,         // 错误数
        success_rate: f64,   // 成功率
        tps: f64,
        mbps: f64,
        partition_num: u32,     // 分区数
        concurrency_level: i32, // 并发数
    }

    async fn storage_store_bench_single_store(
        topic_name: String,
        partition_num: u32,     // 分区数量
        total_messages: u64,    // 总消息量
        message_size: usize,    // 每条消息大小(字节)
        concurrency_level: i32, // 并发生产者数量
        warmup_messages: i32,   // 预热消息量
        skip_print_result: bool,
    ) -> Result<BenchResult> {
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
            let seed = rand::random::<u32>();
            let partition = seed % partition_num;
            let msg = message_pool[seed as usize % message_pool.len()].clone();
            store.store(&topic_name, partition, msg).await?;
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
            let topic_name = topic_name.clone();
            handles.push(tokio::spawn(async move {
                while msg_counter.fetch_add(1, Ordering::Relaxed) < total_messages {
                    let seed: u32 = rand::random::<u32>();
                    let partition = seed % partition_num;
                    let msg_idx = seed as usize % message_pool.len();
                    let msg = message_pool[msg_idx].clone();

                    if let Err(e) = store.store(&topic_name, partition, msg).await {
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
                            "进度: {:0>4.1}% | TPS: {:0>5.0} | 吞吐量: {:0>5.2} MB/s | 错误: {}",
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

        if !skip_print_result {
            println!("\n=== 测试结果 ===");
            println!("总耗时: {:.2?}", elapsed);
            println!("发送消息: {} (目标: {})", total_sent, total_messages);
            println!("错误数: {} (成功率: {:.2}%)", errors, success_rate);
            println!("平均吞吐量: {:.2} 条消息/秒", tps);
            println!("数据速率: {:.2} MB/s", mbps);
            println!("分区数: {}", partition_num);
            println!("并发数: {}", concurrency_level);
        }

        Ok(BenchResult {
            elapsed: elapsed.as_secs_f32(),
            total_sent,
            total_messages,
            errors,
            success_rate,
            tps,
            mbps,
            partition_num,
            concurrency_level,
        })
    }

    #[tokio::test]
    async fn storage_store_bench_single_store_count() -> Result<()> {
        let args = vec![
            // 测试次数，partition_num，total_messages，message_size，concurrency_level，warmup_messages
            (3_u64, 100, 1_000_000, 1024, 100, 10_000),
            (3_u64, 100, 1_000_000, 1024, 200, 10_000),
            (3_u64, 100, 1_000_000, 1024, 300, 10_000),
            (3_u64, 100, 1_000_000, 1024, 400, 10_000),
            (3_u64, 100, 1_000_000, 1024, 500, 10_000),
            (3_u64, 100, 1_000_000, 1024, 600, 10_000),
            (3_u64, 100, 1_000_000, 1024, 700, 10_000),
            (3_u64, 100, 1_000_000, 1024, 800, 10_000),
            (3_u64, 100, 1_000_000, 1024, 900, 10_000),
            (3_u64, 100, 1_000_000, 1024, 1000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 2000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 3000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 4000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 5000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 6000, 10_000),
            (3_u64, 100, 1_000_000, 1024, 7000, 10_000),
        ];

        let all_result: Arc<RefCell<Vec<BenchResult>>> = Arc::default();

        let _all_result = all_result.clone();
        defer::defer!({
            println!("\n=== 最终基准测试结果 ===");
            for r in _all_result.borrow().iter() {
                println!("\n=== 基准测试结果 ===");
                println!("总耗时: {:.2?}", r.elapsed);
                println!("发送消息: {} (目标: {})", r.total_sent, r.total_messages);
                println!("错误数: {} (成功率: {:.2}%)", r.errors, r.success_rate);
                println!("平均吞吐量: {:.2} 条消息/秒", r.tps);
                println!("数据速率: {:.2} MB/s", r.mbps);
                println!("分区数: {}", r.partition_num);
                println!("并发数: {}", r.concurrency_level);
            }
        });

        // let args = vec![
        //     // 测试次数，partition_num，total_messages，message_size，concurrency_level，warmup_messages
        //     (3_u64, 90, 1_000_000, 1024, 100, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 300, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 500, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 700, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 900, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 1000, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 3000, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 5000, 10_000),
        //     (3_u64, 90, 1_000_000, 1024, 7000, 10_000),
        //     //
        //     (3_u64, 70, 1_000_000, 1024, 100, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 300, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 500, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 700, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 900, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 1000, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 3000, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 5000, 10_000),
        //     (3_u64, 70, 1_000_000, 1024, 7000, 10_000),
        //     //
        //     (3_u64, 50, 1_000_000, 1024, 100, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 300, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 500, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 700, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 900, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 1000, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 3000, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 5000, 10_000),
        //     (3_u64, 50, 1_000_000, 1024, 7000, 10_000),
        //     //
        //     (3_u64, 30, 1_000_000, 1024, 100, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 300, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 500, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 700, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 900, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 1000, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 3000, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 5000, 10_000),
        //     (3_u64, 30, 1_000_000, 1024, 7000, 10_000),
        //     //
        //     (3_u64, 10, 1_000_000, 1024, 100, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 300, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 500, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 700, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 900, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 1000, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 3000, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 5000, 10_000),
        //     (3_u64, 10, 1_000_000, 1024, 7000, 10_000),
        // ];

        let ts = chrono::Local::now().timestamp();
        let mut fd = File::options()
            .append(true)
            .create(true)
            .write(true)
            .open(format!("bench_store_{}", ts))?;
        for arg in args {
            let mut result_p = vec![];

            for i in 0..arg.0 {
                result_p.push(
                    storage_store_bench_single_store(
                        format!("store_single_bench_{}_{}_{}", i, arg.1, arg.4),
                        arg.1,
                        arg.2,
                        arg.3,
                        arg.4,
                        arg.5,
                        false,
                    )
                    .await?,
                );
            }

            let elapsed: f32 = result_p.iter().map(|v| v.elapsed).sum();
            let total_sent: u64 = result_p.iter().map(|v| v.total_sent).sum();
            let total_messages: u64 = result_p.iter().map(|v| v.total_messages).sum();
            let errors: u64 = result_p.iter().map(|v| v.errors).sum();
            let success_rate: f64 = result_p.iter().map(|v| v.success_rate).sum();
            let tps: f64 = result_p.iter().map(|v| v.tps).sum();
            let mbps: f64 = result_p.iter().map(|v| v.mbps).sum();

            let rst = BenchResult {
                elapsed: elapsed / arg.0 as f32,
                total_sent: total_sent / arg.0,
                total_messages: total_messages / arg.0,
                errors: errors / arg.0,
                success_rate: success_rate / arg.0 as f64,
                tps: tps / arg.0 as f64,
                mbps: mbps / arg.0 as f64,
                partition_num: arg.1,
                concurrency_level: arg.4,
            };

            let json_data = serde_json::to_string_pretty(&rst)?;
            fd.write_all(json_data.as_bytes())?;

            all_result.borrow_mut().push(rst);
        }

        // println!("\n=== 最终测试结果 ===");
        // for r in all_result {
        //     println!("\n=== 测试结果 ===");
        //     println!("总耗时: {:.2?}", r.elapsed);
        //     println!("发送消息: {} (目标: {})", r.total_sent, r.total_messages);
        //     println!("错误数: {} (成功率: {:.2}%)", r.errors, r.success_rate);
        //     println!("平均吞吐量: {:.2} 条消息/秒", r.tps);
        //     println!("数据速率: {:.2} MB/s", r.mbps);
        //     println!("并发数: {}", r.concurrency_level);
        // }

        Ok(())
    }

    #[tokio::test]
    async fn run_flush_benchmark() -> Result<()> {
        // 测试不同分区配置
        for partition_count in &[20, 40, 70, 100] {
            test_flush_speed_with_dynamic_rate_multi_partition(*partition_count).await?;
        }
        Ok(())
    }

    async fn test_flush_speed_with_dynamic_rate_multi_partition(
        partition_count: u32,
    ) -> Result<()> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::time::{self, Duration, Instant};

        // 准备测试环境
        let config = DiskConfig {
            storage_dir: PathBuf::from("./flush_bench_data"),
            flusher_period: 50,
            flusher_factor: 1024 * 1024 * 1, // 1MB
            max_msg_num_per_file: 4000,
            max_size_per_file: 1024 * 1024 * 100, // 100MB
            compress_type: 0,
            fd_cache_size: 300,
            create_next_record_file_threshold: 80,
            flusher_partition_writer_buffer_tasks_num: 10,
            flusher_partition_writer_ptr_tasks_num: 10,
            partition_writer_buffer_size: 10000, // 大缓冲区防止阻塞
        };

        let store = Arc::new(DiskStorageWriter::new(config)?);

        // 测试参数
        let topic = "flush_speed_test";
        let message_size = 10 * 1024; // 10KB
        let warmup_duration = Duration::from_secs(5);
        let test_duration = Duration::from_secs(3); // 每个速率级别的测试时长
        let max_rate_mbps = 1000; // 降低最大测试速率
        let rate_step = 100; // 速率步长 (MB/s)

        // 创建消息内容池
        let message_pool: Vec<Bytes> = (0..100)
            .map(|_| Bytes::from(vec![b'a'; message_size]))
            .collect();
        let message_pool = Arc::new(message_pool);

        // 预热阶段
        println!("[预热] 开始预热 {} 个分区...", partition_count);
        let warmup_start = Instant::now();
        let mut warmup_handles = vec![];

        for partition in 0..partition_count {
            let store = store.clone();
            let message_pool = message_pool.clone();
            warmup_handles.push(tokio::spawn(async move {
                while warmup_start.elapsed() < warmup_duration {
                    let msg =
                        message_pool[rand::random::<u32>() as usize % message_pool.len()].clone();
                    let _ = store.store(topic, partition, msg).await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }));
        }

        futures::future::join_all(warmup_handles).await;
        println!("[预热] 完成");

        // 获取初始刷盘指标 - 确保分区已初始化
        println!("等待分区初始化...");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut initial_metrics = store.get_partition_metrics();
        println!("初始指标数量: {}", initial_metrics.len());

        // 如果指标数量不足，尝试重新获取
        if initial_metrics.len() < partition_count as usize {
            println!("检测到分区指标缺失，重新获取...");
            tokio::time::sleep(Duration::from_secs(1)).await;
            initial_metrics = store.get_partition_metrics();
        }

        // 确保所有分区都有指标
        if initial_metrics.len() != partition_count as usize {
            println!(
                "警告: 初始化分区指标数量 ({}) 不等于分区数量 ({})",
                initial_metrics.len(),
                partition_count
            );
        }

        // 速率测试控制
        let mut current_rate = 100; // 起始速率 MB/s
        let mut results = vec![];

        println!("\n{:-^120}", " 开始动态速率测试 (多分区) ");
        println!(
            "| {:^10} | {:^12} | {:^15} | {:^15} | {:^15} | {:^15} |",
            "速率(MB/s)", "发送量(MB)", "落盘量(MB)", "平均延迟(ms)", "吞吐量(MB/s)", "分区数"
        );
        println!(
            "|{:-<12}|{:-<14}|{:-<17}|{:-<17}|{:-<17}|{:-<17}|",
            "", "", "", "", "", ""
        );

        while current_rate <= max_rate_mbps {
            // 准备测试
            let target_rate = current_rate;
            let messages_per_second = (target_rate * 1024 * 1024) / message_size;
            let interval = Duration::from_secs_f64(1.0 / messages_per_second as f64);

            // 创建发送任务
            let store_clone = store.clone();
            let message_pool_clone = message_pool.clone();
            let sent_counter = Arc::new(AtomicU64::new(0));

            let mut send_handles = vec![];
            for partition in 0..partition_count {
                let store = store_clone.clone();
                let message_pool = message_pool_clone.clone();
                let sent_counter = sent_counter.clone();
                send_handles.push(tokio::spawn({
                    async move {
                        let start = Instant::now();
                        while start.elapsed() < test_duration {
                            let msg_idx = rand::random::<u32>() as usize % message_pool.len();
                            let msg = message_pool[msg_idx].clone();
                            if store.store(topic, partition, msg).await.is_ok() {
                                sent_counter.fetch_add(1, Ordering::Relaxed);
                            }
                            tokio::time::sleep(interval).await;
                        }
                    }
                }));
            }

            // 获取测试开始前的刷盘指标
            let start_time = Instant::now();
            let start_metrics = store.get_partition_metrics();

            // 打印分区指标状态
            // println!("开始指标状态:");
            // for metric in &start_metrics {
            //     println!(
            //         "分区 {:?}: 刷盘字节={}",
            //         metric.partition_path,
            //         metric.flush_bytes.load(Ordering::Relaxed)
            //     );
            // }

            // 等待测试结束
            futures::future::join_all(send_handles).await;
            let elapsed = start_time.elapsed();

            // 获取测试结束后的刷盘指标
            tokio::time::sleep(Duration::from_secs(1)).await; // 确保所有刷盘完成
            let end_metrics = store.get_partition_metrics();

            // 打印分区指标状态
            // println!("结束指标状态:");
            // for metric in &end_metrics {
            //     println!(
            //         "分区 {:?}: 刷盘字节={}",
            //         metric.partition_path,
            //         metric.flush_bytes.load(Ordering::Relaxed)
            //     );
            // }

            // 计算总指标 - 使用分区路径匹配确保正确
            let sent_count = sent_counter.load(Ordering::Relaxed);
            let sent_bytes = sent_count * message_size as u64;

            let mut total_flushed_bytes = 0;
            let mut total_flush_count = 0;
            let mut total_flush_latency_us = 0;

            for end_metric in &end_metrics {
                if let Some(start_metric) = start_metrics
                    .iter()
                    .find(|m| m.partition_path == end_metric.partition_path)
                {
                    let flushed_bytes = end_metric.flush_bytes.load(Ordering::Relaxed)
                        - start_metric.flush_bytes.load(Ordering::Relaxed);
                    total_flushed_bytes += flushed_bytes;

                    let flush_count = end_metric.flush_count.load(Ordering::Relaxed)
                        - start_metric.flush_count.load(Ordering::Relaxed);
                    total_flush_count += flush_count;

                    let flush_latency_us = end_metric.flush_latency_us.load(Ordering::Relaxed)
                        - start_metric.flush_latency_us.load(Ordering::Relaxed);
                    total_flush_latency_us += flush_latency_us;

                    // println!(
                    //     "分区 {:?}: 刷盘量={}字节",
                    //     end_metric.partition_path, flushed_bytes
                    // );
                } else {
                    println!(
                        "警告: 找不到分区 {:?} 的起始指标",
                        end_metric.partition_path
                    );
                }
            }

            // 计算性能指标
            let actual_rate = (sent_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
            let flush_throughput =
                (total_flushed_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
            let avg_flush_latency = if total_flush_count > 0 {
                (total_flush_latency_us as f64 / total_flush_count as f64) / 1000.0
            } else {
                0.0
            };

            // 存储结果
            results.push((
                target_rate,
                actual_rate,
                flush_throughput,
                avg_flush_latency,
            ));

            // 打印结果
            println!(
                "| {:>10.1} | {:>12.1} | {:>15.1} | {:>15.1} | {:>15.1} | {:>15} |",
                target_rate as f64,
                sent_bytes as f64 / 1024.0 / 1024.0,
                total_flushed_bytes as f64 / 1024.0 / 1024.0,
                avg_flush_latency,
                flush_throughput,
                partition_count
            );

            // 增加发送速率继续测试
            current_rate += rate_step;
        }

        // 分析结果找到瓶颈点
        let mut max_throughput = 0.0;
        let mut optimal_rate = 0.0;

        for (target_rate, _, throughput, _) in &results {
            if *throughput > max_throughput {
                max_throughput = *throughput;
                optimal_rate = *target_rate as f64;
            }
        }

        println!("\n{:-^120}", " 测试结果摘要 ");
        println!("最大落盘吞吐量: {:.2} MB/s", max_throughput);
        println!("最佳发送速率: {:.2} MB/s", optimal_rate);
        println!("分区数量: {}", partition_count);
        println!("消息大小: {} KB", message_size / 1024);

        // 可视化结果
        println!("\n速率与吞吐量关系:");
        for (target_rate, _, throughput, _) in &results {
            let bar_len = (throughput / 50.0).round() as usize;
            let bar = "█".repeat(bar_len);
            println!("{:>5} MB/s: {:.1} MB/s |{}", target_rate, throughput, bar);
        }

        // 清理测试数据
        tokio::fs::remove_dir_all("./flush_bench_data").await?;

        Ok(())
    }
}
