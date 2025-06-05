mod buffer;
// mod disk_bench;
mod flusher;

use super::Config as DiskConfig;
use super::fd_cache::FdWriterCacheAync;
use super::meta::{TOPIC_META, TopicMeta, WRITER_PTR_FILENAME, gen_record_filename};
use crate::StorageWriter;
use crate::disk::StorageError;
use crate::disk::fd_cache::create_writer_fd;
use crate::disk::meta::WriterPositionPtr;
use crate::disk::writer::flusher::PartitionMetrics;
use anyhow::{Result, anyhow};
use buffer::PartitionWriterBuffer;
use bytes::Bytes;
use dashmap::DashMap;
use flusher::Flusher;
use log::{error, warn};
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::select;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

#[derive(Clone, Debug)]
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
    stop: CancellationToken,

    // NOTE: 会导致所有分区刷盘，慎用
    _flush_sender: mpsc::Sender<()>,
}

impl Drop for DiskStorageWriter {
    fn drop(&mut self) {
        self.stop.cancel();
        println!("xxxxxxxx  DiskStorageWriter Dropped, stop cancel xxxxxxxxxxx");
    }
}

impl DiskStorageWriter {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        cfg.validate()?;
        let fd_cache = Arc::new(FdWriterCacheAync::new(
            cfg.max_size_per_file as usize,
            cfg.fd_cache_size as usize,
        ));

        let stop = CancellationToken::new();
        // 启动刷盘守护任务
        let flusher = Flusher::new(
            stop.clone(),
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
            stop,
            _flush_sender: flush_sender,
        })
    }

    // 单独刷盘某个 topic-partition
    async fn flush_topic_partition_force(&self, topic: &str, partition_id: u32) -> Result<()> {
        let ts = self.get_topic_storage(topic).await?;
        if ts.partitions.get(&partition_id).is_none() {
            return Err(anyhow!(
                StorageError::PartitionNotFound("DiskStorageWriter".to_string()).to_string()
            ));
        }

        self.flusher
            .flush_topic_partition(
                &self
                    .conf
                    .storage_dir
                    .join(topic)
                    .join(partition_id.to_string()),
                true,
            )
            .await
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
        let _stop = self.stop.clone();
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
                    _flusher.add_partition_writer_ptr(writer_ptr_filename, wpp.clone());
                    let buffer_size_half = conf.partition_writer_buffer_size / 2 + 1;
                    let mut datas: Vec<Bytes> = Vec::with_capacity(buffer_size_half);
                    loop {
                        select! {
                            _ = _stop.cancelled() => {
                                warn!("DiskStorageWriter: get_partition_writer receive stop signal, exit");
                                break;
                            }

                            s = rx_partition.recv_many(&mut datas, buffer_size_half) => {
                                let _ = pwb.write_batch(&datas[..s]).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("get_partition_writer PartitionWriterBuffer new err: {e:?}");
                    println!("get_partition_writer PartitionWriterBuffer new err: {e:?}");
                    rx_partition.close();
                }
            };
        });
        ts.partitions.insert(partition_id, tx_partition.clone());
        Ok(tx_partition)
    }

    async fn write_to_partition(&self, topic: &str, partition_id: u32, data: Bytes) -> Result<()> {
        let sender = self.get_partition_writer(topic, partition_id).await?;
        sender.send(data).await?;
        Ok(())
    }
}

// metrics
impl DiskStorageWriter {
    // 添加获取分区指标的方法
    pub fn get_partition_metrics(&self) -> Vec<PartitionMetrics> {
        self.flusher.get_partition_metrics()
    }

    pub async fn reset_metrics(&self) -> Result<()> {
        self.flusher.reset_metrics();
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriter {
    async fn store(&self, topic: &str, partition_id: u32, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Err(anyhow!("can't store empty data"));
        }
        self.write_to_partition(topic, partition_id, data).await?;
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
    use governor::{Quota, RateLimiter};
    use std::{
        num::NonZeroU32,
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        sync::{Barrier, mpsc},
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
            with_metrics: false,
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
    async fn run_flush_benchmark() -> Result<()> {
        struct TestArgs {
            partition_count: u32,      // 分区数量
            message_size: usize,       // 10KB
            warmup_duration: Duration, // 预热时长
            current_rate: usize,       // 起始速率 MB/s
            rate_step: usize,          // 速率步长 (MB/s)
            test_duration: Duration,   // 每个速率级别的测试时长
            max_rate_mbps: usize,      // 降低最大测试速率
        }

        // 测试时修改参数
        let args = vec![
            TestArgs {
                partition_count: 1,
                message_size: 10 * 1024,
                warmup_duration: Duration::from_secs(5),
                current_rate: 50,
                rate_step: 50,
                test_duration: Duration::from_secs(5),
                max_rate_mbps: 300,
            },
            TestArgs {
                partition_count: 10,
                message_size: 10 * 1024,
                warmup_duration: Duration::from_secs(5),
                current_rate: 50,
                rate_step: 50,
                test_duration: Duration::from_secs(5),
                max_rate_mbps: 1000,
            },
            TestArgs {
                partition_count: 30,
                message_size: 10 * 1024,
                warmup_duration: Duration::from_secs(5),
                current_rate: 50,
                rate_step: 50,
                test_duration: Duration::from_secs(5),
                max_rate_mbps: 1000,
            },
            TestArgs {
                partition_count: 50,
                message_size: 10 * 1024,
                warmup_duration: Duration::from_secs(5),
                current_rate: 50,
                rate_step: 50,
                test_duration: Duration::from_secs(5),
                max_rate_mbps: 1000,
            },
            TestArgs {
                partition_count: 100,
                message_size: 10 * 1024,
                warmup_duration: Duration::from_secs(5),
                current_rate: 50,
                rate_step: 50,
                test_duration: Duration::from_secs(5),
                max_rate_mbps: 1000,
            },
        ];

        // 测试不同分区配置
        for arg in args {
            test_flush_speed_with_dynamic_rate_multi_partition(
                arg.partition_count,
                arg.message_size,
                arg.warmup_duration,
                arg.current_rate,
                arg.rate_step,
                arg.test_duration,
                arg.max_rate_mbps,
            )
            .await?;
        }
        Ok(())
    }

    async fn test_flush_speed_with_dynamic_rate_multi_partition(
        partition_count: u32,      // 分区数量
        message_size: usize,       // 10KB
        warmup_duration: Duration, // 预热时长
        current_rate: usize,       // 起始速率 MB/s
        rate_step: usize,          // 速率步长 (MB/s)
        test_duration: Duration,   // 每个速率级别的测试时长
        max_rate_mbps: usize,      // 降低最大测试速率
    ) -> Result<()> {
        use std::sync::atomic::{AtomicU64, Ordering};
        use tokio::time::{Duration, Instant};

        // 准备测试环境
        let mut config = default_config();
        config.storage_dir = PathBuf::from("./flush_bench_data");
        config.fd_cache_size = 10000;
        config.partition_writer_buffer_size = 10000; // 大缓冲区防止阻塞
        config.with_metrics = true;

        let store = Arc::new(DiskStorageWriter::new(config)?);
        // 测试参数
        let topic = format!("flush_speed_test_{}", partition_count);
        // let message_size = 10 * 1024; // 10KB
        // let warmup_duration = Duration::from_secs(5); // 预热时长
        // let max_rate_mbps = 1000; // 降低最大测试速率
        // let rate_step = 50; // 速率步长 (MB/s)
        // let current_rate = 50; // 起始速率 MB/s
        // let test_duration = Duration::from_secs(5); // 每个速率级别的测试时长

        println!(
            "起始速度: {current_rate} MB/s, 步长增速: {rate_step} MB/s, 单步持续时长: {}s, 最大速率: {max_rate_mbps} MB/s",
            test_duration.as_secs()
        );
        // 创建消息内容池
        let message_pool: Vec<Bytes> = (0..26)
            .map(|_| Bytes::from(vec![b'a' + 1; message_size]))
            .collect();
        let message_pool = Arc::new(message_pool);

        // 预热阶段
        println!("[预热] 开始预热 {} 个分区...", partition_count);
        let warmup_start = Instant::now();
        let mut warmup_handles = vec![];

        for partition in 0..partition_count {
            let store = store.clone();
            let message_pool = message_pool.clone();
            let topic = topic.clone();
            warmup_handles.push(tokio::spawn(async move {
                while warmup_start.elapsed() < warmup_duration {
                    let msg =
                        message_pool[rand::random::<u32>() as usize % message_pool.len()].clone();
                    if let Err(e) = store.store(&topic, partition, msg).await {
                        eprintln!("store.store err: {e:?}");
                    }
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
        let mut results = vec![];

        println!("\n{:-^94}", " 开始动态速率测试 (多分区) ");
        println!(
            "|速率(MB/s)|测试时长(s)|发送量(MB)|落盘量(MB)|墙钟耗时(ms)|墙钟吞吐量(MB/s)|平均每分区吞吐量(MB/s)|分区数|",
        );
        println!(
            "|{:-<10}|{:-<11}|{:-<10}|{:-<10}|{:-<12}|{:-<16}|{:-<22}|{:-<6}|",
            "", "", "", "", "", "", "", "",
        );

        for target_rate in (current_rate..=max_rate_mbps).step_by(rate_step) {
            // println!("\n=== 测试速率: {} MB/s ===", target_rate);
            // 系统稳定期
            // println!("等待系统稳定 (5秒)...");
            tokio::time::sleep(Duration::from_secs(5)).await;

            // 重置指标
            store.reset_metrics().await?;

            // 准备测试
            let total_messages_per_second =
                (target_rate as f64 * 1024.0 * 1024.0 / message_size as f64).ceil() as u64;
            let total_messages = total_messages_per_second * test_duration.as_secs();
            let messages_per_partition =
                (total_messages as f64 / partition_count as f64).ceil() as u64;
            // 创建发送任务
            let store_clone = store.clone();
            let message_pool_clone = message_pool.clone();
            let sent_counter = Arc::new(AtomicU64::new(0));
            let mut send_handles = vec![];

            // 获取测试开始前的刷盘指标

            let start_metrics = store.get_partition_metrics();
            // println!("start_metrics = {:?}", start_metrics);

            let barrier = Arc::new(Barrier::new(partition_count as usize + 1));
            // 循环分区写消息
            for partition in 0..partition_count {
                let store = store_clone.clone();
                let message_pool = message_pool_clone.clone();
                let sent_counter = sent_counter.clone();
                let topic = topic.clone();

                // 每个分区独立限流（总吞吐量 = 分区数 × 单分区速率）
                let partition_rate = total_messages_per_second as f64 / partition_count as f64;

                let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
                    NonZeroU32::new(partition_rate.max(1.0).ceil() as u32).unwrap(),
                )));
                let barrier = barrier.clone();
                send_handles.push(tokio::spawn(async move {
                    barrier.wait().await;
                    // 每个分区有自己的发送间隔
                    let start = Instant::now();
                    let mut sent = 0;
                    while sent < messages_per_partition && start.elapsed() < test_duration {
                        // 等待速率许可
                        rate_limiter.until_ready().await;
                        if sent >= messages_per_partition || start.elapsed() >= test_duration {
                            break;
                        }

                        let msg_idx = rand::random::<u32>() as usize % message_pool.len();
                        let msg = message_pool[msg_idx].clone();
                        if let Err(e) = store.store(&topic, partition, msg).await {
                            eprintln!("store.store err: {e:?}");
                            break;
                        }
                        sent_counter.fetch_add(1, Ordering::Relaxed);
                        sent += 1;
                    }
                }));
            }

            let start_time = Instant::now();
            store.reset_metrics().await?;
            barrier.wait().await;
            // 等待测试结束
            futures::future::join_all(send_handles).await;
            let elapsed = start_time.elapsed();

            // 获取测试结束后的刷盘指标
            // println!("等待刷盘完成...");
            // tokio::time::sleep(Duration::from_secs(5)).await; // 确保所有刷盘完成
            // 手动触发一次完整刷盘
            let end_metrics = 'BLOCK_EM: {
                loop {
                    tokio::time::sleep(Duration::from_secs(3)).await; // 确保所有刷盘完成
                    let end_metrics = store.get_partition_metrics();

                    let has_metrics = || -> bool {
                        for em in &end_metrics {
                            if em.flush_bytes.load(Ordering::Relaxed) != 0 {
                                return true;
                            }
                        }
                        false
                    };
                    if !has_metrics() {
                        continue;
                    }
                    break 'BLOCK_EM end_metrics;
                }
            };

            // 计算总指标 - 使用分区路径匹配确保正确
            let sent_count = sent_counter.load(Ordering::Relaxed);
            let sent_bytes = sent_count * message_size as u64;

            let mut total_flushed_bytes = 0;
            let mut total_flush_count = 0;
            let mut total_flush_latency_us = 0;

            // 墙钟时间(wall-clock time)
            let mut min_start_timestamp = u64::MAX;
            let mut max_end_timestamp = 0;

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

                    let start_ts = end_metric.min_start_timestamp.load(Ordering::Relaxed);
                    let end_ts = end_metric.max_end_timestamp.load(Ordering::Relaxed);
                    if start_ts > 0 {
                        min_start_timestamp = min_start_timestamp.min(start_ts);
                    }
                    if end_ts > 0 {
                        max_end_timestamp = max_end_timestamp.max(end_ts);
                    }
                } else {
                    println!(
                        "警告: 找不到分区 {:?} 的起始指标",
                        end_metric.partition_path
                    );
                }
            }

            // 计算性能指标
            let actual_rate = (sent_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
            // NOTE: 单分区平均吞吐量 = 总落盘字节数 / 总的落盘耗时
            // NOTE: 如果公式：总吞吐量 = 单分区平均吞吐量 * 分区数 : 这种计算方式不准确。因为墙钟时间原因: 即1个分区刷盘耗时1s, 10个分区刷盘可能还是1s
            let avg_partition_flush_throughput = (total_flushed_bytes as f64 / 1024.0 / 1024.0)
                / Duration::from_micros(total_flush_latency_us).as_secs_f64();
            let avg_flush_latency = if total_flush_count > 0 {
                // 微秒 转 毫秒
                (total_flush_latency_us as f64 / total_flush_count as f64) / 1000.0
            } else {
                0.0
            };

            // 计算基于墙钟时间的吞吐量（该吞吐量才是真实的数据落盘吞吐量）
            // wall_clock_time_us: 墙钟耗时，单位：微秒
            let wall_clock_time_us = max_end_timestamp.saturating_sub(min_start_timestamp);
            let wall_clock_throughput = if wall_clock_time_us > 0 {
                (total_flushed_bytes as f64 / 1024.0 / 1024.0)
                    / (wall_clock_time_us as f64 / 1_000_000.0)
            } else {
                0.0
            };

            // 存储结果
            results.push((
                target_rate,
                actual_rate,
                wall_clock_throughput,
                avg_flush_latency,
            ));

            // 打印结果
            println!(
                "|{:>10.1}|{:>11.1}|{:>10.1}|{:>10.1}|{:>12.1}|{:>16.1}|{:>22.1}|{:>6}|",
                target_rate as f64,
                test_duration.as_secs_f64(),
                sent_bytes as f64 / 1024.0 / 1024.0,
                total_flushed_bytes as f64 / 1024.0 / 1024.0,
                wall_clock_time_us as f64 / 1_000.0,
                wall_clock_throughput,
                avg_partition_flush_throughput,
                partition_count
            );
        }

        // 分析结果找到瓶颈点
        let mut max_wall_clock_throughput = 0.0;
        let mut optimal_rate = 0.0;

        for (target_rate, _, throughput, _) in &results {
            if *throughput > max_wall_clock_throughput {
                max_wall_clock_throughput = *throughput;
                optimal_rate = *target_rate as f64;
            }
        }

        println!("\n{:-^94}", " 测试结果摘要 ");
        println!("最大落盘吞吐量: {:.2} MB/s", max_wall_clock_throughput);
        println!("最佳发送速率: {:.2} MB/s", optimal_rate);
        println!("分区数量: {}", partition_count);
        println!("消息大小: {} KB", message_size / 1024);

        // 可视化结果
        println!("\n速率与吞吐量关系:");
        for (target_rate, _, wall_clock_throughput, _) in &results {
            let bar_len = (wall_clock_throughput / 50.0).round() as usize;
            let bar = "█".repeat(bar_len);
            println!(
                "{:>5} MB/s: {:.1} MB/s |{}",
                target_rate, wall_clock_throughput, bar
            );
        }

        println!("\n清除测试数据...");
        // 清理测试数据
        tokio::fs::remove_dir_all("./flush_bench_data").await?;

        Ok(())
    }

    #[test]
    fn format_form() {
        println!("\n{:-^94}", " 开始动态速率测试 (多分区) ");
        println!(
            "|速率(MB/s)|测试时长(s)|发送量(MB)|落盘量(MB)|墙钟耗时(ms)|墙钟吞吐量(MB/s)|平均每分区吞吐量(MB/s)|分区数|",
        );
        println!(
            "|{:-<10}|{:-<11}|{:-<10}|{:-<10}|{:-<12}|{:-<16}|{:-<22}|{:-<6}|",
            "", "", "", "", "", "", "", "",
        );
        println!(
            "|{:>10.1}|{:>11.1}|{:>10.1}|{:>10.1}|{:>12.1}|{:>16.1}|{:>22.1}|{:>6}|",
            50.0, 5.0, 250.0, 300.2, 53.1, 60.0, 60.0, 1
        );
        println!("\n{:-^100}", " 测试结果摘要 ");
    }
}
