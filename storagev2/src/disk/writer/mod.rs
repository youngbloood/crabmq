mod buffer;
// mod disk_bench;
mod flusher;

use super::Config as DiskConfig;
use super::meta::{WRITER_PTR_FILENAME, gen_record_filename};
use crate::StorageWriter;
use crate::disk::StorageError;
use crate::disk::meta::WriterPositionPtr;
use crate::disk::writer::flusher::PartitionMetrics;
use anyhow::{Result, anyhow};
use buffer::PartitionWriterBuffer;
use bytes::Bytes;
use dashmap::DashMap;
use flusher::Flusher;
use log::{error, info};
use murmur3::murmur3_32;
use std::io::Cursor;
use std::ops::Deref;
use std::path::Path;
use std::time::Instant;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

// Worker 结构
#[derive(Clone)]
struct Worker {
    tx: mpsc::Sender<(String, u32, Vec<Bytes>)>,
}

impl Deref for Worker {
    type Target = mpsc::Sender<(String, u32, Vec<Bytes>)>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[derive(Clone)]
struct TopicPartitionManager {
    storage_dir: Arc<PathBuf>,
    conf: Arc<DiskConfig>,
    flusher: Arc<Flusher>,
    // (topic, partition_id) -> PartitionWriterBuffer
    partitions: Arc<DashMap<(String, u32), PartitionWriterBuffer>>,
    // 创建分区的互斥锁
    partition_create_locks: Arc<DashMap<u32, Mutex<()>>>,

    last_write_times: Arc<DashMap<(String, u32), Instant>>, // 跟踪最后写入时间

    stop: CancellationToken,
}

impl TopicPartitionManager {
    fn new(
        storage_dir: Arc<PathBuf>,
        conf: Arc<DiskConfig>,
        flusher: Arc<Flusher>,
        stop: CancellationToken,
    ) -> Self {
        let mut manager = Self {
            storage_dir,
            conf,
            flusher,
            partitions: Arc::default(),
            partition_create_locks: Arc::default(),
            last_write_times: Arc::default(),
            stop,
        };

        manager.start_cleanup_task();
        manager
    }

    // 启动后台清理任务
    fn start_cleanup_task(&mut self) {
        let partitions = self.partitions.clone();
        let last_write_times = self.last_write_times.clone();
        let flusher = self.flusher.clone();
        let partition_create_locks = self.partition_create_locks.clone();
        let cleanup_interval = Duration::from_secs(self.conf.partition_cleanup_interval);
        let inactive_threshold = Duration::from_secs(self.conf.partition_inactive_threshold);
        let stop = self.stop.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(cleanup_interval);
            loop {
                select! {
                    _ = stop.cancelled() => {
                        break;
                    }

                    _ = interval.tick() => {
                        // 收集需要清理的分区
                        let mut to_remove = Vec::new();
                        for entry in last_write_times.iter() {
                            let ((topic, pid), last_write) = entry.pair();
                            if Instant::now().duration_since(*last_write) > inactive_threshold {
                                to_remove.push((topic.clone(), *pid));
                            }
                        }

                        // 清理不活跃分区
                        for (topic, pid) in to_remove {
                            // 获取分区锁确保安全
                            let lock = partition_create_locks
                                .entry(pid)
                                .or_insert_with(|| Mutex::new(()));
                            let _guard = lock.value().lock().await;

                            // 双重检查：在持有锁后再次检查活跃性
                            if let Some(last_write) = last_write_times.get(&(topic.clone(), pid)) {
                                if Instant::now().duration_since(*last_write) > inactive_threshold {
                                    // 从各组件中移除分区
                                    let key = (topic.clone(), pid);

                                    // 1. 从分区映射中移除
                                    if let Some((_, pwb)) = partitions.remove(&key) {
                                        // 2. 从刷盘器移除
                                        flusher.remove_partition_writer(&pwb.dir);

                                        // 3. 从时间跟踪器中移除
                                        last_write_times.remove(&key);

                                        info!("Removed inactive partition: {}/{}", topic, pid);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    // 更新最后写入时间
    #[inline]
    fn update_last_write_time(&self, topic: &str, partition_id: u32) {
        self.last_write_times
            .insert((topic.to_string(), partition_id), Instant::now());
    }

    #[inline]
    async fn send(&self, topic: &str, partition_id: u32, datas: &[Bytes]) -> Result<()> {
        self.update_last_write_time(topic, partition_id);
        let pwb = self
            .get_or_create_topic_partition(topic, partition_id)
            .await?;
        pwb.write_batch(datas).await?;
        Ok(())
    }

    #[inline]
    fn get_cached_topic_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Option<PartitionWriterBuffer> {
        let key = (topic.to_string(), partition_id);
        // 第一重检查：快速路径
        if let Some(pwb) = self.partitions.get(&key) {
            return Some(pwb.value().clone());
        }
        None
    }

    async fn get_or_create_topic_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<PartitionWriterBuffer> {
        let key = (topic.to_string(), partition_id);
        // 第一重检查：快速路径
        if let Some(pwb) = self.partitions.get(&key) {
            return Ok(pwb.value().clone());
        }

        // 先加载/创建 PartitionWriterPtr，必须放到 _partition_lock 之外
        let dir = self.storage_dir.join(topic).join(partition_id.to_string());
        tokio::fs::create_dir_all(&dir).await?;

        // 加载写指针文件
        let writer_ptr_filename = dir.join(WRITER_PTR_FILENAME);
        let wpp = if writer_ptr_filename.exists() {
            WriterPositionPtr::load(&writer_ptr_filename).await?
        } else {
            WriterPositionPtr::new(
                writer_ptr_filename.clone(),
                dir.join(gen_record_filename(0)),
            )?
        };

        // 加载
        let wpp = Arc::new(wpp);
        let pwb = PartitionWriterBuffer::new(
            dir.clone(),
            self.conf.clone(),
            wpp.clone(),
            self.flusher.clone(),
        )
        .await?;

        // 获取或创建partition级别的锁
        let topic_partition_lock = self
            .partition_create_locks
            .entry(partition_id)
            .or_insert_with(|| Mutex::new(()));
        let _topic_partition_lock = topic_partition_lock.value().lock().await;
        // 第二重检查：在持有锁后再次检查
        if let Some(pwb) = self.partitions.get(&key) {
            return Ok(pwb.value().clone());
        }
        self.flusher.add_partition_writer(dir.clone(), pwb.clone());
        self.flusher
            .add_partition_writer_ptr(dir.clone(), wpp.clone());
        self.partitions.insert(key, pwb.clone());
        Ok(pwb)
    }
}

#[derive(Clone)]
pub struct DiskStorageWriter {
    conf: Arc<DiskConfig>,
    topic_partition_manager: TopicPartitionManager,

    flusher: Arc<Flusher>,
    // NOTE: 会导致所有分区刷盘，慎用
    _flush_sender: mpsc::Sender<()>,

    workers: Arc<DashMap<usize, Worker>>, // worker 池
    worker_locks: Arc<DashMap<usize, Mutex<()>>>,

    stop: CancellationToken,
}

impl Drop for DiskStorageWriter {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl DiskStorageWriter {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        cfg.validate()?;
        let cfg = Arc::new(cfg);

        let stop = CancellationToken::new();
        // 启动刷盘守护任务
        let flusher = Arc::new(Flusher::new(
            stop.clone(),
            cfg.flusher_partition_writer_buffer_tasks_num,
            cfg.flusher_partition_writer_ptr_tasks_num,
            Duration::from_millis(cfg.flusher_period),
        ));
        let mut _flusher = flusher.clone();
        let (flush_sender, flusher_signal) = mpsc::channel(1);
        tokio::spawn(async move { _flusher.run(flusher_signal).await });

        let worker_tasks_num = cfg.worker_tasks_num;
        let dsw = Self {
            topic_partition_manager: TopicPartitionManager::new(
                Arc::new(cfg.storage_dir.clone()),
                cfg.clone(),
                flusher.clone(),
                stop.clone(),
            ),
            flusher,
            stop,
            _flush_sender: flush_sender,
            workers: Arc::new(DashMap::new()),
            worker_locks: Arc::default(),
            conf: cfg,
        };

        for work_id in 0..worker_tasks_num {
            let _stop = dsw.stop.clone();
            let (tx_worker, mut rx_worker) = mpsc::channel::<(String, u32, Vec<Bytes>)>(100);
            let tpm = dsw.topic_partition_manager.clone();
            tokio::spawn(async move {
                loop {
                    if rx_worker.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            break;
                        }

                        // TODO: recv_many()
                        res = rx_worker.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (topic, partition_id, datas) = res.unwrap();
                            if let Err(e) = tpm.send(&topic, partition_id, &datas).await{
                                error!("send data to topic[{topic}]-partition[{partition_id}] err: {e:?}");
                            }
                        }
                    }
                }
            });
            dsw.workers.insert(work_id, Worker { tx: tx_worker });
        }

        Ok(dsw)
    }

    // 单独刷盘某个 topic-partition
    async fn flush_topic_partition_force(&self, topic: &str, partition_id: u32) -> Result<()> {
        if self
            .topic_partition_manager
            .get_cached_topic_partition(topic, partition_id)
            .is_none()
        {
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

    #[inline]
    async fn get_worker(&self, worker_id: usize) -> Worker {
        if let Some(worker) = self.workers.get(&worker_id) {
            return worker.value().clone();
        }
        self.workers.iter().find(|_| true).unwrap().value().clone()
    }

    async fn write_to_worker(&self, topic: &str, partition_id: u32, datas: &[Bytes]) -> Result<()> {
        let worker_id =
            partition_to_worker(topic, partition_id, self.workers.len() as _).unwrap_or(0);
        let worker = self.get_worker(worker_id as _).await;
        worker
            .send((topic.to_string(), partition_id, datas.to_vec()))
            .await?;
        Ok(())
    }

    #[inline]
    fn get_cached_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Option<PartitionWriterBuffer> {
        self.topic_partition_manager
            .get_cached_topic_partition(topic, partition_id)
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
    async fn store(&self, topic: &str, partition_id: u32, datas: &[Bytes]) -> Result<()> {
        if datas.is_empty() {
            return Err(anyhow!("can't store empty data"));
        }
        if let Some(pwb) = self.get_cached_partition(topic, partition_id) {
            pwb.write_batch(datas).await?;
            self.topic_partition_manager
                .update_last_write_time(topic, partition_id);
        } else {
            self.write_to_worker(topic, partition_id, datas).await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct DiskStorageWriterWrapper {
    inner: Arc<DiskStorageWriter>,
}

impl Deref for DiskStorageWriterWrapper {
    type Target = Arc<DiskStorageWriter>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DiskStorageWriterWrapper {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        let dsw = DiskStorageWriter::new(cfg)?;
        Ok(Self {
            inner: Arc::new(dsw),
        })
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriterWrapper {
    async fn store(&self, topic: &str, partition_id: u32, datas: &[Bytes]) -> Result<()> {
        self.inner.store(topic, partition_id, datas).await
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

fn partition_to_worker(topic: &str, partition_id: u32, worker_num: u32) -> Result<u32> {
    let hash = murmur3_32(&mut Cursor::new(format!("{}{}", topic, partition_id)), 0)?; // 种子为 0，与 Kafka 一致
    Ok((hash % worker_num) as u32)
}

#[cfg(test)]
mod test {
    use super::{DiskConfig, DiskStorageWriter};
    use crate::StorageWriter as _;
    use anyhow::Result;
    use bytes::Bytes;
    use std::{path::PathBuf, time::Duration};
    use tokio::time;

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
            partition_writer_prealloc: false,
            worker_tasks_num: 100,
            partition_cleanup_interval: 150,
            partition_inactive_threshold: 300,
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
                .store("topic111", 11, &[Bytes::copy_from_slice(d.as_bytes())])
                .await
            {
                eprintln!("e = {e:?}");
            }
        }
        // }
        time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }
}
