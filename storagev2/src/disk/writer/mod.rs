mod buffer;
// mod disk_bench;
mod flusher;

use super::Config as DiskConfig;
use super::fd_cache::FdWriterCacheAync;
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
use log::error;
use murmur3::murmur3_32;
use std::io::Cursor;
use std::ops::Deref;
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::select;
use tokio::sync::{Mutex, mpsc};
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
    dir: Arc<PathBuf>,
    conf: Arc<DiskConfig>,
    flusher: Arc<Flusher>,
    // (topic, partition_id) -> PartitionWriterBuffer
    partitions: Arc<DashMap<(String, u32), PartitionWriterBuffer>>,
    // 创建分区的互斥锁
    partition_create_locks: Arc<DashMap<u32, Mutex<()>>>,
}

impl TopicPartitionManager {
    fn new(dir: Arc<PathBuf>, conf: Arc<DiskConfig>, flusher: Arc<Flusher>) -> Self {
        Self {
            dir,
            conf,
            flusher,
            partitions: Arc::default(),
            partition_create_locks: Arc::default(),
        }
    }

    #[inline]
    async fn send(&self, topic: &str, partition_id: u32, datas: &[Bytes]) -> Result<()> {
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
        let dir = self.dir.join(topic).join(partition_id.to_string());
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
        let fd_cache = Arc::new(FdWriterCacheAync::new(
            cfg.max_size_per_file as usize,
            cfg.fd_cache_size as usize,
        ));
        let cfg = Arc::new(cfg);

        let stop = CancellationToken::new();
        // 启动刷盘守护任务
        let flusher = Arc::new(Flusher::new(
            stop.clone(),
            cfg.flusher_partition_writer_buffer_tasks_num,
            cfg.flusher_partition_writer_ptr_tasks_num,
            Duration::from_millis(cfg.flusher_period),
            fd_cache.clone(),
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
        } else {
            self.write_to_worker(topic, partition_id, datas).await?;
        }

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
