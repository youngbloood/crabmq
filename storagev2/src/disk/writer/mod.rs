mod buffer;
// mod disk_bench;
mod flusher;

use super::Config as DiskConfig;
use super::meta::{WRITER_PTR_FILENAME, gen_record_filename};
use crate::disk::PartitionIndexManager;
use crate::disk::meta::WriterPositionPtr;
use crate::disk::writer::buffer::PartitionBufferSet;
use crate::metrics::StorageWriterMetrics;
use crate::{MessagePayload, StorageError, StorageResult, StorageWriter};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use flusher::Flusher;
use log::{error, info};
use murmur3::murmur3_32;
use std::io::Cursor;
use std::ops::Deref;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

// 定义批次任务结构
struct WriteTask {
    topic: String,
    partition_id: u32,
    payloads: Vec<MessagePayload>,
    notify: Option<oneshot::Sender<StorageResult<()>>>,
}

// Worker 结构
#[derive(Clone)]
struct Worker {
    tx: mpsc::Sender<WriteTask>,
}

impl Deref for Worker {
    type Target = mpsc::Sender<WriteTask>;
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[derive(Clone)]
struct TopicPartitionManager {
    storage_dir: Arc<PathBuf>,
    conf: Arc<DiskConfig>,
    flusher: Arc<Flusher>,
    // (topic, partition_id) -> PartitionBufferSet
    partitions: Arc<DashMap<(String, u32), PartitionBufferSet>>,
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
                                    if let Some((_, pbs)) = partitions.remove(&key) {
                                        // 2. 从刷盘器移除
                                        flusher.remove_partition_writer(&pbs.dir);

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
    async fn send(&self, topic: &str, partition_id: u32, batch: Vec<MessagePayload>) -> Result<()> {
        self.update_last_write_time(topic, partition_id);
        let pwb = self
            .get_or_create_topic_partition(topic, partition_id)
            .await?;
        pwb.write_batch(batch, true).await?;
        Ok(())
    }

    #[inline]
    fn get_cached_topic_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Option<PartitionBufferSet> {
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
    ) -> Result<PartitionBufferSet> {
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
        let pwb = PartitionBufferSet::new(
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

    stop: CancellationToken,
}

impl DiskStorageWriter {
    fn new(cfg: DiskConfig, stop: CancellationToken) -> Result<Self> {
        cfg.validate()?;
        let cfg = Arc::new(cfg);

        // 启动刷盘守护任务
        let flusher = Arc::new(Flusher::new(
            stop.clone(),
            cfg.flusher_partition_writer_buffer_tasks_num,
            cfg.flusher_partition_writer_ptr_tasks_num,
            cfg.flusher_partition_meta_tasks_num,
            Duration::from_millis(cfg.flusher_period),
            cfg.with_metrics,
        ));
        let mut _flusher = flusher.clone();
        let (flush_sender, flusher_signal) = mpsc::channel(1);
        tokio::spawn(async move { _flusher.run(flusher_signal).await });

        let worker_tasks_num = cfg.writer_worker_tasks_num;
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
            conf: cfg,
        };

        for work_id in 0..worker_tasks_num {
            let _stop = dsw.stop.clone();
            let (tx_worker, mut rx_worker) = mpsc::channel::<WriteTask>(100);
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
                        res = rx_worker.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let task = res.unwrap();
                            let result = tpm.send(&task.topic, task.partition_id, task.payloads).await;
                            let send_result = result.as_ref().map(|_| ());
                            if let Some(notify) = task.notify {
                                let _ = notify.send(send_result.map_err(|e| StorageError::IoError(e.to_string())));
                            }
                            if let Err(e) = &result {
                                error!("send data to topic[{}]-partition[{}] err: {:?}", task.topic, task.partition_id, e);
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
    pub async fn flush_topic_partition_force(&self, topic: &str, partition_id: u32) -> Result<()> {
        if self
            .topic_partition_manager
            .get_cached_topic_partition(topic, partition_id)
            .is_none()
        {
            return Err(anyhow!(
                StorageError::PartitionNotFound("DiskStorageWriter".to_string()).to_string()
            ));
        }

        let p = &self
            .conf
            .storage_dir
            .join(topic)
            .join(partition_id.to_string());

        self.flusher.flush_topic_partition(p, true).await?;
        self.flusher.flush_topic_partition_writer_ptr(p, true).await
    }

    #[inline]
    async fn get_worker(&self, worker_id: usize) -> Worker {
        if let Some(worker) = self.workers.get(&worker_id) {
            return worker.value().clone();
        }
        self.workers.iter().find(|_| true).unwrap().value().clone()
    }

    async fn write_to_worker(
        &self,
        topic: &str,
        partition_id: u32,
        datas: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<()> {
        let worker_id =
            partition_to_worker(topic, partition_id, self.workers.len() as _).unwrap_or(0);
        let worker = self.get_worker(worker_id as _).await;
        let task = WriteTask {
            topic: topic.to_string(),
            partition_id,
            payloads: datas,
            notify,
        };
        worker
            .send(task)
            .await
            .map_err(|e| StorageError::IoError(e.to_string()))?;
        Ok(())
    }

    #[inline]
    fn get_cached_partition(&self, topic: &str, partition_id: u32) -> Option<PartitionBufferSet> {
        self.topic_partition_manager
            .get_cached_topic_partition(topic, partition_id)
    }
}

// metrics
impl DiskStorageWriter {
    // 添加获取分区指标的方法
    pub fn get_metrics(&self) -> StorageWriterMetrics {
        self.flusher.get_metrics()
    }

    pub async fn reset_metrics(&self) -> Result<()> {
        self.flusher.reset_metrics();
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriter {
    async fn store(
        &self,
        topic: &str,
        partition_id: u32,
        mut payloads: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<()> {
        if payloads.is_empty() {
            if let Some(notify) = notify {
                let _ = notify.send(Err(StorageError::EmptyData));
            }
            return Err(StorageError::EmptyData);
        }

        // pre check
        for msg in &mut payloads {
            if msg.msg_id.is_empty() {
                return Err(StorageError::Unknown("msg_id 不能为空".to_string()));
            }
            if msg.payload.is_empty() {
                return Err(StorageError::EmptyData);
            }
            if msg.timestamp == 0 {
                msg.timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
            }
        }

        if let Some(pwb) = self.get_cached_partition(topic, partition_id) {
            let result = pwb.write_batch(payloads, true).await;
            self.topic_partition_manager
                .update_last_write_time(topic, partition_id);
            let send_result = result.as_ref().map(|_| ());
            if let Some(notify) = notify {
                let _ = notify.send(send_result.map_err(|e| StorageError::IoError(e.to_string())));
            }
            Ok(())
        } else {
            self.write_to_worker(topic, partition_id, payloads, notify)
                .await
        }
    }
}

#[derive(Clone)]
pub struct DiskStorageWriterWrapper {
    inner: Arc<DiskStorageWriter>,
    stop: CancellationToken,
}

impl Drop for DiskStorageWriterWrapper {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.stop.cancel();
            // 释放全局 PartitionIndexManager
            let storage_dir = self.inner.conf.storage_dir.clone();
            crate::disk::partition_index::release_global_partition_index_manager(&storage_dir);
        }
    }
}

impl Deref for DiskStorageWriterWrapper {
    type Target = Arc<DiskStorageWriter>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DiskStorageWriterWrapper {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        // 初始化全局 PartitionIndexManager
        crate::disk::partition_index::init_global_partition_index_manager(
            cfg.storage_dir.clone(),
            cfg.partition_index_num_per_topic as _,
        )?;

        let stop = CancellationToken::new();
        let dsw = DiskStorageWriter::new(cfg, stop.clone())?;
        Ok(Self {
            inner: Arc::new(dsw),
            stop,
        })
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriterWrapper {
    async fn store(
        &self,
        topic: &str,
        partition_id: u32,
        payloads: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<()> {
        self.inner
            .store(topic, partition_id, payloads, notify)
            .await
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
    use crate::{
        MessagePayload, StorageWriter as _,
        disk::{DiskStorageWriterWrapper, default_config},
    };
    use anyhow::Result;
    use bytes::Bytes;
    use futures::future::join_all;
    use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
    use tokio::time;

    fn new_disk_storage() -> DiskStorageWriterWrapper {
        DiskStorageWriterWrapper::new(DiskConfig {
            storage_dir: PathBuf::from("./data"),
            flusher_period: 50,
            flusher_factor: 1024 * 1024 * 1, // 1M
            max_msg_num_per_file: 4000,
            max_size_per_file: 500,
            compress_type: 0,
            create_next_record_file_threshold: 80,
            flusher_partition_writer_buffer_tasks_num: 10,
            flusher_partition_writer_ptr_tasks_num: 10,
            flusher_partition_meta_tasks_num: 10,
            batch_pop_size_from_buffer: 36,
            partition_index_num_per_topic: 1000,
            with_metrics: false,
            partition_writer_prealloc: false,
            writer_worker_tasks_num: 100,
            partition_cleanup_interval: 150,
            partition_inactive_threshold: 300,
        })
        .expect("error config")
    }

    #[tokio::test]
    async fn storage_store_multi() -> Result<()> {
        let store = DiskStorageWriterWrapper::new(default_config()).expect("error config");
        let datas: Vec<&'static str> = vec![
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

        let mut handles = vec![];
        for _ in 0..20 {
            let _store = store.clone();
            let _datas = datas.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100000 {
                    let idx = rand::random::<u32>() as usize;
                    let s = _datas[idx % _datas.len()];
                    let msg = MessagePayload {
                        msg_id: format!("id_{}_{}", idx, s),
                        payload: s.as_bytes().to_vec(),
                        timestamp: 0,
                        metadata: HashMap::new(),
                    };
                    if let Err(e) = _store.store("topic111", 11, vec![msg], None).await {
                        eprintln!("e = {e:?}");
                    }
                }
            }));
        }

        join_all(handles).await;
        store.flush_topic_partition_force("topic111", 11).await?;
        time::sleep(Duration::from_secs(5)).await;
        Ok(())
    }
}
