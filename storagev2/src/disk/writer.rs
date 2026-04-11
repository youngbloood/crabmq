use super::config::WriterConfig as DiskConfig;
use crate::disk::flusher::Flusher;
use crate::disk::gen_record_filename;
use crate::disk::partition::PartitionWriterHandle;
use crate::err::ErrorCode;
use crate::metrics::StorageWriterMetrics;
use crate::{MessagePayload, StorageError, StorageResult, StorageSearch, StorageWriter};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use murmur3::murmur3_32;
use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::{CancellationToken, DropGuard};

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

enum LoadPartitionMode {
    Create,
    Check,
}

#[derive(Clone)]
pub struct DiskStorageWriter {
    conf: Arc<DiskConfig>,

    partitions: Arc<DashMap<(String, u32), Arc<PartitionWriterHandle>>>,

    partitions_lock: Arc<DashMap<(String, u32), Mutex<()>>>,

    flusher: Flusher,
    // NOTE: 会导致所有分区刷盘，慎用
    _flush_sender: mpsc::Sender<(bool, bool)>,
    // workers: Arc<DashMap<usize, Worker>>, // worker 池
    stop: Arc<DropGuard>,
}

impl DiskStorageWriter {
    fn new(mut conf: DiskConfig) -> StorageResult<Self> {
        conf = conf.fix();
        conf.validate()?;
        let conf = Arc::new(conf);

        let partitions = Arc::new(DashMap::new());
        let stop = CancellationToken::new();

        let flusher = Flusher::new(
            stop.child_token(),
            1,
            Duration::from_millis(50),
            partitions.clone(),
            true,
        );

        let (tx, rx) = mpsc::channel(1);
        let _flusher = flusher.clone();
        tokio::spawn(async move { _flusher.run(rx) });
        // let worker_tasks_num = cfg.writer_worker_tasks_num;
        Ok(Self {
            partitions,
            flusher,
            _flush_sender: tx,
            stop: Arc::new(stop.drop_guard()),
            // workers: Arc::new(DashMap::new()),
            conf,
            partitions_lock: Arc::new(DashMap::new()),
        })
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

impl DiskStorageWriter {
    async fn get_topic_partition(
        &self,
        topic: &str,
        partition_id: u32,
        m: LoadPartitionMode,
    ) -> Result<Arc<PartitionWriterHandle>> {
        let key = (topic.to_string(), partition_id);
        // 第一重检查：快速路径
        if let Some(pwb) = self.partitions.get(&key) {
            return Ok(pwb.value().clone());
        }

        // 先加载/创建 PartitionWriterPtr，必须放到 _partition_lock 之外
        let dir = self
            .conf
            .storage_dir
            .join(topic)
            .join(partition_id.to_string());
        match m {
            LoadPartitionMode::Create => {
                tokio::fs::create_dir_all(&dir).await?;
            }
            LoadPartitionMode::Check => {
                let exist = fs::exists(&dir)?;
                if !exist {
                    return Err(anyhow!("11"));
                }
            }
        }

        let pwh = PartitionWriterHandle::new(
            dir,
            topic.to_string(),
            partition_id,
            self.conf.disk_write_mode,
        )
        .await?;

        // 获取或创建partition级别的锁
        let mux = self
            .partitions_lock
            .entry(key.clone())
            .or_insert_with(|| Mutex::new(()));

        let _lock = mux.value().lock().await;

        // 第二重检查：在持有锁后再次检查
        if let Some(pwb) = self.partitions.get(&key) {
            return Ok(pwb.value().clone());
        }

        let pwh = Arc::new(pwh);
        self.partitions.insert(key, pwh.clone());
        Ok(pwh)
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriter {
    async fn store(
        &self,
        topic: &str,
        partition_id: u32,
        payloads: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<u64> {
        // 1. 获取 PartitionBufferSet
        let pwh = self
            .get_topic_partition(topic, partition_id, LoadPartitionMode::Create)
            .await
            .map_err(|e| StorageError::with_message(ErrorCode::IoError, e.to_string()))?;

        // 2. 写入
        let seq = pwh
            .write_batch(payloads)
            .await
            .map_err(|e| StorageError::with_message(ErrorCode::IoError, e.to_string()))?;

        // 3. (如果需要)向外面确认返回
        if let Some(notify_tx) = notify {
            // 如果你的架构需要等待 Flusher，就把 notify_tx 塞进 pwb 的内存队列里
            // 如果不需要等待落盘，这里即刻返回 Ok(())
            let _ = notify_tx.send(Ok(()));
        }

        Ok(seq)
    }
}

#[async_trait::async_trait]
impl StorageSearch for DiskStorageWriter {
    async fn search(
        &self,
        topic: &str,
        partition_id: u32,
        logic_seq: u64,
    ) -> StorageResult<MessagePayload> {
        if let Ok(pwb) = self
            .get_topic_partition(topic, partition_id, LoadPartitionMode::Check)
            .await
        {
            let m = pwb
                .search(logic_seq)
                .map_err(|e| StorageError::with_message(ErrorCode::EmptyData, e.to_string()))?;
            Ok(m)
        } else {
            Err(StorageError::new(ErrorCode::PartitionNotFound))
        }
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
    use super::*;
    use crate::{MessagePayload, StorageWriter as _};
    use anyhow::Result;
    use bytes::Bytes;
    use futures::future::join_all;
    use std::time::Duration;
    use tokio::time;

    fn new_disk_storage() -> DiskStorageWriter {
        let cfg = DiskConfig::default();
        DiskStorageWriter::new(cfg).expect("error config")
    }

    #[tokio::test]
    async fn storage_store_multi() -> Result<()> {
        let store = DiskStorageWriter::new(DiskConfig::default()).expect("error config");
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
                    let msg = MessagePayload::new_v1(
                        Bytes::from(format!("id_{}_{}", idx, s)),
                        0,
                        Vec::new(),
                        Bytes::from(s),
                    );
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
