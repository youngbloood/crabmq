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
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

#[derive(Clone)]
struct PartitionWriterWrapper {
    dir: PathBuf,
    writer: Arc<PartitionWriterBuffer>,
    writer_ptr: PartitionWriterPtr,
}

impl PartitionWriterWrapper {
    pub async fn write(&self, data: Bytes) -> Result<()> {
        self.writer.write(data).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct TopicStorage {
    dir: PathBuf,
    partitions: Arc<DashMap<u32, PartitionWriterWrapper>>,
    meta: TopicMeta,
}

#[derive(Clone)]
pub struct DiskStorageWriter {
    cfg: DiskConfig,
    topics: Arc<DashMap<String, TopicStorage>>,
    fd_cache: Arc<FdWriterCacheAync>,
    flusher: Flusher,

    flush_sender: mpsc::Sender<()>,
}

impl DiskStorageWriter {
    pub fn new(cfg: DiskConfig) -> Result<Self> {
        cfg.validate()?;
        let fd_cache = Arc::new(FdWriterCacheAync::new(cfg.fd_cache_size as usize));

        // 启动刷盘守护任务
        let flusher = Flusher::new(Duration::from_millis(cfg.flusher_period));
        let mut _flusher = flusher.clone();
        let (flush_sender, flusher_signal) = mpsc::channel(1);
        tokio::spawn(async move { _flusher.run(flusher_signal).await });

        Ok(Self {
            cfg,
            topics: Arc::new(DashMap::new()),
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
        if let Some(ts) = self.topics.get(topic) {
            return Ok(ts.clone());
        }

        // 创建新 topic 存储
        let dir = self.cfg.storage_dir.join(topic);
        tokio::fs::create_dir_all(&dir).await?;

        let meta_path = dir.join(TOPIC_META);
        let topic_meta = if meta_path.exists() {
            TopicMeta::load(&meta_path).await?
        } else {
            TopicMeta {
                keys: Arc::new(DashMap::new()),
            }
        };

        let ts = TopicStorage {
            dir: dir.clone(),
            partitions: Arc::new(DashMap::new()),
            meta: topic_meta,
        };
        self.topics.insert(topic.to_string(), ts.clone());
        self.flusher.add_topic_meta(meta_path, ts.meta.clone());

        Ok(ts)
    }

    async fn get_partition_writer(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<PartitionWriterWrapper> {
        let ts = self.get_topic_storage(topic).await?;

        if let Some(pd) = ts.partitions.get(&partition) {
            return Ok(pd.clone());
        }

        // 创建新 partition
        let dir = ts.dir.join(format!("{}", partition));
        tokio::fs::create_dir_all(&dir).await?;

        let writer_ptr_filename = dir.join(WRITER_PTR_FILENAME);
        let partition_writer_ptr = if writer_ptr_filename.exists() {
            PartitionWriterPtr::load(&writer_ptr_filename).await?
        } else {
            PartitionWriterPtr::new(dir.join(gen_record_filename(0)))
        };

        let writer = Arc::new(
            PartitionWriterBuffer::new(
                dir.clone(),
                &self.cfg,
                self.fd_cache.clone(),
                partition_writer_ptr.get_inner(),
            )
            .await?,
        );
        self.flusher
            .add_partition_writer(dir.clone(), writer.clone());
        let pd = PartitionWriterWrapper {
            dir,
            writer,
            writer_ptr: partition_writer_ptr,
        };
        self.flusher
            .add_partition_meta(writer_ptr_filename, pd.writer_ptr.clone());
        ts.partitions.insert(partition, pd.clone());
        Ok(pd)
    }
}

#[async_trait::async_trait]
impl StorageWriter for DiskStorageWriter {
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Err(anyhow!("can't store empty data"));
        }
        let pd = self.get_partition_writer(topic, partition).await?;
        pd.write(data).await?;
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
    async fn storage_store_bench() -> Result<()> {
        let store = DiskStorageWriter::new(default_config()).expect("error config");

        let message_content = Bytes::from(vec![b'x'; 1024]);
        for _ in 0..10 {
            for i in 1..101 {
                if let Err(e) = store.store("mytopic", i, message_content.clone()).await {
                    eprintln!("e = {e:?}");
                }
            }
        }
        time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }
}
