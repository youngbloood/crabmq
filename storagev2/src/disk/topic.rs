use crate::{
    MessagePayload,
    disk::{
        DiskReadWriteMode,
        desk::{flusher::Flusher, partition::PartitionWriterHandle},
    },
};
use anyhow::Result;
use dashmap::DashMap;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::sync::CancellationToken;

pub struct TopicWriterHandleConfig {
    dir: PathBuf, // 包含当前 topic 的目录
    topic: String,
    flush_tasks_num: u32,
    flush_interval: Duration, // 单位 ms
}

/**
 * TopicWriterHandle 管理一个 topic
 */
pub struct TopicWriterHandle {
    conf: TopicWriterHandleConfig,

    partitions: Arc<DashMap<u32, Arc<PartitionWriterHandle>>>,
    partition_locks: Arc<DashMap<u32, Arc<tokio::sync::Mutex<()>>>>,
    flush_signal: Sender<(bool, bool)>,
    stop: CancellationToken,
}

impl TopicWriterHandle {
    pub fn new(conf: TopicWriterHandleConfig, stop: CancellationToken) -> Self {
        let partitions = Arc::new(DashMap::new());
        let flusher = Flusher::new(
            stop.clone(),
            conf.flush_tasks_num,
            conf.flush_interval,
            partitions.clone(),
        );

        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(flusher.run(rx));
        Self {
            conf,
            partitions,
            partition_locks: Arc::new(DashMap::new()),
            flush_signal: tx,
            stop,
        }
    }

    pub async fn push(&self, partition_id: u32, messages: Vec<MessagePayload>) -> Result<()> {
        let handle = self.get_or_create_partition_handle(partition_id).await?;
        handle.write_batch(messages).await?;
        Ok(())
    }

    pub async fn flush(&self, all: bool, fsync: bool) -> Result<()> {
        self.flush_signal.send((all, fsync)).await?;
        Ok(())
    }

    pub fn shutdown(&self) {
        self.stop.cancel();
    }

    async fn get_or_create_partition_handle(
        &self,
        partition_id: u32,
    ) -> Result<Arc<PartitionWriterHandle>> {
        // 1. check partition write handle exist
        if let Some(h) = self.partitions.get(&partition_id) {
            return Ok(h.clone());
        }

        // 2. create partition write handle
        let pwh = Arc::new(
            PartitionWriterHandle::new(
                self.conf.dir.clone(),
                self.conf.topic.clone(),
                partition_id,
                DiskReadWriteMode::WriteVectored,
            )
            .await?,
        );

        // 3. get locks
        let lock = self
            .partition_locks
            .entry(partition_id)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();

        lock.lock().await;
        // 4. check partition write handle exist again
        if let Some(h) = self.partitions.get(&partition_id) {
            return Ok(h.clone());
        }

        // 5. insert into dashmap
        self.partitions.insert(partition_id, pwh.clone());

        Ok(pwh)
    }
}

pub struct TopicReaderHandle {
    dir: PathBuf,
    topic: String,

    partitions: DashMap<u32, Arc<PartitionWriterHandle>>,
}
