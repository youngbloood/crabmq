use super::BufferFlushable;
use crate::disk::partition_index::PartitionIndexManager;
use crate::{
    MessageMeta, disk::Config as DiskConfig, disk::writer::buffer::switch_queue::SwitchQueue,
};
use anyhow::Result;
use std::sync::Arc;

pub struct PartitionIndexWriterBuffer {
    pub topic: String,
    pub partition_id: u32,
    conf: Arc<DiskConfig>,
    queue: SwitchQueue<MessageMeta>,
    index_manager: Arc<PartitionIndexManager>,
}

impl PartitionIndexWriterBuffer {
    pub fn new(
        topic: String,
        partition_id: u32,
        conf: Arc<DiskConfig>,
        index_manager: Arc<PartitionIndexManager>,
    ) -> Self {
        Self {
            topic,
            partition_id,
            conf,
            queue: SwitchQueue::new(),
            index_manager,
        }
    }

    /// 写入一批索引
    pub fn push_batch(&self, metas: Vec<MessageMeta>) -> Result<()> {
        for meta in metas {
            self.queue.push(meta);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BufferFlushable for PartitionIndexWriterBuffer {
    async fn is_dirty(&self) -> bool {
        self.queue.is_dirty()
    }

    async fn flush(&self, all: bool, _fsync: bool) -> Result<u64> {
        // 取出所有或部分索引
        let batch = if all {
            self.queue.pop_all()
        } else {
            self.queue
                .pop_batch(self.conf.batch_pop_size_from_buffer as usize)
        };
        if batch.is_empty() {
            return Ok(0);
        }
        // 批量写入 rocksdb
        let mgr = self
            .index_manager
            .get_or_create(&self.topic, self.partition_id)
            .await?;
        let bytes_len = mgr.batch_put(self.partition_id, &batch)?;
        if _fsync {
            mgr.db.flush()?;
        }
        Ok(bytes_len)
    }
}
