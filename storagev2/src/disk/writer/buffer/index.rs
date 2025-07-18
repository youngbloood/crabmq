use super::BufferFlushable;
use crate::disk::partition_index::PartitionIndexManager;
use crate::{MessageMeta, disk::writer::buffer::switch_queue::SwitchQueue};
use anyhow::Result;

pub struct PartitionIndexWriterBuffer {
    pub topic: String,
    pub partition_id: u32,
    queue: SwitchQueue<MessageMeta>,
    index_manager: PartitionIndexManager,
}

impl PartitionIndexWriterBuffer {
    pub fn new(topic: String, partition_id: u32, index_manager: PartitionIndexManager) -> Self {
        Self {
            topic,
            partition_id,
            queue: SwitchQueue::new(),
            index_manager,
        }
    }

    /// 写入一批索引
    pub fn push_batch(&self, metas: Vec<MessageMeta>) {
        for meta in metas {
            self.queue.push(meta);
        }
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
            self.queue.pop_batch(1024) // 可配置
        };
        if batch.is_empty() {
            return Ok(0);
        }
        // 批量写入 rocksdb
        let mgr = self
            .index_manager
            .get_or_create(&self.topic, self.partition_id)
            .await?;

        mgr.batch_put(self.partition_id, &batch)?;

        Ok(batch.len() as u64)
    }
}
