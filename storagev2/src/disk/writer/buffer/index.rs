use super::BufferFlushable;
use crate::SegmentOffset;
use crate::disk::index::IndexManager;
use crate::{disk::Config as DiskConfig, disk::writer::buffer::switch_queue::SwitchQueue};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

pub struct PartitionIndexWriterBuffer {
    pub topic: String,
    pub partition_id: u32,
    conf: Arc<DiskConfig>,
    queue: SwitchQueue<SegmentOffset>,
    // 使用读写分离的索引管理器，专门用于写入
    index_manager: Arc<IndexManager>,
}

impl PartitionIndexWriterBuffer {
    pub async fn new(
        topic: String,
        partition_id: u32,
        conf: Arc<DiskConfig>,
        storage_dir: PathBuf,
    ) -> Result<Self> {
        let index = IndexManager::new(storage_dir, conf.disk_write_mode).await?;

        Ok(Self {
            topic,
            partition_id,
            conf: conf.clone(),
            queue: SwitchQueue::new(conf.message_count_limit_per_partition as usize),
            index_manager: Arc::new(index),
        })
    }

    /// 写入一批索引
    pub fn push_batch(&self, metas: Vec<SegmentOffset>) -> Result<()> {
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

        // 使用读写分离的索引管理器进行批量写入
        self.index_manager.flush(&batch).await?;

        Ok((batch.len() * 16) as u64)
    }
}
