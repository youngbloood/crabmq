use super::BufferFlushable;
use crate::disk::partition_index::ReadWritePartitionIndexManager;
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
    // 使用读写分离的索引管理器，专门用于写入
    read_write_index_manager: Arc<ReadWritePartitionIndexManager>,
}

impl PartitionIndexWriterBuffer {
    pub fn new(
        topic: String,
        partition_id: u32,
        conf: Arc<DiskConfig>,
        storage_dir: std::path::PathBuf,
    ) -> Self {
        // 创建读写分离的索引管理器，专门用于写入操作
        let read_write_index_manager = Arc::new(ReadWritePartitionIndexManager::new(
            storage_dir,
            conf.partition_index_num_per_topic as _,
        ));

        Self {
            topic,
            partition_id,
            conf,
            queue: SwitchQueue::new(),
            read_write_index_manager,
        }
    }

    /// 写入一批索引
    pub fn push_batch(&self, metas: Vec<MessageMeta>) -> Result<()> {
        for meta in metas {
            self.queue.push(meta);
        }
        Ok(())
    }

    /// 获取读写分离的索引管理器（用于外部访问）
    pub fn get_read_write_index_manager(&self) -> Arc<ReadWritePartitionIndexManager> {
        self.read_write_index_manager.clone()
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
        let bytes_len = self
            .read_write_index_manager
            .batch_put(&self.topic, self.partition_id, &batch)
            .await?;

        // 如果需要强制刷盘
        if _fsync {
            // 获取写实例并强制刷盘
            let write_instance = self
                .read_write_index_manager
                .get_write_instance(&self.topic, self.partition_id)
                .await?;
            write_instance.db.flush()?;
        }

        Ok(bytes_len)
    }
}
