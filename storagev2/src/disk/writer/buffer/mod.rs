mod data;
mod index;
mod switch_queue;

use crate::{
    MessagePayload,
    disk::{
        Config as DiskConfig, PartitionIndexManager,
        meta::WriterPositionPtr,
        writer::{
            buffer::{data::PartitionWriterBuffer, index::PartitionIndexWriterBuffer},
            flusher::Flusher,
        },
    },
};
use anyhow::{Result, anyhow};
use std::{path::PathBuf, sync::Arc};

#[async_trait::async_trait]
pub trait BufferFlushable {
    async fn flush(&self, all: bool, fsync: bool) -> Result<u64>;
    async fn is_dirty(&self) -> bool;
}

#[derive(Clone)]
pub struct PartitionBufferSet {
    pub(crate) dir: PathBuf,
    data: Arc<PartitionWriterBuffer>,
    index: Arc<PartitionIndexWriterBuffer>,
}

impl PartitionBufferSet {
    pub async fn new(
        dir: PathBuf,
        conf: Arc<DiskConfig>,
        write_ptr: Arc<WriterPositionPtr>,
        flusher: Arc<Flusher>,
    ) -> Result<Self> {
        let tp = parse_topic_partition_from_dir(&dir);
        if tp.is_none() {
            return Err(anyhow!("not found topic and partition_id in dir"));
        }
        let tp = tp.unwrap();

        let partition_index_writer_buffer = PartitionIndexWriterBuffer::new(
            tp.0,
            tp.1,
            conf.clone(),
            PartitionIndexManager::new(
                conf.storage_dir.clone(),
                conf.partition_index_num_per_topic as _,
            ),
        );
        let partition_writer_buffer =
            PartitionWriterBuffer::new(dir.clone(), conf, write_ptr, flusher).await?;

        Ok(Self {
            dir,
            data: Arc::new(partition_writer_buffer),
            index: Arc::new(partition_index_writer_buffer),
        })
    }

    // will flush data and index
    pub async fn flush(&self, all: bool, fsync: bool) -> Result<u64> {
        // 1. 先刷数据
        let data_bytes = self.data.flush(all, fsync).await?;

        // 2. 再刷索引（索引刷盘可以稍微延迟，因为主要用于查询）
        let index_bytes = self.index.flush(all, fsync).await?;

        Ok(data_bytes + index_bytes)
    }

    // only flush data
    pub async fn flush_data(&self, all: bool, fsync: bool) -> Result<u64> {
        self.data.flush(all, fsync).await
    }

    // only flush index
    pub async fn flush_index(&self, all: bool, fsync: bool) -> Result<u64> {
        self.index.flush(all, fsync).await
    }

    pub async fn write_batch(
        &self,
        batch: Vec<MessagePayload>,
        flush_index_on_rotation: bool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // 1. 先写入数据缓冲区，获取写入结果和元数据
        // 注意：这里需要知道是否发生了翻页
        let (_data_bytes, message_metas, did_rotate) =
            self.data.write_batch_with_metas_and_rotation(batch).await?;

        // 2. 如果发生了翻页，先刷盘数据索引
        if flush_index_on_rotation && did_rotate && self.index.is_dirty().await {
            // 翻页时，确保索引也刷盘到 RocksDB，保证数据一致性
            self.index.flush(true, false).await?;
        }

        // 3. 将索引元数据写入索引缓冲区
        if !message_metas.is_empty() {
            self.index.push_batch(message_metas)?;
        }
        Ok(())
    }
}

/// 从 self.dir 路径中解析出 topic 和 partition_id
pub(crate) fn parse_topic_partition_from_dir(dir: &PathBuf) -> Option<(String, u32)> {
    let components: Vec<_> = dir.components().collect();
    if components.len() < 2 {
        return None;
    }
    let partition_osstr = components.last()?;
    let topic_osstr = components.get(components.len() - 2)?;
    let partition_id = partition_osstr
        .as_os_str()
        .to_string_lossy()
        .parse::<u32>()
        .ok()?;
    let topic = topic_osstr.as_os_str().to_string_lossy().to_string();
    Some((topic, partition_id))
}
