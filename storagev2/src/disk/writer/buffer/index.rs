use super::BufferFlushable;
use crate::SegmentOffset;
use crate::disk::DiskReadWriteMode;
use crate::disk::fd::{Writer, create_reader};
use crate::disk::fd_cache::{FileHandlerWriterAsync, create_writer_fd};
use crate::disk::meta::gen_index_filename;
use crate::disk::partition_index::ReadWritePartitionIndexManager;
use crate::{
    MessageMeta, disk::Config as DiskConfig, disk::writer::buffer::switch_queue::SwitchQueue,
};
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::BytesMut;
use dashmap::mapref::entry;
use std::fs;
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct PartitionIndexWriterBuffer {
    pub topic: String,
    pub partition_id: u32,
    conf: Arc<DiskConfig>,
    queue: SwitchQueue<SegmentOffset>,
    // 使用读写分离的索引管理器，专门用于写入
    index_manager: Arc<IndexManager>,
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
            conf.clone(),
        ));

        Self {
            topic,
            partition_id,
            conf,
            queue: SwitchQueue::new(),
            index_manager: read_write_index_manager,
        }
    }

    /// 写入一批索引
    pub fn push_batch(&self, metas: Vec<SegmentOffset>) -> Result<()> {
        for meta in metas {
            self.queue.push(meta);
        }
        Ok(())
    }

    /// 获取读写分离的索引管理器（用于外部访问）
    pub fn get_read_write_index_manager(&self) -> Arc<ReadWritePartitionIndexManager> {
        self.index_manager.clone()
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
            .index_manager
            .batch_put(&self.topic, self.partition_id, &batch)
            .await?;

        // 如果需要强制刷盘
        if _fsync {
            // 获取写实例并强制刷盘
            let write_instance = self
                .index_manager
                .get_write_instance(&self.topic, self.partition_id)
                .await?;
            write_instance.db.flush()?;
        }

        Ok(bytes_len)
    }
}

// 每个索引文件的最大消息数量
// 1024 * 1024 * 3 * (8 + 8) = 50331648 bytes = 50MB;
const INDEX_FILE_LIMIT: u64 = 1024 * 1024 * 3; // 3145728 条消息的索引

struct IndexManager {
    dir: PathBuf,
    mode: DiskReadWriteMode,
    logic_seq: AtomicU64,

    wh: ArcSwap<FileHandlerWriterAsync>,
}

impl IndexManager {
    pub fn new(dir: PathBuf) -> Self {
        // todo: 从磁盘加载逻辑序列号
        Self {
            dir,
            logic_seq: AtomicU64::new(0),
            wh: ArcSwap::new(Arc::new(FileHandlerWriterAsync::new())),
        }
    }

    fn init(&mut self) -> Result<()> {
        let dir = fs::read_dir(self.dir)?;

        let mut max_index_index = 0;
        dir.for_each(|entry| {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            // 解析文件名中的索引号并更新最大索引号
        });
        let filename = self.get_filename(self.logic_seq.load(Ordering::Relaxed));
        let w = create_writer_fd(&filename, DiskReadWriteMode::WriteVectored).await?;
        self.wh.store(w);
        Ok(())
    }

    fn get_filename(&self, logic_seq: u64) -> PathBuf {
        let offset = (logic_seq / INDEX_FILE_LIMIT) + 1;
        self.dir.join(gen_index_filename(offset))
    }

    pub async fn flush(&self, indexs: &[SegmentOffset]) -> Result<()> {
        let left = self.logic_seq.load(Ordering::Relaxed) % INDEX_FILE_LIMIT;

        // indexs 的长度超过当前索引文件剩余容量，先写满当前索引文件并滚动到下一个索引文件
        if indexs.len() as u64 > left {
            let mut bts = BytesMut::with_capacity((left as usize) * 16);
            for i in 0..left {
                bts.extend_from_slice(indexs[i as usize].segment_id.to_le_bytes().as_ref());
                bts.extend_from_slice(indexs[i as usize].offset.to_le_bytes().as_ref());
            }

            let data: IoSlice<'_> = IoSlice::new(&bts);
            let filename = self.get_filename(self.logic_seq.load(Ordering::Relaxed));
            self.logic_seq.fetch_add(left, Ordering::Relaxed);
            self.wh
                .load()
                .write(&[data], DiskReadWriteMode::WriteVectored)
                .await?;

            // 滚动到下一个索引文件，并写入
            let mut bts = BytesMut::with_capacity((indexs.len() - left as usize) * 16);
            for i in left..(indexs.len() as u64) {
                bts.extend_from_slice(indexs[i as usize].segment_id.to_le_bytes().as_ref());
                bts.extend_from_slice(indexs[i as usize].offset.to_le_bytes().as_ref());
            }

            let data: IoSlice<'_> = IoSlice::new(&bts);
            let filename = self.get_filename(self.logic_seq.load(Ordering::Relaxed));
            let w = create_writer_fd(&filename, DiskReadWriteMode::WriteVectored)
                .await?
                .into();
            self.wh.store(w);
            self.logic_seq
                .fetch_add(indexs.len() as u64 - left, Ordering::Relaxed);
            self.wh
                .load()
                .write(&[data], DiskReadWriteMode::WriteVectored)
                .await?;
        }

        // 所有索引写入当前索引文件（剩余容量足够）
        let mut bts = BytesMut::with_capacity(indexs.len() * 16);
        for so in indexs {
            bts.extend_from_slice(so.segment_id.to_le_bytes().as_ref());
            bts.extend_from_slice(so.offset.to_le_bytes().as_ref());
        }

        let data: IoSlice<'_> = IoSlice::new(&bts);
        self.logic_seq
            .fetch_add(indexs.len() as u64, Ordering::Relaxed);

        self.wh
            .load()
            .write(&[data], DiskReadWriteMode::WriteVectored)
            .await?;

        Ok(())
    }

    async fn find_logic_seq(f: &Path, mode: DiskReadWriteMode) -> Result<u64> {
        let mut reader = create_reader(f, mode).await?;
        let mut logic_num = 0;
        loop {
            logic_num += 1;
            let meta_cell = reader.read(16).await?;
            let segment_id = u16::from_le_bytes(meta_cell[..8].try_into().unwrap());
            if segment_id == 0 {
                break;
            }
        }
        Ok(logic_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_filename() {
        let cases = vec![
            (0, 1_u64),
            (1, 1),
            (INDEX_FILE_LIMIT, 1),
            (INDEX_FILE_LIMIT + 1, 2),
            (INDEX_FILE_LIMIT * 2, 2),
            (INDEX_FILE_LIMIT * 2 + 1, 3),
        ];

        let index = IndexManager::new(PathBuf::from("/tmp"));
        for (logic_seq, expect) in cases {
            assert_eq!(
                index.get_filename(logic_seq),
                PathBuf::from(format!("/tmp/{}", gen_index_filename(expect)))
            );
        }
    }
}
