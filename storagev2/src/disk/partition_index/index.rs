use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::BytesMut;
use std::{
    fs::File,
    path::PathBuf,
    sync::atomic::Ordering,
    sync::{Arc, atomic::AtomicU64},
};

use crate::{
    SegmentOffset,
    disk::{fd_cache::FileHandlerWriterAsync, meta::gen_index_filename},
};

// 每个索引文件的最大消息数量
// 1024 * 1024 * 3 * (8 + 8) = 50331648 bytes = 50MB;
const INDEX_FILE_LIMIT: u64 = 1024 * 1024 * 3; // 3145728 条消息的索引

struct PartitionIndex {
    dir: PathBuf,
    logic_seq: AtomicU64,

    now_file_left: AtomicU64,
    wh: ArcSwap<FileHandlerWriterAsync>,
}

impl PartitionIndex {
    pub fn new(dir: PathBuf) -> Self {
        // todo: 从磁盘加载逻辑序列号
        Self {
            dir,
            logic_seq: AtomicU64::new(0),
            wh: ArcSwap::new(Arc::new(FileHandlerWriterAsync::new())),
        }
    }

    fn get_filename(&self, logic_seq: u64) -> PathBuf {
        let offset = logic_seq % INDEX_FILE_LIMIT;
        self.dir.join(gen_index_filename(offset))
    }

    pub fn flush(&self, indexs: &[SegmentOffset]) -> Result<()> {
        if indexs.len() as u64 > self.now_file_left.load(Ordering::Relaxed) {
            let mut bts = BytesMut::new();
            for i in 0..self.now_file_left.load(Ordering::Relaxed) {
                bts.extend_from_slice(indexs[i as usize].segment_id.to_le_bytes().as_ref());
                bts.extend_from_slice(indexs[i as usize].offset.to_le_bytes().as_ref());
            }
            let filename = self.get_filename(self.logic_seq.load(Ordering::Relaxed));
            self.logic_seq.fetch_add(
                self.now_file_left.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.now_file_left.store(0, Ordering::Relaxed);

            // 滚动到下一个索引文件
        }

        for index in indexs {
            let filename =
                self.get_filename(self.logic_seq.load(std::sync::atomic::Ordering::Relaxed));

            self.wh.load().write(&filename)?;
            let file = File::create(filename)?;
            file.write_all(index.serialize().as_ref())?;
        }
        Ok(())
    }
}
