use anyhow::Result;
use arc_swap::ArcSwap;
use std::{path::PathBuf, sync::atomic::AtomicU64};

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
        for index in indexs {
            let filename = self.get_filename(index.logic_seq);
            let file = File::create(filename)?;
            file.write_all(index.serialize().as_ref())?;
        }
        Ok(())
    }
}
