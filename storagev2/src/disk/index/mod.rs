use crate::{
    SegmentOffset,
    disk::{
        DiskReadWriteMode,
        fd::create_reader,
        fd_cache::{FileHandlerWriterAsync, create_writer_fd},
        gen_index_filename,
    },
};
use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::BytesMut;
use std::{
    io::IoSlice,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::fs;

// 每个索引文件的最大消息数量
// 1024 * 1024 * 3 * (8 + 8) = 50331648 bytes = 50MB;
const INDEX_FILE_LIMIT: u64 = 1024 * 1024 * 3; // 3145728 条消息的索引

pub struct IndexManager {
    dir: PathBuf,
    mode: DiskReadWriteMode,
    logic_seq: AtomicU64,

    wh: ArcSwap<FileHandlerWriterAsync>,
}

impl IndexManager {
    pub async fn new(dir: PathBuf, mode: DiskReadWriteMode) -> Result<Self> {
        // 读取改目录下的索引文件，找到最大的索引文件编号和对应的逻辑序列号
        let mut read_dir = fs::read_dir(&dir).await?;
        let mut max_index_index = 0;

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            // 解析文件名中的索引号并更新最大索引号
            let file_name = entry.file_name();
            let file_name = file_name.to_str().unwrap();
            let file_name = file_name.split(".").nth(0).unwrap();
            let file_name = file_name.parse::<u64>().unwrap();
            if file_name > max_index_index {
                max_index_index = file_name;
            }
        }

        let filename = gen_index_filename(max_index_index);
        let w = create_writer_fd(&dir.join(&filename), mode).await?;

        let logic_seq = Self::find_logic_seq(&dir.join(filename), mode).await?;

        Ok(IndexManager {
            dir,
            mode,
            logic_seq: AtomicU64::new(max_index_index * INDEX_FILE_LIMIT + logic_seq),
            wh: ArcSwap::new(Arc::new(w)),
        })
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

    #[tokio::test]
    async fn test_get_filename() {
        let cases = vec![
            (0, 1_u64),
            (1, 1),
            (INDEX_FILE_LIMIT, 1),
            (INDEX_FILE_LIMIT + 1, 2),
            (INDEX_FILE_LIMIT * 2, 2),
            (INDEX_FILE_LIMIT * 2 + 1, 3),
        ];

        let index = IndexManager::new(PathBuf::from("/tmp"), DiskReadWriteMode::Mmap)
            .await
            .unwrap();
        for (logic_seq, expect) in cases {
            assert_eq!(
                index.get_filename(logic_seq),
                PathBuf::from(format!("/tmp/{}", gen_index_filename(expect)))
            );
        }
    }
}
