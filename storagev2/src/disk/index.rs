use crate::{
    disk::{
        config::DiskReadWriteMode,
        fd::create_reader,
        fd_cache::{FileHandlerReaderAsync, FileHandlerWriterAsync, create_writer_fd},
        gen_index_filename,
    },
    err::{ErrorCode, StorageError, StorageResult},
};
use anyhow::Result;
use bytes::BytesMut;
use std::{
    io::IoSlice,
    path::{Path, PathBuf},
};
use tokio::fs;

const SIGNLE_OFFSET_LENGTH: usize = 8 + 4;
/**
 * SignleOffset mean signle MessagePayload start offset, and the length
 */
pub struct SignleOffset {
    pub offset: u64,
    pub length: u32,
}

/**
 * 索引管理器：负责管理一个 topic-partition 的索引文件写
 */
#[derive(Clone)]
pub struct IndexWriterHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    mode: DiskReadWriteMode,

    wh: FileHandlerWriterAsync,
}

impl IndexWriterHandle {
    pub async fn new(
        dir: PathBuf,
        topic: String,
        partition_id: u32,
        mode: DiskReadWriteMode,
    ) -> Result<Self> {
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
        let wh: FileHandlerWriterAsync = create_writer_fd(&dir.join(&filename), mode).await?;

        Ok(IndexWriterHandle {
            dir,
            topic,
            partition_id,
            mode,
            wh,
        })
    }

    pub async fn flush(&self, indexs: &[SignleOffset], fsync: bool) -> StorageResult<()> {
        // 所有索引写入当前索引文件（剩余容量足够）
        let mut bts = BytesMut::with_capacity(indexs.len() * SIGNLE_OFFSET_LENGTH);
        for idx in indexs {
            bts.extend_from_slice(&idx.offset.to_le_bytes());
            bts.extend_from_slice(&idx.length.to_le_bytes());
        }
        let data: IoSlice<'_> = IoSlice::new(&bts);

        self.wh
            .write(&[data], self.mode)
            .await
            .map_err(|e| StorageError::with_message(ErrorCode::IoError, e.to_string()))?;

        if fsync {
            self.wh
                .sync_data()
                .await
                .map_err(|e| StorageError::with_message(ErrorCode::IoError, e.to_string()))?;
        }

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

/**
 * 索引管理器：负责管理一个 topic-partition 的索引文件写
 */
#[derive(Clone)]
pub struct IndexReaderHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    wh: FileHandlerReaderAsync,
}
