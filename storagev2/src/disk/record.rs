#[cfg(not(target_os = "linux"))]
use crate::disk::fd_cache::{FileHandlerReaderAsync, FileHandlerWriterAsync, create_writer_fd};
use crate::disk::{config::DiskReadWriteMode, gen_record_filename, index::SignleOffset};
use anyhow::Result;
use std::{io::IoSlice, path::PathBuf};
use tokio::fs;

/**
 * 记录管理器：负责管理一个 topic-partition 的数据文件
 */
#[derive(Clone)]
pub struct RecordWriterHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    mode: DiskReadWriteMode,

    #[cfg(not(target_os = "linux"))]
    pub(crate) current_fd: FileHandlerWriterAsync,

    #[cfg(target_os = "linux")]
    pub(crate) current_fd: Arc<RwLock<UringFile>>,
}

impl RecordWriterHandle {
    pub async fn new(
        dir: PathBuf,
        topic: String,
        partition_id: u32,
        mode: DiskReadWriteMode,
    ) -> Result<Self> {
        let mut read_dir = fs::read_dir(&dir).await?;
        let mut max_record_index = 0;

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            // 解析文件名中的索引号并更新最大索引号
            let file_name = entry.file_name();
            let file_name = file_name.to_str().unwrap();
            let file_name = file_name.split(".").nth(0).unwrap();
            let file_name = file_name.parse::<u64>().unwrap();
            if file_name > max_record_index {
                max_record_index = file_name;
            }
        }

        let filename = dir.join(gen_record_filename(max_record_index));

        #[cfg(not(target_os = "linux"))]
        let current_fd = create_writer_fd(&filename, mode).await?;

        #[cfg(target_os = "linux")]
        let current_fd = Arc::new(RwLock::new(UringFile::create(&dir, mode).await?));

        Ok(Self {
            dir,
            topic,
            partition_id,
            mode,
            current_fd,
        })
    }

    pub async fn flush(&self, data: &[IoSlice<'_>], fsync: bool) -> Result<Vec<SignleOffset>> {
        // build the index info
        let mut offsets = Vec::with_capacity(data.len());
        let mut start = self.current_fd.get_write_cursor();
        for d in data {
            offsets.push(SignleOffset {
                offset: start,
                length: d.len() as u32,
            });
            start += d.len() as u64;
        }

        // write to PageCache
        self.current_fd.write(data, self.mode).await?;

        // fsync
        if fsync {
            self.current_fd.sync_data().await?;
        }

        Ok(offsets)
    }
}

/**
 * 记录管理器：负责管理一个 topic-partition 的数据文件
 */
#[derive(Clone)]
pub struct RecordReaderHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    mode: DiskReadWriteMode,

    #[cfg(not(target_os = "linux"))]
    pub(crate) current_fd: FileHandlerReaderAsync,

    #[cfg(target_os = "linux")]
    pub(crate) current_fd: Arc<RwLock<UringFile>>,
}
