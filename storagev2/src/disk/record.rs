use crate::disk::config::DiskReadWriteMode;
#[cfg(not(target_os = "linux"))]
use crate::disk::fd_cache::{FileHandlerReaderAsync, FileHandlerWriterAsync, create_writer_fd};
use std::{io::IoSlice, path::PathBuf};

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
    ) -> Self {
        #[cfg(not(target_os = "linux"))]
        let current_fd = create_writer_fd(&dir, mode).await.unwrap();

        #[cfg(target_os = "linux")]
        let current_fd = Arc::new(RwLock::new(UringFile::create(&dir, mode).await.unwrap()));

        Self {
            dir,
            topic,
            partition_id,
            mode,
            current_fd,
        }
    }

    pub async fn flush(&self, data: &[IoSlice<'_>], fsync: bool) -> anyhow::Result<Vec<u64>> {
        Ok(vec![])
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
