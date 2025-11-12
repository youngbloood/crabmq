use crate::{StorageError, StorageResult};

use super::fd::{self, Writer};
use super::prealloc::preallocate;
use anyhow::Result;
use common::util::{check_and_create_dir, check_exist};
use lru::LruCache;
use std::{
    fs::OpenOptions,
    io::{IoSlice, Seek, SeekFrom},
    num::NonZeroUsize,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::File as AsyncFile,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::RwLock,
};
use once_cell::sync::Lazy;

// 读文件句柄
#[derive(Clone, Debug)]
pub struct FileHandlerReaderAsync {
    inner: Arc<RwLock<AsyncFile>>,
}

impl Deref for FileHandlerReaderAsync {
    type Target = Arc<RwLock<AsyncFile>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone, Debug)]
pub struct FdReaderCacheAync {
    inner: Arc<RwLock<LruCache<PathBuf, FileHandlerReaderAsync>>>,
}

impl FdReaderCacheAync {
    pub fn new(size: usize) -> Self {
        FdReaderCacheAync {
            inner: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(size).unwrap()))),
        }
    }

    pub async fn get(&self, key: &Path) -> Option<FileHandlerReaderAsync> {
        let mut wl = self.inner.write().await;
        wl.get(key).cloned()
    }

    pub async fn get_or_create(
        &self,
        key: &Path,
        read_offset: u64,
    ) -> StorageResult<FileHandlerReaderAsync> {
        // 第一次检查缓存
        if let Some(handler) = self.get(key).await {
            return Ok(handler);
        }

        // 然后打开读文件句柄
        let mut read_fd = OpenOptions::new()
            .read(true)
            .open(key)
            .map_err(|e| StorageError::IoError(e.to_string()))?;
        if read_offset != 0 {
            read_fd
                .seek(SeekFrom::Start(read_offset))
                .map_err(|e| StorageError::IoError(e.to_string()))?;
        }
        let async_read = Arc::new(RwLock::new(AsyncFile::from_std(read_fd)));
        let handler = FileHandlerReaderAsync { inner: async_read };

        // 再次检查并插入缓存
        let mut wg = self.inner.write().await;
        if let Some(existing) = wg.get(key) {
            return Ok(existing.clone());
        }
        wg.put(key.to_path_buf(), handler.clone());
        Ok(handler)
    }
}

// 元数据文件句柄：用于 TopicMeta 和 WriterPositionPtr
// 这些文件需要完整的 AsyncFile 功能（set_len, write_all 等）
#[derive(Clone, Debug)]
pub struct MetaFileHandler {
    inner: Arc<tokio::sync::Mutex<AsyncFile>>,
}

impl MetaFileHandler {
    pub fn new(f: AsyncFile) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(f)),
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, AsyncFile> {
        self.inner.lock().await
    }
}

// 写文件句柄：包装 fd::Writer，提供统一接口（用于数据文件）
///
/// 注意：不再使用 Mutex！Writer 内部已经是并发安全的，使用 Arc 共享即可
#[derive(Clone)]
pub struct FileHandlerWriterAsync {
    inner: Arc<dyn Writer>,
}

impl FileHandlerWriterAsync {
    /// 从 fd::Writer 创建
    pub fn from_writer(writer: Box<dyn Writer>) -> Self {
        Self {
            inner: Arc::from(writer),
        }
    }

    /// 统一写入方法：委托给底层的 Writer（无需锁！）
    pub async fn write(
        &self,
        datas: &[IoSlice<'_>],
        _mode: crate::disk::DiskWriteMode, // 保留参数兼容性，但不再使用
    ) -> Result<usize> {
        self.inner.write(datas).await
    }

    /// 同步数据到磁盘（无需锁！）
    pub async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    /// 获取当前写入位置（无需锁！）
    pub fn write_pos(&self) -> u64 {
        self.inner.write_pos()
    }
}

// 注意：FdWriterCacheAync 已废弃，不再使用
// 现在直接使用 create_writer_fd 和 create_writer_fd_with_prealloc

/// 创建简单的 AsyncFile（用于元数据文件）
///
/// 注意：元数据文件使用简单的 AsyncFile，不经过 fd 抽象层
pub async fn create_simple_async_file(p: &Path) -> Result<AsyncFile> {
    use std::fs::OpenOptions;
    let mut write_opt = OpenOptions::new();
    write_opt.read(true).write(true);
    if !check_exist(p) {
        write_opt.create(true);
    }

    let write_fd = write_opt
        .open(p)
        .map_err(|e| anyhow::anyhow!("Failed to open write file: {}", e))?;
    Ok(AsyncFile::from_std(write_fd))
}

/// 创建写入文件句柄（不预分配）
///
/// 注意：现在返回 FileHandlerWriterAsync，内部使用 fd::Writer
pub async fn create_writer_fd(
    p: &Path,
    mode: crate::disk::DiskWriteMode,
) -> Result<FileHandlerWriterAsync> {
    let writer = fd::create_writer(p, mode, false, 0).await?;
    Ok(FileHandlerWriterAsync::from_writer(writer))
}

/// 创建写入文件句柄（带预分配）
///
/// 注意：现在返回 FileHandlerWriterAsync，内部使用 fd::Writer
pub async fn create_writer_fd_with_prealloc(
    p: &Path,
    alloc_size: u64,
    mode: crate::disk::DiskWriteMode,
) -> Result<FileHandlerWriterAsync> {
    let writer = fd::create_writer(p, mode, true, alloc_size).await?;
    Ok(FileHandlerWriterAsync::from_writer(writer))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[tokio::test]
    async fn test_lru_cache_push() -> Result<()> {
        let fd_cache = FdReaderCacheAync::new(4);
        for i in 0..10 {
            let p = format!("../target/debug/{}", i);
            fd_cache
                .get_or_create(Path::new(&p), 0)
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }
        Ok(())
    }
}
