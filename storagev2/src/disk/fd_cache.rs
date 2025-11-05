use crate::{StorageError, StorageResult};

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

// 写文件句柄
#[derive(Clone, Debug)]
pub struct FileHandlerWriterAsync {
    inner: Arc<tokio::sync::Mutex<AsyncFile>>,
}

impl Deref for FileHandlerWriterAsync {
    type Target = Arc<tokio::sync::Mutex<AsyncFile>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl FileHandlerWriterAsync {
    pub fn new(f: AsyncFile) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(f)),
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, AsyncFile> {
        self.inner.lock().await
    }

    pub async fn reset(&self, f: AsyncFile) {
        *self.inner.lock().await = f;
    }

    pub async fn write_vectored(&self, datas: &[IoSlice<'_>]) -> Result<usize> {
        let mut w = self.inner.lock().await;
        Ok(w.write_vectored(datas).await?)
    }

    pub async fn sync_data(&self) -> Result<()> {
        let w = self.inner.lock().await;
        Ok(w.sync_data().await?)
    }

    pub async fn seek(&self, pos: SeekFrom) -> Result<u64> {
        let mut w = self.inner.lock().await;
        Ok(w.seek(pos).await?)
    }
}

#[derive(Clone, Debug)]
pub struct FdWriterCacheAync {
    prealloc_size: usize,
    inner: Arc<RwLock<LruCache<PathBuf, FileHandlerWriterAsync>>>,
}

impl FdWriterCacheAync {
    pub fn new(prealloc_size: usize, buffer_size: usize) -> Self {
        FdWriterCacheAync {
            prealloc_size,
            inner: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(buffer_size).unwrap(),
            ))),
        }
    }

    pub async fn get(&self, key: &Path) -> Option<FileHandlerWriterAsync> {
        let mut wg = self.inner.write().await;
        wg.get(key).cloned()
    }

    pub async fn get_or_create(
        &self,
        key: &Path,
        prealloc_size: u64,
    ) -> Result<FileHandlerWriterAsync> {
        // 第一次检查缓存
        if let Some(handler) = self.get(key).await {
            return Ok(handler);
        }

        // 确保父目录存在
        if let Some(parent) = key.parent() {
            if !check_exist(parent) {
                check_and_create_dir(parent)?;
            }
        }

        let mut write_opt = OpenOptions::new();
        write_opt.write(true);
        if !check_exist(key) {
            // 当前文件不存在，则创建
            write_opt.create(true);
        }

        // 先创建写文件句柄，确保文件存在
        let mut write_fd = write_opt
            .open(key)
            .map_err(|e| anyhow::anyhow!("Failed to open write file: {}", e))?;
        if prealloc_size > 0 {
            preallocate(&write_fd, prealloc_size as _)?;
        }
        write_fd.seek(std::io::SeekFrom::Start(0))?;

        let async_write = AsyncFile::from_std(write_fd);
        let handler = FileHandlerWriterAsync::new(async_write);

        // 再次检查并插入缓存
        let mut wg = self.inner.write().await;
        if let Some(existing) = wg.get(key) {
            return Ok(existing.clone());
        }
        wg.put(key.to_path_buf(), handler.clone());
        Ok(handler)
    }
}

pub fn create_writer_fd(p: &Path) -> Result<AsyncFile> {
    let mut write_opt = OpenOptions::new();
    write_opt.write(true);
    if !check_exist(p) {
        // 当前文件不存在，则创建
        write_opt.create(true);
    }

    // 先创建写文件句柄，确保文件存在
    let write_fd = write_opt
        .open(p)
        .map_err(|e| anyhow::anyhow!("Failed to open write file: {}", e))?;
    Ok(AsyncFile::from_std(write_fd))
}

pub fn create_writer_fd_with_prealloc(p: &Path, alloc_size: u64) -> Result<AsyncFile> {
    let mut write_opt = OpenOptions::new();
    write_opt.write(true);
    let mut create = false;
    if !check_exist(p) {
        // 当前文件不存在，则创建
        write_opt.create(true);
        create = true;
    }

    // 先创建写文件句柄，确保文件存在
    let mut write_fd = write_opt
        .open(p)
        .map_err(|e| anyhow::anyhow!("Failed to open write file: {}", e))?;
    if create && alloc_size > 0 {
        preallocate(&write_fd, alloc_size)?;
        write_fd.seek(SeekFrom::Start(0))?;
    }
    Ok(AsyncFile::from_std(write_fd))
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
