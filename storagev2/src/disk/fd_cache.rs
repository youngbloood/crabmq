use super::prealloc::preallocate;
use anyhow::Result;
use common::util::{check_and_create_dir, check_exist};
use crossbeam::sync::ShardedLock;
use lru::LruCache;
use std::{
    fs::OpenOptions,
    io::Seek,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs::File as AsyncFile, sync::RwLock};

#[derive(Clone, Debug)]
pub struct FileHandlerAsync {
    reader: Arc<RwLock<AsyncFile>>,
    writer: Arc<RwLock<AsyncFile>>,
}

impl FileHandlerAsync {
    fn new(reader: AsyncFile, writer: AsyncFile) -> Self {
        FileHandlerAsync {
            reader: Arc::new(RwLock::new(reader)),
            writer: Arc::new(RwLock::new(writer)),
        }
    }

    pub fn reader(&self) -> &Arc<RwLock<AsyncFile>> {
        &self.reader
    }

    pub fn writer(&self) -> &Arc<RwLock<AsyncFile>> {
        &self.writer
    }
}

#[derive(Clone, Debug)]
pub struct FdCacheAync {
    inner: Arc<ShardedLock<LruCache<PathBuf, FileHandlerAsync>>>,
}

impl FdCacheAync {
    pub fn new(size: usize) -> Self {
        FdCacheAync {
            inner: Arc::new(ShardedLock::new(LruCache::new(
                NonZeroUsize::new(size).unwrap(),
            ))),
        }
    }

    pub fn get(&self, key: &Path) -> Option<FileHandlerAsync> {
        let mut wg = self.inner.write().expect("Failed to acquire write lock");
        wg.get(key).cloned()
    }

    pub fn get_or_create(&self, key: &Path) -> Result<FileHandlerAsync> {
        // 第一次检查缓存
        {
            let mut wg = self.inner.write().expect("Failed to acquire write lock");
            if let Some(handler) = wg.get(key) {
                return Ok(handler.clone());
            }
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

        preallocate(&write_fd, 1000)?;
        write_fd.seek(std::io::SeekFrom::Start(0))?;

        // 然后打开读文件句柄
        let read_fd = OpenOptions::new()
            .read(true)
            .open(key)
            .map_err(|e| anyhow::anyhow!("Failed to open read file: {}", e))?;

        let async_write = AsyncFile::from_std(write_fd);
        let async_read = AsyncFile::from_std(read_fd);
        let handler = FileHandlerAsync::new(async_read, async_write);

        // 再次检查并插入缓存
        let mut wg = self.inner.write().expect("Failed to acquire write lock");
        if let Some(existing) = wg.get(key) {
            return Ok(existing.clone());
        }
        wg.put(key.to_path_buf(), handler.clone());
        Ok(handler)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_lru_cache_push() -> Result<()> {
        let fd_cache = FdCacheAync::new(4);
        for i in 0..10 {
            let p = format!("../target/debug/{}", i);
            fd_cache.get_or_create(Path::new(&p))?;
        }
        Ok(())
    }
}
