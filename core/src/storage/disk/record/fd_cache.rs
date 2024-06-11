use anyhow::Result;
use common::util::{check_and_create_dir, check_exist};
use crossbeam::sync::ShardedLock;
use futures::{executor::block_on, join};
use lru::LruCache;
use parking_lot::RwLock;
use std::{
    fs::{File, OpenOptions},
    num::NonZeroUsize,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs::{File as AsyncFile, OpenOptions as AsyncOpenOptions};

#[derive(Clone, Debug)]
pub struct FileHandler {
    fd: Arc<RwLock<File>>,
}

impl FileHandler {
    fn new(fd: File) -> Self {
        FileHandler {
            fd: Arc::new(RwLock::new(fd)),
        }
    }
}

impl Deref for FileHandler {
    type Target = Arc<RwLock<File>>;
    fn deref(&self) -> &Self::Target {
        &self.fd
    }
}

// impl DerefMut for FileHandler {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.fd
//     }
// }

#[derive(Clone, Debug)]
pub struct FdCache {
    inner: Arc<ShardedLock<LruCache<PathBuf, FileHandler>>>,
}

impl FdCache {
    pub fn new(size: usize) -> Self {
        FdCache {
            inner: Arc::new(ShardedLock::new(LruCache::new(
                NonZeroUsize::new(size).unwrap(),
            ))),
        }
    }

    pub fn get(&self, key: &PathBuf) -> Option<FileHandler> {
        let mut wg = self.inner.write().expect("get sharedlock read failed");
        if let Some(fd) = wg.get(key) {
            return Some(fd.clone());
        }
        None
    }

    pub fn get_or_insert(&self, key: &Path, fd: File) -> FileHandler {
        let mut wd = self.inner.write().expect("get sharedlock read failed");
        let fd_handler = FileHandler::new(fd);
        wd.get_or_insert(key.to_path_buf().clone(), || fd_handler.clone());
        fd_handler
    }

    pub fn get_or_create(&self, key: &PathBuf) -> Result<FileHandler> {
        let mut wd = self.inner.write().expect("get sharedlock read failed");
        if let Some(fd) = wd.get(key) {
            return Ok(fd.clone());
        }
        drop(wd);
        if let Some(parent) = key.parent() {
            if !check_exist(parent) {
                check_and_create_dir(parent)?;
            }
        }
        let fd = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(key)
            .expect("open filename[{key:?}] failed");
        Ok(self.get_or_insert(key, fd))
    }
}

#[derive(Clone, Debug)]
pub struct FileHandlerAsync {
    fd: Arc<RwLock<AsyncFile>>,
}

impl FileHandlerAsync {
    fn new(fd: AsyncFile) -> Self {
        FileHandlerAsync {
            fd: Arc::new(RwLock::new(fd)),
        }
    }
}

impl Deref for FileHandlerAsync {
    type Target = Arc<RwLock<AsyncFile>>;
    fn deref(&self) -> &Self::Target {
        &self.fd
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

    pub fn get(&self, key: &PathBuf) -> Option<FileHandlerAsync> {
        let mut wg = self.inner.write().expect("get sharedlock write failed");
        if let Some(fd) = wg.get(key) {
            return Some(fd.clone());
        }
        None
    }

    pub fn get_or_insert(&self, key: &Path, fd: AsyncFile) -> FileHandlerAsync {
        let mut wd = self.inner.write().expect("get sharedlock write failed");
        let fd_handler = FileHandlerAsync::new(fd);
        wd.get_or_insert(key.to_path_buf().clone(), || fd_handler.clone());
        fd_handler
    }

    pub fn get_or_create(&self, key: &PathBuf) -> Result<FileHandlerAsync> {
        let mut wd = self.inner.write().expect("get sharedlock write failed");
        if let Some(fd) = wd.get(key) {
            return Ok(fd.clone());
        }
        drop(wd);
        if let Some(parent) = key.parent() {
            if !check_exist(parent) {
                check_and_create_dir(parent)?;
            }
        }

        let mut opts = AsyncOpenOptions::new();
        let fd_fur = opts
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(key.clone());
        let fd = block_on(fd_fur)?;
        Ok(self.get_or_insert(key, fd))
    }
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, path::Path};

    use super::*;

    #[test]
    fn test_lru_cache_push() {
        let fd_cache = FdCache::new(4);

        for i in 0..10 {
            let fd = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(format!("../target/debug/{}", i))
                .unwrap();
            fd_cache.get_or_insert(Path::new(&i.to_string()), fd);
            println!("fd_cache = {fd_cache:?}");
        }
    }
}
