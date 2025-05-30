use super::COMMIT_PTR_FILENAME;
use super::{READER_PTR_FILENAME, fd_cache::FdReaderCacheAync, meta::ReaderPositionPtr};
use crate::{StorageReader, StorageReaderSession, disk::StorageError};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use common::check_exist;
use dashmap::DashMap;
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct DiskStorageReader {
    dir: PathBuf,
    // group_id: session
    sessions: Arc<DashMap<u32, DiskStorageReaderSession>>,
}

impl DiskStorageReader {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            sessions: Arc::default(),
        }
    }
}

#[async_trait::async_trait]
impl StorageReader for DiskStorageReader {
    /// New a session with group_id, it will be return Err() when session has been created.
    async fn new_session(&self, group_id: u32) -> Result<Box<dyn StorageReaderSession>> {
        Ok(Box::new(DiskStorageReaderSession::new(
            PathBuf::new(),
            group_id,
        )))
    }

    /// Close a session by group_id.
    async fn close_session(&self, group_id: u32) {
        self.sessions.remove(&group_id);
    }
}

#[derive(Debug)]
pub struct DiskStorageReaderSession {
    // 消息文件存储路径
    dir: PathBuf,
    group_id: u32,
    // (topic, partition_id) -> FdReaderCacheAync
    readers: Arc<DashMap<(String, u32), DiskStorageReaderSessionPartition>>,
}

impl DiskStorageReaderSession {
    pub fn new(dir: PathBuf, group_id: u32) -> Self {
        Self {
            dir,
            group_id,
            readers: Arc::default(),
        }
    }
}

#[async_trait::async_trait]
impl StorageReaderSession for DiskStorageReaderSession {
    /// Get the next message
    ///
    /// It should block when there are no new message
    async fn next(
        &self,
        topic: &str,
        partition_id: u32,
        stop_signal: CancellationToken,
    ) -> Result<Bytes> {
        if self
            .readers
            .get(&(topic.to_string(), partition_id))
            .is_none()
        {
            let partition_dir = self.dir.join(topic).join(partition_id.to_string());
            if !check_exist(&partition_dir) {
                return Err(anyhow!(StorageError::PathNotExist.to_string()));
            }

            let mut partition =
                DiskStorageReaderSessionPartition::new(partition_dir, self.group_id);
            partition.load_ptr().await?;

            self.readers
                .insert((topic.to_string(), partition_id), partition);
        }

        let reader = self
            .readers
            .get(&(topic.to_string(), partition_id))
            .unwrap();

        Ok(reader.next().await?)
    }

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32) -> Result<()> {
        // TODO:
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DiskStorageReaderSessionPartition {
    // topic-partition 的目录
    dir: PathBuf,
    group_id: u32,
    fd_cache: FdReaderCacheAync,
    reader_ptr: Arc<RwLock<ReaderPositionPtr>>,
    reader_ptr_filename: PathBuf,
    commit_ptr: Arc<RwLock<ReaderPositionPtr>>,
    commit_ptr_filename: PathBuf,
}

impl DiskStorageReaderSessionPartition {
    fn new(dir: PathBuf, group_id: u32) -> Self {
        Self {
            dir: dir.clone(),
            group_id,
            fd_cache: FdReaderCacheAync::new(2),
            reader_ptr: Arc::default(),
            reader_ptr_filename: dir.join(format!("{}{}", READER_PTR_FILENAME, group_id)),
            commit_ptr: Arc::default(),
            commit_ptr_filename: dir.join(format!("{}{}", COMMIT_PTR_FILENAME, group_id)),
        }
    }

    async fn load_ptr(&mut self) -> Result<()> {
        if !check_exist(&self.reader_ptr_filename) {
            return Ok(());
        }
        self.reader_ptr = Arc::new(RwLock::new(
            ReaderPositionPtr::load(&self.reader_ptr_filename).await?,
        ));
        if !check_exist(&self.commit_ptr_filename) {
            return Ok(());
        }
        self.commit_ptr = Arc::new(RwLock::new(
            ReaderPositionPtr::load(&self.commit_ptr_filename).await?,
        ));
        Ok(())
    }

    async fn save_reader_ptr(&self) -> Result<()> {
        self.reader_ptr
            .read()
            .await
            .save(&self.reader_ptr_filename)
            .await?;
        Ok(())
    }

    async fn save_commit_ptr(&self) -> Result<()> {
        self.commit_ptr
            .read()
            .await
            .save(&self.commit_ptr_filename)
            .await?;
        Ok(())
    }

    async fn next(&self) -> Result<Bytes> {
        let (filename, read_offset) = {
            let reader_ptr_rl = self.reader_ptr.read().await;
            (reader_ptr_rl.filename.clone(), reader_ptr_rl.offset)
        };

        let file = self.fd_cache.get_or_create(&filename)?;

        let mut wl = file.write().await;
        wl.seek(std::io::SeekFrom::Start(read_offset)).await?;

        // 读取消息长度前缀
        let mut len_buf = [0u8; 8];
        wl.read_exact(&mut len_buf).await?;
        let len = u64::from_le_bytes(len_buf);
        if len == 0 {
            return Ok(Bytes::new());
        }

        // 读取消息内容
        let mut buf = vec![0u8; len as usize];
        wl.read_exact(&mut buf).await?;

        // 更新读指针
        self.reader_ptr.write().await.offset += 8 + len;
        let reader = self.clone();
        tokio::spawn(async move {
            reader.save_reader_ptr().await;
        });
        Ok(Bytes::from(buf))
    }
}
