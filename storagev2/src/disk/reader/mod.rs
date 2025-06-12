use super::COMMIT_PTR_FILENAME;
use super::{READER_PTR_FILENAME, fd_cache::FdReaderCacheAync, meta::ReaderPositionPtr};
use crate::SegmentOffset;
use crate::disk::meta::gen_record_filename;
use crate::{StorageReader, StorageReaderSession, disk::StorageError};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use common::check_exist;
use dashmap::DashMap;
use std::num::NonZero;
use std::path::Path;
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
            self.dir.clone(),
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
        n: NonZero<u64>,
    ) -> Result<Vec<(Bytes, SegmentOffset)>> {
        if self
            .readers
            .get(&(topic.to_string(), partition_id))
            .is_none()
        {
            let partition_dir = self.dir.join(topic).join(partition_id.to_string());
            if !check_exist(&partition_dir) {
                return Err(anyhow!(
                    StorageError::PathNotExist(format!("{:?}", partition_dir)).to_string()
                ));
            }

            let mut partition =
                DiskStorageReaderSessionPartition::new(partition_dir, self.group_id);
            partition.load_ptr().await?;

            self.readers
                .insert((topic.to_string(), partition_id), partition);
        }

        let mut reader = self
            .readers
            .get_mut(&(topic.to_string(), partition_id))
            .unwrap();

        let mut list = vec![];
        for _ in 0..n.into() {
            list.push(reader.next().await?);
        }

        Ok(list)
    }

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32, offset: SegmentOffset) -> Result<()> {
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
            reader_ptr: Arc::new(RwLock::new(ReaderPositionPtr::with_dir_and_group_id(
                dir.clone(),
                group_id,
            ))),
            reader_ptr_filename: dir.join(format!("{}{}", READER_PTR_FILENAME, group_id)),
            commit_ptr: Arc::new(RwLock::new(ReaderPositionPtr::with_dir_and_group_id(
                dir.clone(),
                group_id,
            ))),
            commit_ptr_filename: dir.join(format!("{}{}", COMMIT_PTR_FILENAME, group_id)),
        }
    }

    async fn load_ptr(&mut self) -> Result<()> {
        if check_exist(&self.reader_ptr_filename) {
            self.reader_ptr = Arc::new(RwLock::new(
                ReaderPositionPtr::load(&self.reader_ptr_filename).await?,
            ));
        }

        if check_exist(&self.commit_ptr_filename) {
            self.commit_ptr = Arc::new(RwLock::new(
                ReaderPositionPtr::load(&self.commit_ptr_filename).await?,
            ));
        }

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

    async fn get_segment_offset(&self) -> SegmentOffset {
        let rl = self.reader_ptr.read().await;
        SegmentOffset {
            filename: rl.filename.clone(),
            offset: rl.offset,
        }
    }

    async fn read(&self) -> Result<(Bytes, SegmentOffset)> {
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
        let len = u64::from_be_bytes(len_buf);
        if len == 0 {
            return Ok((Bytes::new(), SegmentOffset::default()));
        }

        // 读取消息内容
        let mut buf = vec![0u8; len as usize];
        wl.read_exact(&mut buf).await?;

        // 更新读指针
        self.reader_ptr.write().await.offset += 8 + len;
        let reader = self.clone();
        tokio::spawn(async move {
            let _ = reader.save_reader_ptr().await;
        });
        Ok((Bytes::from(buf), self.get_segment_offset().await))
    }

    async fn next(&mut self) -> Result<(Bytes, SegmentOffset)> {
        match self.read().await {
            Ok(data) => {
                if !data.0.is_empty() {
                    return Ok(data);
                }
                // data为空，滚动文件查找
                let filename = {
                    let reader_ptr_rl = self.reader_ptr.read().await;
                    reader_ptr_rl.filename.clone()
                };
                // 表示该文件读取完毕，寻找下一个
                let next_filename = self.dir.join(filename_factor_next_record(&filename));
                if check_exist(&next_filename) {
                    let mut read_ptr_wl = self.reader_ptr.write().await;
                    read_ptr_wl.filename = next_filename;
                    read_ptr_wl.offset = 0;
                }
                return self.read().await;
            }
            Err(_) => todo!(),
        }
    }
}

fn filename_factor_next_record(filename: &Path) -> PathBuf {
    let filename: PathBuf = PathBuf::from(filename.file_name().unwrap());
    let filename_factor = filename
        .with_extension("")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    PathBuf::from(gen_record_filename(filename_factor + 1))
}

#[cfg(test)]
mod test {
    use crate::{StorageReader, disk::DiskStorageReader};
    use std::{num::NonZero, path::PathBuf};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn reader_session() {
        let dsr = DiskStorageReader::new(PathBuf::from("./data"));
        let sess = dsr.new_session(2).await.expect("new session failed");
        let stop = CancellationToken::new();
        while let Ok(data) = sess
            .next("topic111", 11, NonZero::new(1_u64).unwrap())
            .await
        {
            // if data.is_empty() {
            //     return;
            // }
            println!("data = {data:?}");
        }
    }
}
