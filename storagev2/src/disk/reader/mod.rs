use super::COMMIT_PTR_FILENAME;
use super::{READER_PTR_FILENAME, fd_cache::FdReaderCacheAync, meta::ReaderPositionPtr};
use crate::disk::meta::{WRITER_PTR_FILENAME, WriterPositionPtrSnapshot, gen_record_filename};
use crate::{ReadPosition, SegmentOffset, StorageError, StorageResult};
use crate::{StorageReader, StorageReaderSession};
use anyhow::{Result};
use bytes::Bytes;
use common::check_exist;
use dashmap::DashMap;
use log::{error, warn};
use std::num::NonZero;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncReadExt as _;
use tokio::sync::RwLock;

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
    async fn new_session(
        &self,
        group_id: u32,
        read_position: Vec<(String, ReadPosition)>,
    ) -> StorageResult<Box<dyn StorageReaderSession>> {
        let sess = self
            .sessions
            .entry(group_id)
            .or_insert(DiskStorageReaderSession::new(
                self.dir.clone(),
                group_id,
                read_position,
            ));
        Ok(Box::new(sess.value().clone()))
    }

    /// Close a session by group_id.
    async fn close_session(&self, group_id: u32) {
        self.sessions.remove(&group_id);
    }
}

#[derive(Debug, Clone)]
pub struct DiskStorageReaderSession {
    // 消息文件存储路径
    dir: PathBuf,
    group_id: u32,
    read_positions: Arc<DashMap<String, ReadPosition>>,
    // (topic, partition_id) -> FdReaderCacheAync
    readers: Arc<DashMap<(String, u32), DiskStorageReaderSessionPartition>>,
}

impl DiskStorageReaderSession {
    pub fn new(dir: PathBuf, group_id: u32, read_positions: Vec<(String, ReadPosition)>) -> Self {
        let m = DashMap::new();
        read_positions.iter().for_each(|(k, v)| {
            m.insert(k.clone(), v.clone());
        });

        Self {
            dir,
            group_id,
            read_positions: Arc::new(m),
            readers: Arc::default(),
        }
    }

    fn get_read_position(&self, topic: &str) -> ReadPosition {
        if let Some(v) = self.read_positions.get(topic) {
            return v.value().clone();
        }
        ReadPosition::Begin
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
    ) -> StorageResult<Vec<(Bytes, SegmentOffset)>> {
        if self
            .readers
            .get(&(topic.to_string(), partition_id))
            .is_none()
        {
            let partition_dir = self.dir.join(topic).join(partition_id.to_string());
            if !check_exist(&partition_dir) {
                return Err(
                    StorageError::PathNotExist(format!("{:?}", partition_dir))
                );
            }

            let mut partition =
                DiskStorageReaderSessionPartition::new(partition_dir, self.group_id);
            partition.load_ptr(&self.get_read_position(topic)).await?;

            self.readers
                .insert((topic.to_string(), partition_id), partition);
        }
        let reader = self
            .readers
            .get(&(topic.to_string(), partition_id))
            .unwrap();

        let mut list = vec![];
        for _ in 0..n.into() {
            let (data, offset, last) = reader.next().await?;
            list.push((data, offset));
            if last {
                break;
            }
        }

        Ok(list)
    }

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32, offset: SegmentOffset) -> StorageResult<()> {
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

    async fn load_ptr(&mut self, read_position: &ReadPosition) -> StorageResult<()> {
        if check_exist(&self.reader_ptr_filename) {
            self.reader_ptr = Arc::new(RwLock::new(
                ReaderPositionPtr::load(&self.reader_ptr_filename).await.map_err(|e| StorageError::IoError(e.to_string()))?,
            ));
        } else if let Some(parent) = self.reader_ptr_filename.parent() {
            if read_position == &ReadPosition::Latest {
                let writer_ptr_filename = parent.join(WRITER_PTR_FILENAME);

                let data = tokio::fs::read_to_string(writer_ptr_filename).await.map_err(|e| StorageError::IoError(e.to_string()))?;
                if !data.is_empty() {
                    let sp: WriterPositionPtrSnapshot = serde_json::from_str(&data).map_err(|e| StorageError::SerializeError(e.to_string()))?;
                    let mut wl = self.reader_ptr.write().await;
                    wl.filename = sp.filename;
                    wl.offset = sp.offset;
                }
            }
        }

        if check_exist(&self.commit_ptr_filename) {
            self.commit_ptr = Arc::new(RwLock::new(
                ReaderPositionPtr::load(&self.commit_ptr_filename).await.map_err(|e| StorageError::IoError(e.to_string()))?,
            ));
        } else if read_position == &ReadPosition::Latest {
            let (filename, offset) = {
                let rl = self.reader_ptr.read().await;
                (rl.filename.clone(), rl.offset)
            };
            let mut wl = self.commit_ptr.write().await;
            wl.filename = filename;
            wl.offset = offset;
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
            segment_id: extract_segment_id_from_filename(&rl.filename),
            offset: rl.offset,
        }
    }

    /// returns:
    /// 消息体，该消息的 SegmentOffset 信息， 该消息是否是该文件的最后一个消息
    async fn read(&self) -> StorageResult<(Bytes, SegmentOffset, bool)> {
        let (filename, read_offset) = {
            let rl = self.reader_ptr.read().await;
            (rl.filename.clone(), rl.offset)
        };

        let file = self.fd_cache.get_or_create(&filename, read_offset).await?;

        let mut wl = file.write().await;
        // 读取消息长度前缀
        let mut len_buf = [0u8; 8];
        match wl.read_exact(&mut len_buf).await {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                warn!("read reached EOF in file {:?}", filename);
                return Ok((Bytes::new(), SegmentOffset::default(), true));
            }
            Err(e) => {
                error!("failed to read header length: {:?}", e);
                return Err(StorageError::IoError(e.to_string()));
            }
        }

        let len = u64::from_be_bytes(len_buf);
        if len == 0 {
            return Ok((Bytes::new(), SegmentOffset::default(), true));
        }

        // 读取消息内容
        let mut buf = vec![0u8; len as usize];
        match wl.read_exact(&mut buf).await {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                error!(
                    "incomplete message: len={}, file={:?}, offset={}",
                    len, filename, read_offset
                );
                return Ok((Bytes::new(), SegmentOffset::default(), true));
            }
            Err(e) => {
                error!("failed to read message content: {:?}", e);
                return Err(StorageError::IoError(e.to_string()));
            }
        }

        {
            let mut ptr_wl = self.reader_ptr.write().await;
            // 更新读指针
            ptr_wl.offset += 8 + len;
        }
        let reader = self.clone();
        tokio::spawn(async move {
            let _ = reader.save_reader_ptr().await;
        });
        Ok((Bytes::from(buf), self.get_segment_offset().await, false))
    }

    async fn next(&self) -> StorageResult<(Bytes, SegmentOffset, bool)> {
        match self.read().await {
            Ok(data) => {
                if !data.0.is_empty() {
                    return Ok(data);
                }
                // 文件滚动
                let (current_filename, next_filename) = {
                    let reader_ptr_rl = self.reader_ptr.read().await;
                    let next = self
                        .dir
                        .join(filename_factor_next_record(&reader_ptr_rl.filename));
                    (reader_ptr_rl.filename.clone(), next)
                };
                if check_exist(&next_filename) {
                    let mut read_ptr_wl = self.reader_ptr.write().await;
                    read_ptr_wl.filename = next_filename;
                    read_ptr_wl.offset = 0;
                    return self.read().await;
                }
                // 没有更多文件
                Ok((Bytes::new(), SegmentOffset::default(), true))
            }
            Err(e) => {
                error!("StorageReader read failed: {:?}", e.to_string());
                Err(e)
            }
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

fn extract_segment_id_from_filename(p: &Path) -> u64 {
    // 获取文件名（去除目录）
    let file_name = p
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("0.record");

    // 提取 xxxx.record 中的 xxxx
    let segment_id = file_name
        .strip_suffix(".record")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    segment_id
}

#[cfg(test)]
mod test {
    use crate::{StorageReader, disk::DiskStorageReader};
    use std::{num::NonZero, path::PathBuf};

    #[tokio::test]
    async fn reader_session() {
        let dsr = DiskStorageReader::new(PathBuf::from("./messages"));
        let sess = dsr
            .new_session(1001, vec![])
            .await
            .expect("new session failed");
        while let Ok(data) = sess
            .next("topic111", 11, NonZero::new(1_u64).unwrap())
            .await
        {
            println!("data.len() = {}", data.len());
            println!("bytes.len() = {}", data.first().unwrap().0.len());
        }
    }

    #[tokio::test]
    async fn reader_session2() {
        let dsr = DiskStorageReader::new(PathBuf::from("../data/message2"));
        let sess = dsr
            .new_session(1000, vec![])
            .await
            .expect("new session failed");
        while let Ok(data) = sess.next("mytopic", 6, NonZero::new(1_u64).unwrap()).await {
            println!("data.len() = {}", data.len());
            println!("bytes.len() = {}", data.first().unwrap().0.len());
        }
    }
}
