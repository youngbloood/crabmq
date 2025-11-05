use super::COMMIT_PTR_FILENAME;
use super::{READER_PTR_FILENAME, fd_cache::FdReaderCacheAync, meta::ReaderPositionPtr};
use crate::MessagePayload;
use crate::disk::meta::{WRITER_PTR_FILENAME, WriterPositionPtrSnapshot, gen_record_filename};
use crate::{ReadPosition, SegmentOffset, StorageError, StorageResult};
use crate::{StorageReader, StorageReaderSession};
use anyhow::Result;
use bytes::Bytes;
use common::check_exist;
use dashmap::DashMap;
use log::{error, warn};
use std::num::NonZero;
use std::path::Path;
use std::{path::PathBuf, sync::{Arc, atomic::AtomicUsize}};
use tokio::io::AsyncReadExt as _;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DiskStorageReader {
    // storage_dir
    dir: PathBuf,
    partition_index_num_per_topic: u32,
    sessions: Arc<DashMap<u32, DiskStorageReaderSession>>,
}

impl DiskStorageReader {
    pub fn new(dir: PathBuf, partition_index_num_per_topic: u32) -> Self {
        Self {
            dir,
            partition_index_num_per_topic,
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
                self.partition_index_num_per_topic,
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
    // 消息文件存储路径（storage_dir）
    dir: PathBuf,
    group_id: u32,

    partition_index_num_per_topic: u32,
    read_positions: Arc<DashMap<String, ReadPosition>>,
    // (topic, partition_id) -> FdReaderCacheAync
    readers: Arc<DashMap<(String, u32), DiskStorageReaderSessionPartition>>,

    // 使用读写分离的索引管理器，专门用于读取操作
    read_write_index_manager: Arc<crate::disk::partition_index::ReadWritePartitionIndexManager>,
}

impl DiskStorageReaderSession {
    pub fn new(
        dir: PathBuf,
        group_id: u32,
        partition_index_num_per_topic: u32,
        read_positions: Vec<(String, ReadPosition)>,
    ) -> Self {
        let m = DashMap::new();
        read_positions.iter().for_each(|(k, v)| {
            m.insert(k.clone(), v.clone());
        });

        // 创建读写分离的索引管理器，专门用于读取操作
        let read_write_index_manager = Arc::new(
            crate::disk::partition_index::ReadWritePartitionIndexManager::new(
                dir.clone(),
                partition_index_num_per_topic as _,
            ),
        );

        Self {
            dir: dir.clone(),
            group_id,
            partition_index_num_per_topic,
            read_positions: Arc::new(m),
            readers: Arc::default(),
            read_write_index_manager,
        }
    }

    fn get_read_position(&self, topic: &str) -> ReadPosition {
        if let Some(v) = self.read_positions.get(topic) {
            return v.value().clone();
        }
        ReadPosition::Begin
    }

    /// 获取读写分离的索引管理器（用于外部访问）
    pub fn get_read_write_index_manager(
        &self,
    ) -> Arc<crate::disk::partition_index::ReadWritePartitionIndexManager> {
        self.read_write_index_manager.clone()
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
    ) -> StorageResult<Vec<(MessagePayload, u64, SegmentOffset)>> {
        if self
            .readers
            .get(&(topic.to_string(), partition_id))
            .is_none()
        {
            let partition_dir = self.dir.join(topic).join(partition_id.to_string());
            if !check_exist(&partition_dir) {
                return Err(StorageError::PathNotExist(format!("{:?}", partition_dir)));
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
            if !data.is_empty() {
                // 反序列化内部数据
                let inner: crate::MessagePayloadInner =
                    rkyv::from_bytes::<crate::MessagePayloadInner, rkyv::rancor::Error>(&data)
                        .map_err(|e| StorageError::SerializeError(e.to_string()))?;

                // 构造 MessagePayload（不公开 inner）
                let payload = MessagePayload::new(
                    inner.msg_id,
                    inner.timestamp,
                    inner.metadata,
                    inner.payload,
                );
                list.push((payload, data.len() as u64, offset));
            }
            if last {
                break;
            }
        }

        Ok(list)
    }

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(
        &self,
        topic: &str,
        partition_id: u32,
        offset: SegmentOffset,
    ) -> StorageResult<()> {
        // 获取对应的 partition reader
        let reader = self
            .readers
            .get(&(topic.to_string(), partition_id))
            .ok_or_else(|| {
                StorageError::PartitionNotFound(format!(
                    "topic: {}, partition: {}",
                    topic, partition_id
                ))
            })?;

        // 使用读写分离的索引管理器验证 offset 的有效性
        let _ = self
            .read_write_index_manager
            .get_msg_id_by_segment_offset(topic, partition_id, &offset)
            .await
            .map_err(|e| {
                StorageError::OffsetMismatch(format!("Invalid offset {:?}: {:?}", offset, e))
            })?;

        // 验证并更新 commit_ptr
        reader.commit_offset(offset).await?;

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
                ReaderPositionPtr::load(&self.reader_ptr_filename)
                    .await
                    .map_err(|e| StorageError::IoError(e.to_string()))?,
            ));
        } else if let Some(parent) = self.reader_ptr_filename.parent() {
            if read_position == &ReadPosition::Latest {
                let writer_ptr_filename = parent.join(WRITER_PTR_FILENAME);

                let data = tokio::fs::read_to_string(writer_ptr_filename)
                    .await
                    .map_err(|e| StorageError::IoError(e.to_string()))?;
                if !data.is_empty() {
                    let sp: WriterPositionPtrSnapshot = serde_json::from_str(&data)
                        .map_err(|e| StorageError::SerializeError(e.to_string()))?;
                    let mut wl = self.reader_ptr.write().await;
                    // 从 segment_id 重建 filename
                    wl.filename = parent.join(crate::disk::meta::gen_record_filename(sp.segment_id));
                    wl.offset = sp.offset;
                }
            }
        }

        if check_exist(&self.commit_ptr_filename) {
            self.commit_ptr = Arc::new(RwLock::new(
                ReaderPositionPtr::load(&self.commit_ptr_filename)
                    .await
                    .map_err(|e| StorageError::IoError(e.to_string()))?,
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

        // 计算消息开始位置的offset（当前读取位置）
        let message_start_offset = SegmentOffset {
            segment_id: extract_segment_id_from_filename(&filename),
            offset: read_offset,
        };

        {
            let mut ptr_wl = self.reader_ptr.write().await;
            // 更新读指针
            ptr_wl.offset += 8 + len;
        }
        let reader = self.clone();
        tokio::spawn(async move {
            let _ = reader.save_reader_ptr().await;
        });
        Ok((Bytes::from(buf), message_start_offset, false))
    }

    async fn next(&self) -> StorageResult<(Bytes, SegmentOffset, bool)> {
        match self.read().await {
            Ok(data) => {
                if !data.0.is_empty() {
                    return Ok(data);
                }
                // 文件滚动
                let (_current_filename, next_filename) = {
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

    async fn commit_offset(&self, offset: SegmentOffset) -> StorageResult<()> {
        // 获取当前 commit_ptr 位置
        let current_commit = {
            let commit_rl = self.commit_ptr.read().await;
            SegmentOffset {
                segment_id: extract_segment_id_from_filename(&commit_rl.filename),
                offset: commit_rl.offset,
            }
        };

        // 验证 offset 不能回退（基本的顺序性检查）
        if offset.segment_id < current_commit.segment_id
            || (offset.segment_id == current_commit.segment_id
                && offset.offset < current_commit.offset)
        {
            return Err(StorageError::OffsetMismatch(format!(
                "Cannot commit offset {:?} which is before current commit offset {:?}",
                offset, current_commit
            )));
        }

        // 更新 commit_ptr
        let target_filename = self.dir.join(gen_record_filename(offset.segment_id));
        {
            let mut commit_wl = self.commit_ptr.write().await;
            commit_wl.filename = target_filename;
            commit_wl.offset = offset.offset;
        }

        // 异步保存 commit_ptr
        let reader = self.clone();
        tokio::spawn(async move {
            if let Err(e) = reader.save_commit_ptr().await {
                error!("Failed to save commit pointer: {:?}", e);
            }
        });

        Ok(())
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
        let dsr = DiskStorageReader::new(PathBuf::from("./messages"), 100);
        let sess = dsr
            .new_session(1001, vec![])
            .await
            .expect("new session failed");
    }

    #[tokio::test]
    async fn reader_session2() {
        let dsr = DiskStorageReader::new(PathBuf::from("./messages"), 100);
        let sess = dsr
            .new_session(1001, vec![])
            .await
            .expect("new session failed");
    }

    /// 测试数据写入正确性和commit offset验证逻辑
    #[tokio::test]
    async fn test_data_integrity_and_commit_validation() {
        use crate::{
            MessagePayload, ReadPosition, SegmentOffset, StorageWriter,
            disk::{DiskStorageWriterWrapper, default_config},
        };
        use std::fs;
        use tokio::time::Duration;

        // 测试目录
        let test_dir = "./test_data_integrity_commit_1";
        let topic = "test_topic";
        let partition_id = 0;
        let group_id = 1001;

        // 清理之前的测试数据
        if fs::metadata(test_dir).is_ok() {
            fs::remove_dir_all(test_dir).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 创建存储配置
        let mut config = default_config();
        config.storage_dir = PathBuf::from(test_dir);
        config.with_metrics = true;

        // 创建writer并写入测试数据
        let writer = DiskStorageWriterWrapper::new(config).expect("Failed to create writer");

        println!("[测试] 开始写入测试数据...");

        // 写入多条消息
        let messages = vec![
            MessagePayload::new(
                "msg_1".to_string(),
                1000,
                Default::default(),
                b"Hello World 1".to_vec(),
            ),
            MessagePayload::new(
                "msg_2".to_string(),
                1001,
                Default::default(),
                b"Hello World 2".to_vec(),
            ),
            MessagePayload::new(
                "msg_3".to_string(),
                1002,
                Default::default(),
                b"Hello World 3".to_vec(),
            ),
        ];

        // 写入消息
        for (i, msg) in messages.iter().enumerate() {
            let result = writer
                .store(topic, partition_id, vec![msg.clone()], None)
                .await;
            assert!(
                result.is_ok(),
                "Failed to write message {}: {:?}",
                i,
                result.err()
            );
            println!("[测试] 写入消息 {}: {}", i, msg.msg_id);
        }

        // 等待数据刷盘
        tokio::time::sleep(Duration::from_secs(5)).await;

        // 强制刷盘
        writer
            .flush_topic_partition_force(topic, partition_id)
            .await
            .expect("Failed to flush");

        println!("[测试] 数据写入完成，开始验证...");

        // 等待更长时间确保索引刷盘完成
        tokio::time::sleep(Duration::from_secs(10)).await;

        // 关闭writer并等待一段时间，避免锁冲突
        drop(writer);
        tokio::time::sleep(Duration::from_secs(5)).await;

        // 创建reader并验证数据
        let reader = DiskStorageReader::new(PathBuf::from(test_dir), 100);
        let session = reader
            .new_session(group_id, vec![(topic.to_string(), ReadPosition::Begin)])
            .await
            .expect("Failed to create reader session");

        // 读取所有消息
        let mut all_messages = Vec::new();
        let mut all_offsets = Vec::new();

        loop {
            let messages = session
                .next(topic, partition_id, NonZero::new(10).unwrap())
                .await;
            match messages {
                Ok(msgs) => {
                    if msgs.is_empty() {
                        break;
                    }
                    for (msg, _, offset) in msgs {
                        println!("[测试] 读取消息: {}, offset: {:?}", msg.msg_id, offset);
                        all_messages.push(msg);
                        all_offsets.push(offset);
                    }
                }
                Err(e) => {
                    println!("[测试] 读取完成或出错: {:?}", e);
                    break;
                }
            }
        }

        // 验证读取的消息数量
        assert_eq!(all_messages.len(), messages.len(), "消息数量不匹配");
        println!("[测试] 成功读取 {} 条消息", all_messages.len());

        // 验证消息内容
        for (i, (read_msg, original_msg)) in all_messages.iter().zip(messages.iter()).enumerate() {
            assert_eq!(
                read_msg.msg_id, original_msg.msg_id,
                "消息ID不匹配 at index {}",
                i
            );
            assert_eq!(
                read_msg.payload, original_msg.payload,
                "消息内容不匹配 at index {}",
                i
            );
            println!("[测试] 消息 {} 内容验证通过", i);
        }

        // 测试commit offset验证逻辑
        println!("\n[测试] 开始测试commit offset验证逻辑...");

        // 1. 测试正常commit - 应该成功
        if !all_offsets.is_empty() {
            let first_offset = all_offsets[0].clone();
            println!("[测试] 测试正常commit offset: {:?}", first_offset);
            let result = session.commit(topic, partition_id, first_offset).await;
            assert!(result.is_ok(), "正常commit应该成功: {:?}", result.err());
            println!("[测试] 正常commit成功");
        }

        // 2. 测试commit无效的offset - 应该失败
        // 构造一个不存在的segment_id来测试partition_index_manager的验证
        let invalid_offset = SegmentOffset {
            segment_id: 9999,
            offset: 0,
        };
        println!("[测试] 测试无效commit offset: {:?}", invalid_offset);
        let result = session.commit(topic, partition_id, invalid_offset).await;
        assert!(result.is_err(), "无效offset的commit应该失败");
        println!("[测试] 无效offset commit正确失败: {:?}", result.err());

        // 3. 测试commit回退offset - 应该失败
        if all_offsets.len() >= 2 {
            let later_offset = all_offsets[1].clone();
            let earlier_offset = all_offsets[0].clone();

            // 先commit后面的offset
            session
                .commit(topic, partition_id, later_offset.clone())
                .await
                .expect("Failed to commit later offset");
            println!("[测试] 已commit later offset: {:?}", later_offset);

            // 尝试commit前面的offset - 应该失败
            println!(
                "[测试] 测试回退commit offset: {:?} -> {:?}",
                later_offset, earlier_offset
            );
            let result = session.commit(topic, partition_id, earlier_offset).await;
            assert!(result.is_err(), "回退commit应该失败");
            println!("[测试] 回退commit正确失败: {:?}", result.err());
        }

        // 4. 测试commit当前reader位置的offset - 应该成功
        if !all_offsets.is_empty() {
            let last_offset = all_offsets.last().unwrap().clone();
            println!("[测试] 测试commit最后一个offset: {:?}", last_offset);
            let result = session.commit(topic, partition_id, last_offset).await;
            assert!(
                result.is_ok(),
                "commit最后一个offset应该成功: {:?}",
                result.err()
            );
            println!("[测试] commit最后一个offset成功");
        }

        // 5. 验证offset的递增性
        println!("\n[测试] 验证offset的递增性...");
        for i in 1..all_offsets.len() {
            let prev = all_offsets[i - 1].clone();
            let curr = all_offsets[i].clone();

            // 验证segment_id和offset都是递增的
            assert!(
                curr.segment_id > prev.segment_id
                    || (curr.segment_id == prev.segment_id && curr.offset > prev.offset),
                "Offset应该递增: {:?} -> {:?}",
                prev,
                curr
            );
            println!("[测试] Offset递增验证通过: {:?} -> {:?}", prev, curr);
        }

        // 6. 测试从Latest位置读取
        println!("\n[测试] 测试从Latest位置读取...");
        let latest_session = reader
            .new_session(
                group_id + 1,
                vec![(topic.to_string(), ReadPosition::Latest)],
            )
            .await
            .expect("Failed to create latest reader session");

        let latest_messages = latest_session
            .next(topic, partition_id, NonZero::new(10).unwrap())
            .await;
        match latest_messages {
            Ok(msgs) => {
                println!("[测试] Latest位置读取到 {} 条消息", msgs.len());
                // Latest位置应该读取到较少的消息或没有消息
                assert!(
                    msgs.len() <= all_messages.len(),
                    "Latest位置不应该读取到更多消息"
                );
            }
            Err(e) => {
                println!("[测试] Latest位置读取结果: {:?}", e);
            }
        }

        // 清理测试数据
        drop(session);
        drop(latest_session);
        tokio::time::sleep(Duration::from_millis(500)).await;

        if fs::metadata(test_dir).is_ok() {
            fs::remove_dir_all(test_dir).unwrap();
            println!("[测试] 清理测试数据完成");
        }

        println!("[测试] 所有测试通过！");
    }

    /// 测试commit offset边界条件
    #[tokio::test]
    async fn test_commit_offset_edge_cases() {
        use crate::{
            MessagePayload, ReadPosition, SegmentOffset, StorageWriter,
            disk::{DiskStorageWriterWrapper, default_config},
        };
        use std::fs;
        use tokio::time::Duration;

        let test_dir = "./test_commit_edge_cases_1";
        let topic = "edge_test_topic";
        let partition_id = 0;
        let group_id = 2001;

        // 清理测试数据
        if fs::metadata(test_dir).is_ok() {
            fs::remove_dir_all(test_dir).unwrap();
        }

        // 创建writer
        let mut config = default_config();
        config.storage_dir = PathBuf::from(test_dir);
        config.with_metrics = true;
        let writer = DiskStorageWriterWrapper::new(config).expect("Failed to create writer");

        // 写入一条消息
        let msg = MessagePayload::new(
            "edge_test_msg".to_string(),
            2000,
            Default::default(),
            b"Edge test message".to_vec(),
        );

        writer
            .store(topic, partition_id, vec![msg.clone()], None)
            .await
            .expect("Failed to write message");
        tokio::time::sleep(Duration::from_millis(100)).await;
        writer
            .flush_topic_partition_force(topic, partition_id)
            .await
            .expect("Failed to flush");

        // 关闭writer并等待一段时间，避免锁冲突
        drop(writer);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 创建reader
        let reader = DiskStorageReader::new(PathBuf::from(test_dir), 100);
        let session = reader
            .new_session(group_id, vec![(topic.to_string(), ReadPosition::Begin)])
            .await
            .expect("Failed to create reader session");

        // 读取消息获取offset
        let messages = session
            .next(topic, partition_id, NonZero::new(1).unwrap())
            .await
            .expect("Failed to read messages");
        assert!(!messages.is_empty(), "Should read at least one message");

        let (_, _, offset) = &messages[0];
        println!("[边界测试] 读取到的offset: {:?}", offset);

        // 测试边界条件
        println!("\n[边界测试] 开始测试边界条件...");

        // 1. 测试相同segment_id但offset稍小的commit（应该失败，因为不是有效的消息边界）
        // 注意：如果当前offset是0，则无法测试更小的offset，跳过这个测试
        if offset.offset > 0 {
            let smaller_offset = SegmentOffset {
                segment_id: offset.segment_id,
                offset: offset.offset - 1,
            };
            let result = session.commit(topic, partition_id, smaller_offset).await;
            assert!(result.is_err(), "较小的offset应该失败");
            println!("[边界测试] 较小offset正确失败: {:?}", result.err());
        } else {
            println!("[边界测试] 跳过较小offset测试，因为当前offset为0");
        }

        // 2. 测试相同segment_id但offset稍大的commit（应该失败，因为不是有效的消息边界）
        let larger_offset = SegmentOffset {
            segment_id: offset.segment_id,
            offset: offset.offset + 1,
        };
        let result = session.commit(topic, partition_id, larger_offset).await;
        assert!(result.is_err(), "超出范围的offset应该失败");
        println!("[边界测试] 超出范围offset正确失败: {:?}", result.err());

        // 3. 测试相同segment_id相同offset的commit
        let result = session.commit(topic, partition_id, offset.clone()).await;
        assert!(result.is_ok(), "相同offset应该成功");
        println!("[边界测试] 相同offset成功");

        // 4. 测试segment_id为0的特殊情况
        let zero_offset = SegmentOffset {
            segment_id: 0,
            offset: 0,
        };
        let result = session.commit(topic, partition_id, zero_offset).await;
        // 这个测试的结果取决于当前commit位置，可能成功也可能失败
        println!("[边界测试] 零offset结果: {:?}", result);

        // 清理
        drop(session);
        tokio::time::sleep(Duration::from_millis(100)).await;

        if fs::metadata(test_dir).is_ok() {
            fs::remove_dir_all(test_dir).unwrap();
        }

        println!("[边界测试] 边界条件测试完成");
    }
}
