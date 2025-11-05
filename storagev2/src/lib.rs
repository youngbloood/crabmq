pub mod disk;
pub mod mem;
pub mod metrics;
pub use mem::*;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::{collections::HashMap, num::NonZero, sync::Arc};
use tokio::sync::oneshot;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MessageMeta {
    pub msg_id: String,
    pub timestamp: u64,
    pub segment_id: u64,
    pub offset: u64,
    pub msg_len: u32,
}

// 内部数据结构：只包含数据，可被 rkyv 序列化
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub(crate) struct MessagePayloadInner {
    pub msg_id: String,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
    pub payload: Vec<u8>,
}

// 外部结构：数据 + 缓存
// TODO: 使用 rkyv 替换 bincode, 使用 rkyv 的 string 类型，msg_id 使用零拷贝
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct MessagePayload {
    // 公开字段以支持直接访问和修改
    pub msg_id: String,
    pub timestamp: u64,
    pub metadata: HashMap<String, String>,
    pub payload: Vec<u8>,

    // 私有缓存：序列化结果和长度，序列化时跳过
    #[rkyv(with = rkyv::with::Skip)]
    cached: Arc<std::sync::Mutex<Option<(Bytes, usize)>>>,
}

impl MessagePayload {
    pub fn new(
        msg_id: String,
        timestamp: u64,
        metadata: HashMap<String, String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            msg_id,
            timestamp,
            metadata,
            payload,
            cached: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// 从 rkyv 序列化的字节反序列化（用于性能测试）
    pub fn from_rkyv_bytes(data: &[u8]) -> Result<Self> {
        let inner: MessagePayloadInner =
            rkyv::from_bytes::<MessagePayloadInner, rkyv::rancor::Error>(data)
                .map_err(|e| anyhow::anyhow!("rkyv deserialize error: {}", e))?;
        Ok(Self::new(
            inner.msg_id,
            inner.timestamp,
            inner.metadata,
            inner.payload,
        ))
    }
}

impl MessagePayload {
    pub fn to_bytes(&self) -> Result<Bytes> {
        // ✅ 检查缓存
        {
            let cache = self.cached.lock().unwrap();
            if let Some((bytes, _len)) = cache.as_ref() {
                // 缓存命中：Bytes::clone() 只增加引用计数，零拷贝
                return Ok(bytes.clone());
            }
        }

        // ✅ 缓存未命中：直接序列化 self，cached 字段被跳过，零拷贝
        let aligned_vec = rkyv::to_bytes::<rkyv::rancor::Error>(self)?;
        // ✅ 零拷贝：from_owner 接管 AlignedVec 所有权
        let bytes = Bytes::from_owner(aligned_vec);
        let len = bytes.len();

        // 保存到缓存
        {
            let mut cache = self.cached.lock().unwrap();
            *cache = Some((bytes.clone(), len));
        }

        Ok(bytes)
    }

    pub(crate) fn to_vec(&self) -> Result<Vec<u8>> {
        let bytes = self.to_bytes()?;
        // ⚠️ 这里会拷贝一次，但只在真正需要 Vec 时才调用（实际很少使用）
        Ok(bytes.to_vec())
    }

    pub(crate) fn len(&self) -> Result<usize> {
        // 直接调用 to_bytes().len()
        // 如果已缓存，to_bytes() 返回很快；如果未缓存，会序列化并缓存
        let bytes = self.to_bytes()?;
        Ok(bytes.len())
    }

    pub(crate) fn gen_meta(&self, segment_id: u64, offset: u64) -> MessageMeta {
        MessageMeta {
            msg_id: self.msg_id.clone(),
            timestamp: self.timestamp,
            segment_id,
            offset,
            // 这里直接用 payload 长度，避免重复序列化
            msg_len: self.payload.len() as u32,
        }
    }
}

#[async_trait]
pub trait StorageWriter: Send + Sync + Clone + 'static {
    /// Store the message to the Storage Media
    async fn store(
        &self,
        topic: &str,
        partition: u32,
        payloads: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<()>;
}

#[async_trait]
pub trait StorageReader: Send + Sync + Clone + 'static {
    /// New a session with group_id, it will be return Err() when session has been created.
    async fn new_session(
        &self,
        group_id: u32,
        read_position: Vec<(String, ReadPosition)>, // 该 consumer-grpup 指定消费的 topic 的位置
    ) -> StorageResult<Box<dyn StorageReaderSession>>;

    /// Close a session by group_id.
    async fn close_session(&self, group_id: u32);
}

#[async_trait]
pub trait StorageReaderSession: Send + Sync + 'static {
    /// Get the next n message
    async fn next(
        &self,
        topic: &str,
        partition: u32,
        n: NonZero<u64>,
    ) -> StorageResult<Vec<(MessagePayload, u64, SegmentOffset)>>;

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32, offset: SegmentOffset)
    -> StorageResult<()>;
}

#[derive(Default, Debug, Clone)]
pub struct SegmentOffset {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadPosition {
    Begin,  // 从头开始消费
    Latest, // 从最新消息开始消费，以第一次调用next为快照
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    TopicNotFound(String),     // 主题不存在
    PartitionNotFound(String), // 分区不存在
    RecordNotFound(String),    // 记录不存在
    PathNotExist(String),      // 路径不存在
    EmptyData,                 // 写入数据为空
    IoError(String),           // IO 错误
    SerializeError(String),    // 序列化/反序列化错误
    DiskFull,                  // 磁盘空间不足
    PermissionDenied,          // 权限不足
    NoMoreMessages(String),    // 没有更多消息
    OffsetMismatch(String),    // Offset 不匹配
    Unknown(String),           // 其他未知错误
}

impl ToString for StorageError {
    fn to_string(&self) -> String {
        match self {
            StorageError::TopicNotFound(key) => format!("[{}]: topic not found", key),
            StorageError::PartitionNotFound(key) => format!("[{}]: partition not found", key),
            StorageError::RecordNotFound(key) => format!("[{}]: record not found", key),
            StorageError::PathNotExist(key) => format!("[{}]: path not exist", key),
            StorageError::EmptyData => "empty data".to_string(),
            StorageError::IoError(e) => format!("io error: {}", e),
            StorageError::SerializeError(e) => format!("serialize error: {}", e),
            StorageError::DiskFull => "disk full".to_string(),
            StorageError::PermissionDenied => "permission denied".to_string(),
            StorageError::NoMoreMessages(e) => format!("no more messages: {}", e),
            StorageError::OffsetMismatch(e) => format!("offset mismatch: {}", e),
            StorageError::Unknown(e) => format!("unknown error: {}", e),
        }
    }
}
