pub mod disk;
pub mod mem;
pub mod metrics;

use std::{num::NonZero};

pub use mem::*;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::oneshot;

#[async_trait]
pub trait StorageWriter: Send + Sync + Clone + 'static {
    /// Store the message to the Storage Media
    async fn store(&self, topic: &str, partition: u32, datas: &[Bytes], notify: Option<oneshot::Sender<StorageResult<()>>>) -> StorageResult<()>;
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
    ) -> StorageResult<Vec<(Bytes, SegmentOffset)>>;

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32, offset: SegmentOffset) -> StorageResult<()>;
}

#[derive(Default, Debug)]
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
    TopicNotFound(String),         // 主题不存在
    PartitionNotFound(String),     // 分区不存在
    RecordNotFound(String),        // 记录不存在
    PathNotExist(String),          // 路径不存在
    EmptyData,                     // 写入数据为空
    IoError(String),               // IO 错误
    SerializeError(String),        // 序列化/反序列化错误
    DiskFull,                      // 磁盘空间不足
    PermissionDenied,              // 权限不足
    NoMoreMessages(String),        // 没有更多消息
    Unknown(String),               // 其他未知错误
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
            StorageError::Unknown(e) => format!("unknown error: {}", e),
        }
    }
}
