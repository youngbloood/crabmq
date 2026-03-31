pub mod disk;
pub mod mem;
pub mod metrics;
pub mod serializer;
use bytes::Bytes;
pub use mem::*;

pub mod err;

use anyhow::Result;
use async_trait::async_trait;
use rkyv::{Archive, Deserialize, Serialize};
use smallvec::SmallVec;
use std::{collections::HashMap, num::NonZero};
use tokio::sync::oneshot;

use crate::err::{ErrorCode, StorageError, StorageResult};

#[derive(Debug, Clone)]
pub struct MessageMeta {
    pub msg_id: Bytes,
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

/**
 * 消息负载结构
 *
 * 磁盘格式：
 * 8bytes(u64): total_len: 该消息的总长
 *
 * msg_id:
 * 1byte(u8) + nbytes(msg_id): msg_id 的长度 + msg_id
 * timestamp:
 * 8bytes(u64): timestamp
 *
 * metadata_len:
 * u16(2bytes): metadata 的数量
 *
 * payload_len:
 * u32(4bytes): payload 的长度
 *
 * payload:
 * nbytes(payload): payload
 */
#[derive(Debug, Clone)]
pub struct MessagePayload {
    // 公开字段以支持直接访问和修改
    pub msg_id: Bytes,
    pub timestamp: u64,
    // Key: u8, 最长 255
    // Value: u16, 最长 255
    pub(crate) metadata: SmallVec<[(Bytes, Bytes); 5]>,
    pub payload: Bytes,
}

impl MessagePayload {
    pub fn new(
        msg_id: Bytes,
        timestamp: u64,
        metadata: Vec<(Bytes, Bytes)>,
        payload: Bytes,
    ) -> Self {
        let metadata = SmallVec::from_vec(metadata);
        Self {
            msg_id,
            timestamp,
            metadata,
            payload,
        }
    }

    /// 从 rkyv 序列化的字节反序列化（用于性能测试和向后兼容）
    pub fn from_rkyv_bytes(data: Bytes) -> Result<Self> {
        let inner: MessagePayloadInner =
            rkyv::from_bytes::<MessagePayloadInner, rkyv::rancor::Error>(data.as_ref())
                .map_err(|e| anyhow::anyhow!("rkyv deserialize error: {}", e))?;
        Ok(Self::new(
            Bytes::from(inner.msg_id),
            inner.timestamp,
            inner
                .metadata
                .into_iter()
                .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
                .collect(),
            Bytes::from(inner.payload),
        ))
    }

    pub(crate) fn gen_meta(&self, segment_id: u64, offset: u64) -> MessageMeta {
        MessageMeta {
            msg_id: self.msg_id.clone(),
            timestamp: self.timestamp,
            segment_id,
            offset,
            // payload 字段的原始长度（不是序列化后的长度）
            msg_len: self.payload.len() as u32,
        }
    }

    pub fn validate(&self) -> StorageResult<()> {
        if self.msg_id.len() < 1 {
            return Err(StorageError::new(ErrorCode::MsgIDTooShort));
        }
        if self.msg_id.len() > u8::MAX as usize {
            return Err(StorageError::new(ErrorCode::MsgIDTooLong));
        }
        if self.metadata.len() > u16::MAX as usize {
            return Err(StorageError::new(ErrorCode::MetadataTooMany));
        }
        for (k, v) in &self.metadata {
            if k.len() > u8::MAX as usize {
                return Err(StorageError::new(ErrorCode::MetadataKeyTooLong));
            }
            if v.len() > u16::MAX as usize {
                return Err(StorageError::new(ErrorCode::MetadataValueTooLong));
            }
        }
        if self.timestamp == 0 {
            return Err(StorageError::new(ErrorCode::TimestampInvalid));
        }
        if self.payload.is_empty() {
            return Err(StorageError::new(ErrorCode::PayloadTooShort));
        }
        if self.payload.len() > u32::MAX as usize {
            return Err(StorageError::new(ErrorCode::PayloadTooLong));
        }

        Ok(())
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

#[derive(Default, Debug, Clone, PartialEq, PartialOrd)]
pub struct SegmentOffset {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadPosition {
    Begin,  // 从头开始消费
    Latest, // 从最新消息开始消费，以第一次调用next为快照
}
