pub mod disk;
// pub mod mem;
pub mod metrics;
pub mod serializer;
use bytes::Bytes;
// pub use mem::*;
pub mod err;

use crate::err::{ErrorCode, StorageError, StorageResult};
use async_trait::async_trait;
use smallvec::SmallVec;
use std::num::NonZero;
use tokio::{fs::File, sync::oneshot};

const VERSION_V1: u8 = 1;

enum CompressionType {
    None = 0,
    Lz4 = 1,
    Snappy = 2,
    Gzip = 3,
    Zstd = 4,
}

impl From<u8> for CompressionType {
    fn from(value: u8) -> Self {
        match value {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Snappy,
            3 => CompressionType::Gzip,
            4 => CompressionType::Zstd,
            _ => CompressionType::None,
        }
    }
}

enum SerializationType {
    SgIo = 0,
    Rkyv = 1,
    Json = 2,
    Protobuf = 3,
    Avro = 4,
    Thrift = 5,
    Msgpack = 6,
    Capnproto = 7,
}

impl From<u8> for SerializationType {
    fn from(value: u8) -> Self {
        match value {
            0 => SerializationType::SgIo,
            1 => SerializationType::Rkyv,
            2 => SerializationType::Json,
            3 => SerializationType::Protobuf,
            4 => SerializationType::Avro,
            5 => SerializationType::Thrift,
            6 => SerializationType::Msgpack,
            7 => SerializationType::Capnproto,
            _ => SerializationType::SgIo,
        }
    }
}

pub trait MessageSize {
    fn get_size(&self) -> u32;
}

/**
 * # Attributes
 * u8:
 *  0-3 bits: compression type
 *  4-7 bits: serialization type
 *
 * u8:
 *  8 bits: reserved
 */
#[derive(Debug, Clone)]
struct Attributes([u8; 2]);

impl From<[u8; 2]> for Attributes {
    fn from(value: [u8; 2]) -> Self {
        Self(value)
    }
}

impl Attributes {
    pub fn new() -> Self {
        Self([0; 2])
    }

    pub fn with_comporess(&mut self, compress: CompressionType) -> &mut Self {
        let c = (compress as u8) << 4;
        self.0[0] |= c;
        self
    }

    pub fn with_serialization(&mut self, serialization: SerializationType) -> &mut Self {
        let c = (serialization as u8) << 4 >> 4;
        self.0[0] |= c;
        self
    }

    pub fn get_compression(&self) -> CompressionType {
        CompressionType::from(self.0[0] >> 4)
    }

    pub fn get_serialization(&self) -> SerializationType {
        SerializationType::from(self.0[0] << 4 >> 4)
    }
}
/**
 * # MessagePayload
 * **version**: 消息版本，用于标识消息的版本
 * **check_sum**: 校验和，用于验证消息的完整性
 * **attributes**: 消息属性，用于标识消息的属性
 *   - compression: 压缩类型
 *   - serialization: 序列化类型
 * **msg_id**: 消息id，用于唯一标识消息
 * **timestamp**: 消息时间戳，用于消息的排序
 * **metadata**: 消息元数据，用于存储消息的元数据
 * **metadata.key**: 消息元数据键，用于存储消息的元数据键
 * **metadata.value**: 消息元数据值，用于存储消息的元数据值
 * **payload**: 消息负载，用于存储消息的负载数据
 *
 *
 * # 消息负载结构
 *
 * 磁盘格式：
 * 1byte(u8): version
 *
 * 4bytes(u32): check_sum
 *
 * 2bytes(u16): attributes
 *
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
    version: u8,

    // CRC 校验码, 以下字段均参与校验
    check_sum: u32,

    attributes: Attributes,

    // 公开字段以支持直接访问和修改
    msg_id: Bytes,
    timestamp: u64,
    // Key: u8, 最长 255
    // Value: u16, 最长 255
    metadata: SmallVec<[(Bytes, Bytes); 5]>,
    payload: Bytes,
}

impl MessagePayload {
    /**
     * New a MessagePayload
     *
     * @param msg_id: the message id, msg_id length between 1 and 255
     *
     * @param timestamp: the message timestamp, timestamp must be greater than 0
     *
     * @param metadata: the message metadata, metadata length between 0 and 255
     * @param metadata_key: the message metadata key, metadata key length between 1 and 255
     * @param metadata_value: the message metadata value, metadata value length between 1 and u16::MAX
     *
     * @param payload: the message payload, payload length between 1 and u32::MAX
     *
     * @return: a MessagePayload
     */
    pub fn new_v1(
        msg_id: Bytes,
        timestamp: u64,
        metadata: Vec<(Bytes, Bytes)>,
        payload: Bytes,
    ) -> Self {
        let metadata = SmallVec::from_vec(metadata);
        let mut mp = Self {
            version: VERSION_V1,
            check_sum: 0,
            attributes: Attributes::new(),
            msg_id,
            timestamp,
            metadata,
            payload,
        };
        mp.check_sum = mp.calc_check_sum();
        mp
    }

    pub fn with_comporess(&mut self, compress: CompressionType) -> &mut Self {
        self.attributes.with_comporess(compress);
        self
    }

    pub fn with_serialization(&mut self, serialization: SerializationType) -> &mut Self {
        self.attributes.with_serialization(serialization);
        self
    }

    fn calc_check_sum(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.attributes.0[..]);
        hasher.update(self.msg_id.as_ref());
        hasher.update(&self.timestamp.to_le_bytes());
        self.metadata.iter().for_each(|(k, v)| {
            hasher.update(k.as_ref());
            hasher.update(v.as_ref());
        });
        hasher.update(self.payload.as_ref());
        hasher.finalize()
    }

    pub(crate) fn validate_check_sum(&self, check_sum: u32) -> StorageResult<()> {
        if self.check_sum != check_sum {
            return Err(StorageError::new(ErrorCode::CheckSumMismatch));
        }
        Ok(())
    }

    // /// 从 rkyv 序列化的字节反序列化（用于性能测试和向后兼容）
    // pub(crate) fn from_rkyv_bytes(data: Bytes) -> Result<Self> {
    //     let inner: MessagePayloadInner =
    //         rkyv::from_bytes::<MessagePayloadInner, rkyv::rancor::Error>(data.as_ref())
    //             .map_err(|e| anyhow::anyhow!("rkyv deserialize error: {}", e))?;
    //     Ok(Self::new(
    //         Bytes::from(inner.msg_id),
    //         inner.timestamp,
    //         inner
    //             .metadata
    //             .into_iter()
    //             .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
    //             .collect(),
    //         Bytes::from(inner.payload),
    //     ))
    // }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        if self.msg_id.len() < 1 {
            return Err(StorageError::new(ErrorCode::MsgIDTooShort));
        }
        if self.msg_id.len() > u8::MAX as usize {
            return Err(StorageError::new(ErrorCode::MsgIDTooLong));
        }

        if self.timestamp == 0 {
            return Err(StorageError::new(ErrorCode::TimestampInvalid));
        }

        if self.metadata.len() > u8::MAX as usize {
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

        if self.payload.is_empty() {
            return Err(StorageError::new(ErrorCode::PayloadTooShort));
        }
        if self.payload.len() > u32::MAX as usize {
            return Err(StorageError::new(ErrorCode::PayloadTooLong));
        }

        Ok(())
    }

    fn get_size(&self) -> u32 {
        (1 + 4
            + 2
            + self.msg_id.len()
            + 8
            + self
                .metadata
                .iter()
                .map(|(k, v)| 1 + k.len() + 2 + v.len())
                .sum::<usize>()
            + self.payload.len()) as u32
    }
}

pub(crate) struct MessagePayloadWithSize {
    payload: MessagePayload,
    size: u32,
}

impl MessageSize for MessagePayloadWithSize {
    fn get_size(&self) -> u32 {
        self.size
    }
}

#[async_trait]
pub trait StorageSearch {
    /**
     * Search the message by logic sequence, and return the message payload
     */
    async fn search(
        &self,
        topic: &str,
        partition_id: u32,
        logic_seq: u64,
    ) -> StorageResult<MessagePayload>;
}

#[async_trait]
pub trait StorageWriter: Send + Sync + Clone + 'static + StorageSearch {
    /**
     * Store the message to the Storage Media
     * @param topic: the topic name
     * @param partition: the partition id
     * @param payloads: the message payloads to store
     * @param notify: the notify channel to notify the result of the operation, if None, the operation will be performed in fire-and-forget mode
     * @return: the start of the payloads's logic_sequence.
     */
    async fn store(
        &self,
        topic: &str,
        partition: u32,
        payloads: Vec<MessagePayload>,
        notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<u64>;
}

#[async_trait]
pub trait StorageReader: Send + Sync + Clone + 'static {
    /// New a session with group_id, it will be return Err() when session has been created.
    async fn new_session(
        &self,
        group_id: &str,
        read_position: Vec<(String, ConsumerReaderPositionType)>, // 该 consumer-grpup 指定消费的 topic 的位置
    ) -> StorageResult<Box<dyn StorageReaderSession>>;

    /// Close a session by group_id.
    async fn close_session(&self, group_id: &str);
}

#[async_trait]
pub trait StorageReaderSession: Send + Sync + 'static + StorageSearch {
    /**
     * Get the next n message in the topic-partition from disk
     *
     * @param topic: the topic name
     * @param partition: the partition id
     * @param n: the number of messages to get
     * @return: a vector of (MessagePayload, u64, SegmentOffset)
     *          the first element is the message payload,
     *          the second element is the logic_seq,
     */
    async fn next(
        &self,
        topic: &str,
        partition_id: u32,
        n: NonZero<u64>,
    ) -> StorageResult<Vec<(MessagePayload, u64)>>;

    async fn next_fd(&self, topic: &str, partition: u32, n: NonZero<u64>) -> StorageResult<File>;

    /**
     * Commit the message has been consumed, and the consume ptr should rorate the next ptr.
     *
     * @param topic: the topic name
     * @param partition: the partition id
     * @param logic_seq: the logic sequence of the message
     * @return: a result of the operation
     */
    async fn commit(&self, topic: &str, partition: u32, logic_seq: u64) -> StorageResult<()>;
}

#[derive(Default, Debug, Clone, PartialEq, PartialOrd)]
pub struct SegmentOffset {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConsumerReaderPositionType {
    Earliest, // 从头开始消费
    Latest,   // 从最新消息开始消费，以第一次调用next为快照
}
