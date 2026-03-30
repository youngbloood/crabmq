use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::serializer::SerializedMessage;
/// S-G IO 序列化实现
///
/// 零拷贝写入：使用scatter-gather IO，不拷贝原始数据
///
/// 磁盘格式：
/// [1:msg_id_len] + [msg_id]
/// + [8:timestamp]
/// + [4:metadata_len] + [metadata_serialized]
/// + [4:payload_len] + [payload]
///
/// metadata格式：
/// [4:entry_count] + N * ([4:key_len] + [key] + [4:val_len] + [val])
use crate::{MessagePayload, StorageError, StorageResult};
use std::io::IoSlice;

/// S-G IO 序列化实现
pub fn serialize_sg_io<'a>(
    msg: &'a MessagePayload,
    header: &'a mut BytesMut,
) -> StorageResult<SerializedMessage<'a>> {
    // 1. msg_id: [1:len] + [data]
    if msg.msg_id.len() > u8::MAX as usize {
        return Err(StorageError::SerializeError(format!(
            "msg_id too long: {}",
            msg.msg_id.len()
        )));
    }

    // ===== 计算总长度 =====
    // 1: 存储 msg_id 的长度，u8 + len(msg_id)
    let mut total_data_len = 1 + msg.msg_id.len();
    // 8: 存储 timestamp，u64
    total_data_len += 8;
    // 存储 metadata 的数量，u16
    total_data_len += msg.metadata.len() as usize;
    for (k, v) in &msg.metadata {
        // 每个 kv 对需要 1(key_len) + 2(val_len) = 3 字节的 header
        total_data_len += 1 + k.len() + 2 + v.len();
    }
    // 最后 payload 的长度不需要存储
    total_data_len += msg.payload.len();

    // ===== 构建 Header =====
    header.clear();
    {
        header.extend_from_slice(&[msg.msg_id.len() as u8]);
        header.extend_from_slice(&msg.msg_id);
        header.extend_from_slice(&msg.timestamp.to_le_bytes());
        header.extend_from_slice(&(msg.metadata.len() as u16).to_le_bytes());

        for (k, v) in &msg.metadata {
            header.extend_from_slice(&[k.len() as u8]);
            header.extend_from_slice(&k);
            header.extend_from_slice(&(v.len() as u16).to_le_bytes());
            header.extend_from_slice(&v);
        }
    }

    // ===== 构建 IoSlice =====
    let mut iovecs = Vec::with_capacity(2);
    // --- 写入 Header ---
    iovecs.push(IoSlice::new(&header[..]));
    // --- 放入 Payload ---
    if !msg.payload.is_empty() {
        iovecs.push(IoSlice::new(&msg.payload));
    }

    Ok(SerializedMessage {
        iovecs,
        total_len: total_data_len as u64,
    })
}

/// 反序列化消息
///
/// 从字节流解析消息，实现全链路零拷贝
///
/// 不包含落盘的 8 bytes 长度头
pub fn deserialize_sg_io(mut data: Bytes) -> StorageResult<MessagePayload> {
    use bytes::Buf;

    // 解析 msg_id
    let msg_id_len = data.get_u8() as usize;
    let msg_id = data.split_to(msg_id_len);

    // 解析 timestamp
    let timestamp = data.get_u64_le();

    // 解析 metadata 数量
    let metadata_len = data.get_u16_le() as usize;

    let mut metadata = SmallVec::new();
    for _ in 0..metadata_len {
        let key_len = data.get_u8() as usize;
        let key = data.split_to(key_len);
        let val_len = data.get_u16_le() as usize;
        let val = data.split_to(val_len);
        metadata.push((key, val));
    }

    Ok(MessagePayload {
        msg_id,
        timestamp,
        metadata,
        payload: data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sg_io_roundtrip() -> StorageResult<()> {
        let mut metadata = Vec::new();
        metadata.push((Bytes::from("key1"), Bytes::from("value1")));
        metadata.push((Bytes::from("key2"), Bytes::from("value2")));

        let msg = MessagePayload::new(
            Bytes::from("test_msg_123"),
            1234567890,
            metadata,
            Bytes::from(vec![0xAB; 100]),
        );

        // 序列化
        let mut headers = BytesMut::new();
        let serialized = serialize_sg_io(&msg, &mut headers)?;

        // 收集数据
        let mut buf = BytesMut::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        assert_eq!(buf.len(), serialized.total_len as usize);

        // 反序列化
        let deserialized = deserialize_sg_io(buf.freeze())?;

        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert_eq!(msg.timestamp, deserialized.timestamp);
        assert_eq!(msg.metadata, deserialized.metadata);
        assert_eq!(msg.payload, deserialized.payload);

        Ok(())
    }

    #[test]
    fn test_empty_metadata() -> StorageResult<()> {
        let msg = MessagePayload::new(
            Bytes::from("test"),
            12345,
            Vec::new(),
            Bytes::from(vec![1, 2, 3]),
        );

        let mut headers = BytesMut::new();
        let serialized = serialize_sg_io(&msg, &mut headers)?;

        let mut buf = BytesMut::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        let deserialized = deserialize_sg_io(buf.freeze())?;
        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert!(deserialized.metadata.is_empty());

        Ok(())
    }

    #[test]
    fn test_msg_id_too_long() {
        let msg = MessagePayload::new(
            Bytes::from("a".repeat(256)),
            12345,
            Vec::new(),
            Bytes::from(vec![]),
        );

        let mut headers = BytesMut::new();
        let result = serialize_sg_io(&msg, &mut headers);
        assert!(result.is_err());
    }
}
