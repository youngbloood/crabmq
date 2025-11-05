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
use crate::serializer::SerializedMessage;
use std::io::IoSlice;

/// 序列化消息（零拷贝）
///
/// # 零拷贝保证
/// - msg_id.as_bytes(): 只获取引用，不拷贝
/// - timestamp.to_le_bytes(): 8字节临时数组，存储在headers中
/// - metadata: 序列化到headers中，但key/value数据用IoSlice引用
/// - payload: 直接用IoSlice引用，不拷贝
///
/// # 参数
/// - msg: 消息对象的引用
/// - headers: 可变Vec，用于存储长度头等临时数据（生命周期与msg一致）
pub fn serialize_sg_io<'a>(
    msg: &'a MessagePayload,
    headers: &'a mut Vec<Vec<u8>>,
) -> StorageResult<SerializedMessage<'a>> {
    // 1. msg_id: [1:len] + [data]
    if msg.msg_id.len() > 255 {
        return Err(StorageError::SerializeError(
            format!("msg_id too long: {}", msg.msg_id.len())
        ));
    }

    let msg_id_len_header = vec![msg.msg_id.len() as u8];
    let msg_id_bytes = msg.msg_id.as_bytes();

    // 2. timestamp: [8:u64]
    let timestamp_bytes = msg.timestamp.to_le_bytes().to_vec();

    // 3. metadata: 先序列化到一个临时缓冲区
    // 格式: [4:total_len] + [4:count] + entries
    let mut metadata_buf = Vec::new();

    // 占位total_len
    metadata_buf.extend_from_slice(&[0u8; 4]);

    // entry_count
    metadata_buf.extend_from_slice(&(msg.metadata.len() as u32).to_le_bytes());

    // 每个entry
    for (key, val) in &msg.metadata {
        metadata_buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        metadata_buf.extend_from_slice(key.as_bytes());
        metadata_buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        metadata_buf.extend_from_slice(val.as_bytes());
    }

    // 填充total_len（不包括长度头本身）
    let metadata_content_len = (metadata_buf.len() - 4) as u32;
    metadata_buf[0..4].copy_from_slice(&metadata_content_len.to_le_bytes());

    // 4. payload: [4:len] + [data]
    let payload_len_header = (msg.payload.len() as u32).to_le_bytes().to_vec();

    // ===== 阶段1：收集所有headers =====
    let header_start_idx = headers.len();
    headers.push(msg_id_len_header);     // idx 0
    headers.push(timestamp_bytes);        // idx 1
    headers.push(metadata_buf);           // idx 2
    headers.push(payload_len_header);     // idx 3

    // ===== 阶段2：创建IoSlice数组 =====
    let mut iovecs = Vec::with_capacity(8);
    let mut total_len = 0u64;

    // msg_id
    iovecs.push(IoSlice::new(&headers[header_start_idx + 0]));
    iovecs.push(IoSlice::new(msg_id_bytes));
    total_len += 1 + msg_id_bytes.len() as u64;

    // timestamp
    iovecs.push(IoSlice::new(&headers[header_start_idx + 1]));
    total_len += 8;

    // metadata
    iovecs.push(IoSlice::new(&headers[header_start_idx + 2]));
    total_len += headers[header_start_idx + 2].len() as u64;

    // payload
    iovecs.push(IoSlice::new(&headers[header_start_idx + 3]));
    iovecs.push(IoSlice::new(&msg.payload));
    total_len += 4 + msg.payload.len() as u64;

    Ok(SerializedMessage {
        iovecs,
        total_len,
    })
}

/// 反序列化消息
///
/// 从字节流解析消息
pub fn deserialize_sg_io(data: &[u8]) -> StorageResult<MessagePayload> {
    let mut offset = 0;

    // 1. msg_id
    if offset >= data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at msg_id_len".to_string()));
    }
    let msg_id_len = data[offset] as usize;
    offset += 1;

    if offset + msg_id_len > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at msg_id".to_string()));
    }
    let msg_id = String::from_utf8(data[offset..offset + msg_id_len].to_vec())
        .map_err(|e| StorageError::SerializeError(format!("invalid msg_id utf8: {}", e)))?;
    offset += msg_id_len;

    // 2. timestamp
    if offset + 8 > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at timestamp".to_string()));
    }
    let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // 3. metadata
    if offset + 4 > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at metadata_len".to_string()));
    }
    let metadata_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let metadata_end = offset + metadata_len;
    if metadata_end > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at metadata".to_string()));
    }

    // 解析metadata
    let mut metadata = std::collections::HashMap::new();

    if offset + 4 > metadata_end {
        return Err(StorageError::SerializeError("unexpected EOF at entry_count".to_string()));
    }
    let entry_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    for _ in 0..entry_count {
        // key
        if offset + 4 > metadata_end {
            return Err(StorageError::SerializeError("unexpected EOF at key_len".to_string()));
        }
        let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + key_len > metadata_end {
            return Err(StorageError::SerializeError("unexpected EOF at key".to_string()));
        }
        let key = String::from_utf8(data[offset..offset + key_len].to_vec())
            .map_err(|e| StorageError::SerializeError(format!("invalid key utf8: {}", e)))?;
        offset += key_len;

        // value
        if offset + 4 > metadata_end {
            return Err(StorageError::SerializeError("unexpected EOF at val_len".to_string()));
        }
        let val_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + val_len > metadata_end {
            return Err(StorageError::SerializeError("unexpected EOF at val".to_string()));
        }
        let val = String::from_utf8(data[offset..offset + val_len].to_vec())
            .map_err(|e| StorageError::SerializeError(format!("invalid val utf8: {}", e)))?;
        offset += val_len;

        metadata.insert(key, val);
    }

    offset = metadata_end;

    // 4. payload
    if offset + 4 > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at payload_len".to_string()));
    }
    let payload_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    if offset + payload_len > data.len() {
        return Err(StorageError::SerializeError("unexpected EOF at payload".to_string()));
    }
    let payload = data[offset..offset + payload_len].to_vec();

    Ok(MessagePayload::new(msg_id, timestamp, metadata, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sg_io_roundtrip() -> StorageResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let msg = MessagePayload::new(
            "test_msg_123".to_string(),
            1234567890,
            metadata,
            vec![0xAB; 100],
        );

        // 序列化
        let mut headers = Vec::new();
        let serialized = serialize_sg_io(&msg, &mut headers)?;

        // 收集数据
        let mut buf = Vec::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        assert_eq!(buf.len(), serialized.total_len as usize);

        // 反序列化
        let deserialized = deserialize_sg_io(&buf)?;

        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert_eq!(msg.timestamp, deserialized.timestamp);
        assert_eq!(msg.metadata, deserialized.metadata);
        assert_eq!(msg.payload, deserialized.payload);

        Ok(())
    }

    #[test]
    fn test_empty_metadata() -> StorageResult<()> {
        let msg = MessagePayload::new(
            "test".to_string(),
            12345,
            HashMap::new(),
            vec![1, 2, 3],
        );

        let mut headers = Vec::new();
        let serialized = serialize_sg_io(&msg, &mut headers)?;

        let mut buf = Vec::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        let deserialized = deserialize_sg_io(&buf)?;
        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert!(deserialized.metadata.is_empty());

        Ok(())
    }

    #[test]
    fn test_msg_id_too_long() {
        let msg = MessagePayload::new(
            "a".repeat(256),
            12345,
            HashMap::new(),
            vec![],
        );

        let mut headers = Vec::new();
        let result = serialize_sg_io(&msg, &mut headers);
        assert!(result.is_err());
    }
}
