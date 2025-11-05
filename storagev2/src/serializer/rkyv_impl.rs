/// rkyv 序列化实现（保留用于性能对比）
///
/// 磁盘格式：[8:len] + [rkyv_data]
///
/// 特点：
/// - 需要拷贝数据到AlignedVec（450-12000ns）
/// - 需要8字节长度头
/// - 反序列化略慢于S-G IO

use crate::{MessagePayload, StorageError, StorageResult};
use crate::serializer::SerializedMessage;
use std::io::IoSlice;

/// 序列化消息（使用rkyv）
///
/// 注意：这个实现会拷贝数据！
/// - msg_id、metadata、payload 会被拷贝到AlignedVec
/// - 不是零拷贝
/// - 每次都重新序列化，不缓存（rkyv主要用于性能对比）
pub fn serialize_rkyv<'a>(
    msg: &'a MessagePayload,
    headers: &'a mut Vec<Vec<u8>>,
) -> StorageResult<SerializedMessage<'a>> {
    // 直接使用 rkyv 序列化
    let aligned_vec = rkyv::to_bytes::<rkyv::rancor::Error>(msg)
        .map_err(|e| StorageError::SerializeError(e.to_string()))?;

    let data_len = aligned_vec.len() as u64;

    // 创建8字节长度头
    let len_header = data_len.to_le_bytes().to_vec();
    headers.push(len_header);

    // rkyv 数据（从 AlignedVec 转换为 Vec）
    headers.push(aligned_vec.to_vec());

    // 创建IoSlice数组
    let mut iovecs = Vec::with_capacity(2);
    iovecs.push(IoSlice::new(&headers[headers.len() - 2])); // 长度头
    iovecs.push(IoSlice::new(&headers[headers.len() - 1])); // rkyv数据

    Ok(SerializedMessage {
        iovecs,
        total_len: 8 + data_len,
    })
}

/// 反序列化消息（使用rkyv）
pub fn deserialize_rkyv(data: &[u8]) -> StorageResult<MessagePayload> {
    if data.len() < 8 {
        return Err(StorageError::SerializeError("data too short for rkyv".to_string()));
    }

    // 读取长度头
    let expected_len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;

    if data.len() != 8 + expected_len {
        return Err(StorageError::SerializeError(
            format!("data length mismatch: expected {}, got {}", 8 + expected_len, data.len())
        ));
    }

    // 反序列化rkyv数据
    let rkyv_data = &data[8..];
    MessagePayload::from_rkyv_bytes(rkyv_data)
        .map_err(|e| StorageError::SerializeError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_rkyv_roundtrip() -> StorageResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());

        let msg = MessagePayload::new(
            "test_msg".to_string(),
            1234567890,
            metadata,
            vec![0xAB; 100],
        );

        // 序列化
        let mut headers = Vec::new();
        let serialized = serialize_rkyv(&msg, &mut headers)?;

        // 收集数据
        let mut buf = Vec::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        assert_eq!(buf.len(), serialized.total_len as usize);

        // 反序列化
        let deserialized = deserialize_rkyv(&buf)?;

        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert_eq!(msg.timestamp, deserialized.timestamp);
        assert_eq!(msg.metadata, deserialized.metadata);
        assert_eq!(msg.payload, deserialized.payload);

        Ok(())
    }
}
