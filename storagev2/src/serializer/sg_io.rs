use bytes::{Bytes, BytesMut};

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
    header: &'a mut BytesMut,
) -> StorageResult<SerializedMessage<'a>> {
    // 1. msg_id: [1:len] + [data]
    if msg.msg_id.len() > 255 {
        return Err(StorageError::SerializeError(format!(
            "msg_id too long: {}",
            msg.msg_id.len()
        )));
    }

    header.clear();

    // 1. 预计算总长度和所需的 IoSlice 数量
    // 基础长度：总长(4) + 时间戳(8) + msg_id长(2) + metadata数量(2) + payload长(4) = 20 字节
    let mut total_data_len = 20 + msg.msg_id.len() + msg.payload.len();

    // 每个 kv 对需要 2(key_len) + 4(val_len) = 6 字节的 header
    total_data_len += msg.metadata.len() * 6;
    for (k, v) in &msg.metadata {
        total_data_len += k.len() + v.len();
    }
    // 预估 IoSlice 数量：
    // 1(主Header) + 1(msg_id) + N*(1(kv_header) + 1(key) + 1(val)) + 1(payload_header) + 1(payload)
    // 为了简化，我们把所有的 header 字节都紧凑地写在 header_buf 里，
    // 然后在 iovecs 中按需切片引用 header_buf。
    let iovec_capacity = 2 + (msg.metadata.len() * 2) + 1;
    let mut iovecs = Vec::with_capacity(iovec_capacity);
    // ==========================================
    // 2. 开始构建 Header Buffer 和 IoSlice 数组
    // ==========================================
    // 记录当前在 header_buf 中的写入偏移量
    let mut offset = 0;
    // --- 写入主 Header ---
    header.extend_from_slice(&(total_data_len as u32).to_le_bytes());
    header.extend_from_slice(&msg.timestamp.to_le_bytes());
    header.extend_from_slice(&(msg.msg_id.len() as u16).to_le_bytes());
    header.extend_from_slice(&(msg.metadata.len() as u16).to_le_bytes());

    // 将主 Header 作为第一个 IoSlice
    iovecs.push(IoSlice::new(&header[offset..offset + 16]));
    offset += 16;
    // --- 放入 msg_id 数据 ---
    if !msg.msg_id.is_empty() {
        iovecs.push(IoSlice::new(&msg.msg_id));
    }
    // --- 遍历 Metadata ---
    for (k, v) in &msg.metadata {
        let start = offset;
        header.extend_from_slice(&(k.len() as u16).to_le_bytes());
        header.extend_from_slice(&(v.len() as u32).to_le_bytes());

        // 将这对 KV 的长度 Header 作为一个 IoSlice
        iovecs.push(IoSlice::new(&header[start..start + 6]));
        offset += 6;
        // 放入 Key 和 Value 的真实数据
        if !k.is_empty() {
            iovecs.push(IoSlice::new(k));
        }
        if !v.is_empty() {
            iovecs.push(IoSlice::new(v));
        }
    }
    // --- 写入 Payload Header ---
    let start = offset;
    header.extend_from_slice(&(msg.payload.len() as u32).to_le_bytes());
    iovecs.push(IoSlice::new(&header[start..start + 4]));

    // --- 放入 Payload 数据 ---
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
pub fn deserialize_sg_io(mut data: Bytes) -> StorageResult<MessagePayload> {
    use bytes::Buf;

    // 检查是否包含最小的 Header 长度 (总长4 + 时间戳8 + msg_id长2 + metadata数量2 + payload长4 = 20)
    if data.len() < 20 {
        return Err(StorageError::SerializeError(
            "data too short for header".to_string(),
        ));
    }

    // 1. 读取主 Header
    let total_len = data.get_u32_le() as usize;

    // 校验总长度是否匹配（可选，取决于上层读取时是否已经按照 total_len 切割好了）
    // 如果上层传进来的 data 刚好是一条完整消息，这里可以不校验或者做个断言
    // if data.len() + 4 != total_len { ... }

    let timestamp = data.get_u64_le();
    let msg_id_len = data.get_u16_le() as usize;
    let metadata_count = data.get_u16_le() as usize;

    // 2. 提取 msg_id (零拷贝)
    if data.len() < msg_id_len {
        return Err(StorageError::SerializeError(
            "unexpected EOF at msg_id".to_string(),
        ));
    }
    let msg_id = data.split_to(msg_id_len);

    // 3. 提取 metadata
    let mut metadata = std::collections::HashMap::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        // 读取 key_len (2 bytes) 和 val_len (4 bytes)
        if data.len() < 6 {
            return Err(StorageError::SerializeError(
                "unexpected EOF at metadata header".to_string(),
            ));
        }
        let key_len = data.get_u16_le() as usize;
        let val_len = data.get_u32_le() as usize;

        // 提取 key (零拷贝)
        if data.len() < key_len {
            return Err(StorageError::SerializeError(
                "unexpected EOF at metadata key".to_string(),
            ));
        }
        let key = data.split_to(key_len);

        // 提取 value (零拷贝)
        if data.len() < val_len {
            return Err(StorageError::SerializeError(
                "unexpected EOF at metadata value".to_string(),
            ));
        }
        let val = data.split_to(val_len);

        metadata.insert(key, val);
    }

    // 4. 提取 payload_len (4 bytes)
    if data.len() < 4 {
        return Err(StorageError::SerializeError(
            "unexpected EOF at payload_len".to_string(),
        ));
    }
    let payload_len = data.get_u32_le() as usize;

    // 5. 提取 payload (零拷贝)
    if data.len() < payload_len {
        return Err(StorageError::SerializeError(
            "unexpected EOF at payload".to_string(),
        ));
    }
    let payload = data.split_to(payload_len);

    Ok(MessagePayload {
        msg_id,
        timestamp,
        metadata,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sg_io_roundtrip() -> StorageResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert(Bytes::from("key1"), Bytes::from("value1"));
        metadata.insert(Bytes::from("key2"), Bytes::from("value2"));

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
            HashMap::new(),
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
            HashMap::new(),
            Bytes::from(vec![]),
        );

        let mut headers = BytesMut::new();
        let result = serialize_sg_io(&msg, &mut headers);
        assert!(result.is_err());
    }
}
