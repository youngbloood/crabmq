/// 序列化器抽象
///
/// 设计目标：
/// 1. 支持多种序列化实现（rkyv, S-G IO, 压缩等）
/// 2. 零拷贝写入（通过IoSlice）
/// 3. 便于性能测试和切换
use crate::{MessagePayload, StorageResult};
use bytes::Bytes;
use std::io::IoSlice;

/// 序列化结果
///
/// 包含两种形式：
/// - IoSlice数组：用于零拷贝写入（write_vectored）
/// - 总长度：用于更新写指针
pub struct SerializedMessage<'a> {
    /// IoSlice数组，指向原始数据，零拷贝
    pub iovecs: Vec<IoSlice<'a>>,
    /// 序列化后的总长度（包括所有头）
    pub total_len: u64,
}

/// 序列化器trait
///
/// 不同的实现可以采用不同的格式：
/// - RkyvSerializer: [8:len] + [rkyv_data]
/// - SgIoSerializer: [1:msg_id_len] + [msg_id] + [8:timestamp] + ...
/// - CompressedSerializer: [8:len] + [1:compress_type] + [compressed_data]
pub trait MessageSerializer: Send + Sync {
    /// 序列化消息（零拷贝）
    ///
    /// 返回IoSlice数组，可以直接用于write_vectored
    ///
    /// # 零拷贝保证
    /// - 不拷贝msg_id、metadata、payload的原始数据
    /// - 只创建必要的长度头字节数组
    /// - IoSlice只持有数据的引用
    fn serialize<'a>(
        &self,
        msg: &'a MessagePayload,
        headers: &'a mut Vec<Vec<u8>>,
    ) -> StorageResult<SerializedMessage<'a>>;

    /// 反序列化消息
    ///
    /// 从字节流构造MessagePayload
    fn deserialize(&self, data: &[u8]) -> StorageResult<MessagePayload>;

    /// 获取序列化格式名称（用于日志和调试）
    fn name(&self) -> &'static str;
}

/// S-G IO序列化器
///
/// 格式：[8:len] + [1:msg_id_len] + [msg_id] + [8:timestamp] + [4:metadata_len] + [metadata] + [4:payload_len] + [payload]
///
/// 特点：
/// 1. 零拷贝写入
/// 2. 空间效率高
pub struct SgIoSerializer;

/// rkyv序列化器（保留用于性能对比）
///
/// 格式：[8:len] + [rkyv_data]
///
/// 特点：
/// 1. 使用rkyv库自动序列化
/// 2. 需要拷贝数据到AlignedVec
/// 3. 反序列化快
pub struct RkyvSerializer;

impl MessageSerializer for SgIoSerializer {
    fn serialize<'a>(
        &self,
        msg: &'a MessagePayload,
        headers: &'a mut Vec<Vec<u8>>,
    ) -> StorageResult<SerializedMessage<'a>> {
        // 将在sg_io模块中实现
        crate::serializer::sg_io::serialize_sg_io(msg, headers)
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<MessagePayload> {
        crate::serializer::sg_io::deserialize_sg_io(data)
    }

    fn name(&self) -> &'static str {
        "sg-io"
    }
}

impl MessageSerializer for RkyvSerializer {
    fn serialize<'a>(
        &self,
        msg: &'a MessagePayload,
        headers: &'a mut Vec<Vec<u8>>,
    ) -> StorageResult<SerializedMessage<'a>> {
        crate::serializer::rkyv_impl::serialize_rkyv(msg, headers)
    }

    fn deserialize(&self, data: &[u8]) -> StorageResult<MessagePayload> {
        crate::serializer::rkyv_impl::deserialize_rkyv(data)
    }

    fn name(&self) -> &'static str {
        "rkyv"
    }
}

// 子模块
pub mod rkyv_impl;
pub mod sg_io;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_serializer_roundtrip() -> StorageResult<()> {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());

        let msg = MessagePayload::new(
            "test_msg_id".to_string(),
            1234567890,
            metadata,
            vec![0xAB; 100],
        );

        // 测试 S-G IO
        let sg_io = SgIoSerializer;
        let mut headers = Vec::new();
        let serialized = sg_io.serialize(&msg, &mut headers)?;

        // 模拟写入后的读取：需要将IoSlice的数据收集起来
        let mut buf = Vec::new();
        for iov in &serialized.iovecs {
            buf.extend_from_slice(iov);
        }

        let deserialized = sg_io.deserialize(&buf)?;
        assert_eq!(msg.msg_id, deserialized.msg_id);
        assert_eq!(msg.timestamp, deserialized.timestamp);
        assert_eq!(msg.metadata, deserialized.metadata);
        assert_eq!(msg.payload, deserialized.payload);

        Ok(())
    }
}
