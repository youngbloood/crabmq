mod nope;
use anyhow::Result;
use bytes::Bytes;
use nope::Nope;
use std::ops::Deref;
use async_trait::async_trait;

pub const COMPRESS_TYPE_NONE: &str = "no";

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
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

/// [`Compress`] is compress the message and uncompress message.
///
/// In order to use less space in disk.
#[async_trait]
pub trait Compress: Send + Sync {
    /// compress the message and return the compressed bytes.
    async fn compress(&self, _: Bytes) -> Result<Bytes>;

    /// decompress the message from the bytes.
    async fn decompress(&self, _: &[u8]) -> Result<Bytes>;
}

pub struct CompressWrapper {
    inner: Box<dyn Compress>,
}

impl CompressWrapper {
    pub fn new(cache: Box<dyn Compress>) -> Self {
        CompressWrapper { inner: cache }
    }

    pub fn with_type(t: &str) -> Self {
        match t {
            COMPRESS_TYPE_NONE => CompressWrapper {
                inner: Box::new(Nope) as Box<dyn Compress>,
            },
            _ => CompressWrapper {
                inner: Box::new(Nope) as Box<dyn Compress>,
            },
        }
    }
}

impl Deref for CompressWrapper {
    type Target = Box<dyn Compress>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// 压缩消息格式工具
pub struct CompressedMessage;

impl CompressedMessage {
    /// 压缩消息并返回新的格式：8字节压缩后长度 + 1字节压缩类型 + 压缩后的消息体
    pub async fn compress_message(
        message: &[u8], 
        compressor: &CompressWrapper
    ) -> Result<Vec<u8>> {
        let compressed = compressor.compress(Bytes::from(message.to_vec())).await?;
        
        // 如果压缩后更大，返回原始消息
        if compressed.len() >= message.len() {
            let mut result = Vec::with_capacity(9 + message.len());
            result.extend_from_slice(&(message.len() as u64).to_be_bytes());
            result.push(CompressionType::None as u8);
            result.extend_from_slice(message);
            return Ok(result);
        }
        
        // 压缩后的格式：8字节长度 + 1字节压缩类型 + 压缩数据
        let mut result = Vec::with_capacity(9 + compressed.len());
        result.extend_from_slice(&(compressed.len() as u64).to_be_bytes());
        result.push(CompressionType::Lz4 as u8); // 这里需要根据实际压缩器类型设置
        result.extend_from_slice(&compressed);
        
        Ok(result)
    }
    
    /// 解压消息
    pub async fn decompress_message(
        data: &[u8], 
        compressor: &CompressWrapper
    ) -> Result<Vec<u8>> {
        if data.len() < 9 {
            return Err(anyhow::anyhow!("Invalid compressed message format: too short"));
        }
        
        let compressed_len = u64::from_be_bytes(data[0..8].try_into()?) as usize;
        let compression_type = CompressionType::from(data[8]);
        let compressed_data = &data[9..9+compressed_len];
        
        match compression_type {
            CompressionType::None => {
                // 无压缩，直接返回原始数据
                Ok(compressed_data.to_vec())
            }
            _ => {
                // 有压缩，需要解压
                let decompressed = compressor.decompress(compressed_data).await?;
                Ok(decompressed.to_vec())
            }
        }
    }
    
    /// 从压缩消息中提取长度信息
    pub fn get_compressed_length(data: &[u8]) -> Result<u64> {
        if data.len() < 8 {
            return Err(anyhow::anyhow!("Invalid message format: too short"));
        }
        Ok(u64::from_be_bytes(data[0..8].try_into()?))
    }
    
    /// 获取压缩消息的总长度（包括头部）
    pub fn get_total_length(data: &[u8]) -> Result<usize> {
        let compressed_len = Self::get_compressed_length(data)? as usize;
        Ok(8 + 1 + compressed_len) // 8字节长度 + 1字节压缩类型 + 压缩数据
    }
}

// 压缩器实现可以后续根据需要添加
// 目前使用现有的 Compress trait 和 CompressWrapper
