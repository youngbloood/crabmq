pub mod mem;
pub use mem::*;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Storage: Send + Sync + Clone + 'static {
    // 存储消息
    async fn store(topic: &str, partition: u32, data: Bytes) -> Result<()>;
    // 获取下一个消息
    async fn next(topic: &str, partition: u32) -> Result<Bytes>;
    // 确认某个消息已消费
    async fn commit(topic: &str, partition: u32, data: Bytes) -> Result<()>;
}
