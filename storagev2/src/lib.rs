pub mod mem;
pub use mem::*;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

#[async_trait]
pub trait Storage: Send + Sync + Clone + 'static {
    // 存储消息
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()>;
    // 获取下一个消息
    async fn next(&self, topic: &str, partition: u32) -> Result<Bytes>;
    // 确认某个消息已消费
    async fn commit(&self, topic: &str, partition: u32) -> Result<()>;
}
