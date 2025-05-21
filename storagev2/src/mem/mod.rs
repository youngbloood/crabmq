use crate::Storage;
use anyhow::{Ok, Result};
use bytes::Bytes;

#[derive(Clone)]
pub struct MemStorage {}

impl MemStorage {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Storage for MemStorage {
    // 存储消息
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
        Ok(())
    }
    // 获取下一个消息
    async fn next(&self, topic: &str, partition: u32) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    // 确认某个消息已消费
    async fn commit(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
        Ok(())
    }
}
