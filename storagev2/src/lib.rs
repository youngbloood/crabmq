pub mod disk;
pub mod mem;
pub mod metrics;

use std::{num::NonZero, path::PathBuf};

pub use mem::*;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait StorageWriter: Send + Sync + Clone + 'static {
    /// Store the message to the Storage Media
    async fn store(&self, topic: &str, partition: u32, datas: &[Bytes]) -> Result<()>;
}

#[async_trait]
pub trait StorageReader: Send + Sync + Clone + 'static {
    /// New a session with group_id, it will be return Err() when session has been created.
    async fn new_session(&self, group_id: u32) -> Result<Box<dyn StorageReaderSession>>;

    /// Close a session by group_id.
    async fn close_session(&self, group_id: u32);
}

#[async_trait]
pub trait StorageReaderSession: Send + Sync + 'static {
    /// Get the next n message
    async fn next(
        &self,
        topic: &str,
        partition: u32,
        n: NonZero<u64>,
    ) -> Result<Vec<(Bytes, SegmentOffset)>>;

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32, offset: SegmentOffset) -> Result<()>;
}

#[derive(Default, Debug)]
pub struct SegmentOffset {
    pub filename: PathBuf,
    pub offset: u64,
}

pub enum ReadType {
    FromBegin, // 从头开始消费
    Latest,    // 从最新消息开始消费，以第一次调用next为快照
}
