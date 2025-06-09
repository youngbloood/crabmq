pub mod disk;
pub mod mem;
pub mod metrics;

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
    /// Get the next message
    ///
    /// It should block when there are no new message
    async fn next(
        &self,
        topic: &str,
        partition: u32,
        stop_signal: CancellationToken,
    ) -> Result<Bytes>;

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(&self, topic: &str, partition: u32) -> Result<()>;
}
