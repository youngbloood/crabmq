pub mod mem;
pub use mem::*;
pub mod disk;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Storage: Send + Sync + Clone + 'static {
    /// Store the message to the Storage Media
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()>;

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
