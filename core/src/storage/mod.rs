/*!
 * Persist Messages into Storage and parse Messages from Storage
 */

mod dummy;
mod local;
pub mod storage;

use crate::message::Message;
use anyhow::Result;

pub const STORAGE_TYPE_DUMMY: &str = "dummy";
pub const STORAGE_TYPE_LOCAL: &str = "local";

// #[async_trait::async_trait]
pub trait StorageOperation {
    /// init the Storage Media.
    async fn init(&mut self) -> Result<()>;

    /// return the next Message from Storage Media.
    async fn next(&mut self) -> Option<Message>;

    /// push a message into Storage.
    async fn push(&mut self, _: Message) -> Result<()>;

    /// flush the messages to Storage Media.
    async fn flush(&mut self) -> Result<()>;

    /// stop the Storage Media.
    async fn stop(&mut self) -> Result<()>;
}
