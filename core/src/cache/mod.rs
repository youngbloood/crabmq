pub mod cache;
pub mod memory;

pub const CACHE_TYPE_MEM: &str = "momery";

use crate::message::Message;
use anyhow::Result;

/// [`Cache`] is cache of store temporary Message.
///
/// In order to get Message quickly.
pub trait CacheOperation {
    /// push a message into cache. it will be block if the cache is filled.
    async fn push(&self, _: Message) -> Result<()>;

    /// pop a message from cache. if it is defer message, cache should control it pop when it's expired. or pop the None
    async fn pop(&self) -> Option<Message>;

    /// resize the buffer length in cache.
    async fn resize(&self, size: usize) -> Result<()>;
}
