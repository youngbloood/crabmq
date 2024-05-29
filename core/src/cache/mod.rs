pub mod cache;
pub mod memory;

pub const CACHE_TYPE_MEM: &str = "momery";

use crate::message::Message;
use anyhow::Result;

/// [`Cache`] is cache of store temporary Message.
///
/// In order to get Message quickly.
pub trait CacheOperation {
    /// push a message into cache.
    async fn push(&mut self, _: Message) -> Result<()>;

    /// pop a message from cache.
    async fn pop(&mut self) -> Option<Message>;
}
