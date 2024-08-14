pub mod queue;
pub mod slide_window;

pub const CACHE_TYPE_MEM: &str = "momery";
pub const CACHE_TYPE_MEM_SLIDE_WINDOW: &str = "momery_slide_window";

use anyhow::Result;
use enum_dispatch::enum_dispatch;
use protocol::message::Message;
use slide_window::MessageCacheSlidingWindows;
use std::ops::Deref;

/// [`Cache`] is cache of store temporary Message.
///
/// In order to get Message quickly.
#[enum_dispatch]
pub trait CacheOperation {
    /// push a message into cache. it will be block if the cache is filled.
    async fn try_push(&self, _: Message) -> Result<()>;

    /// push a message into cache. it will be block if the cache is filled.
    async fn push(&self, _: Message) -> Result<()>;

    /// seek a message from cache. if it is defer message, cache should control it pop when it's expired. or pop the None
    async fn seek(&self, _: bool) -> Option<Message>;

    /// pop a message from cache. if it is defer message, cache should control it pop when it's expired. or pop the None
    async fn pop(&self, _: bool) -> Option<Message>;

    /// resize the buffer length in cache.
    async fn resize(&self, cap: usize, slide_win: usize);

    /// consume a message by id.
    async fn update_consume(&self, id: &str, consumed: bool) -> Result<()>;

    /// consume a message by id.
    async fn update_notready(&self, id: &str, notready: bool) -> Result<()>;

    /// consume a message by id.
    async fn update_delete(&self, id: &str, deleted: bool) -> Result<()>;
}

#[enum_dispatch(CacheOperation)]
pub enum CacheEnum {
    // Queue(MessageCacheQueue),
    SlideWindow(MessageCacheSlidingWindows),
}

pub struct CacheWrapper {
    inner: CacheEnum,
}

impl CacheWrapper {
    pub fn new(cache_type: &str, cap: usize, slide_win: usize) -> Self {
        match cache_type {
            CACHE_TYPE_MEM_SLIDE_WINDOW => {
                let cache = CacheEnum::SlideWindow(MessageCacheSlidingWindows::new(cap, slide_win));
                CacheWrapper { inner: cache }
            }

            _ => {
                let cache = CacheEnum::SlideWindow(MessageCacheSlidingWindows::new(cap, slide_win));
                CacheWrapper { inner: cache }
            }
        }
    }

    pub async fn try_push(&self, msg: Message) -> Result<()> {
        self.inner.try_push(msg).await
    }

    pub async fn push(&self, msg: Message) -> Result<()> {
        self.inner.push(msg).await
    }

    pub async fn seek(&self, block: bool) -> Option<Message> {
        self.inner.seek(block).await
    }

    pub async fn pop(&self, block: bool) -> Option<Message> {
        self.inner.pop(block).await
    }

    pub async fn resize(&self, cap: usize, slide_win: usize) {
        self.inner.resize(cap, slide_win).await
    }

    pub async fn update_consume(&self, id: &str, consume: bool) -> Result<()> {
        self.inner.update_consume(id, consume).await
    }

    pub async fn update_delete(&self, id: &str, delete: bool) -> Result<()> {
        self.inner.update_delete(id, delete).await
    }

    pub async fn update_notready(&self, id: &str, notready: bool) -> Result<()> {
        self.inner.update_notready(id, notready).await
    }
}

impl Deref for CacheWrapper {
    type Target = CacheEnum;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
