use super::{memory::Memory, CacheOperation, CACHE_TYPE_MEM};
use crate::message::Message;
use anyhow::Result;
use dynamic_queue::DefaultQueue;
use std::ops::Deref;

pub enum CacheEnum {
    Memory(Memory),
}

impl CacheOperation for CacheEnum {
    async fn push(&self, msg: Message) -> Result<()> {
        match self {
            CacheEnum::Memory(mem) => mem.push(msg).await,
        }
    }

    async fn pop(&self) -> Option<Message> {
        match self {
            CacheEnum::Memory(mem) => mem.pop().await,
        }
    }

    async fn resize(&self, size: usize) -> Result<()> {
        match self {
            CacheEnum::Memory(mem) => mem.resize(size),
        }
    }
}

pub struct CacheWrapper {
    inner: CacheEnum,
}

impl CacheWrapper {
    pub fn new(cache_type: &str, size: usize) -> Self {
        match cache_type {
            CACHE_TYPE_MEM => {
                let cache = CacheEnum::Memory(Memory::new(size, DefaultQueue::new(10)));
                CacheWrapper { inner: cache }
            }

            _ => {
                let cache = CacheEnum::Memory(Memory::new(size, DefaultQueue::new(10)));
                CacheWrapper { inner: cache }
            }
        }
    }

    pub async fn push(&self, msg: Message) -> Result<()> {
        self.inner.push(msg).await
    }

    pub async fn pop(&self) -> Option<Message> {
        self.inner.pop().await
    }

    pub async fn resize(&self, size: usize) -> Result<()> {
        self.inner.resize(size).await
    }
}

impl Deref for CacheWrapper {
    type Target = CacheEnum;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
