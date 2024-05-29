use super::{memory::Memory, CacheOperation};
use crate::message::Message;
use anyhow::Result;
use std::ops::Deref;

pub enum CacheEnum {
    Memory(Memory),
}

impl CacheOperation for CacheEnum {
    async fn push(&mut self, _: Message) -> Result<()> {
        todo!()
    }

    async fn pop(&mut self) -> Option<Message> {
        todo!()
    }
}

pub struct CacheWrapper {
    inner: CacheEnum,
}

impl CacheWrapper {
    pub fn new(cache_type: &str, size: usize) -> Self {
        match cache_type {
            CACHE_TYPE_MEM => {
                let cache = CacheEnum::Memory(Memory::new(size));
                CacheWrapper { inner: cache }
            }

            _ => {
                let cache = CacheEnum::Memory(Memory::new(size));
                CacheWrapper { inner: cache }
            }
        }
    }

    pub async fn push(&mut self, msg: Message) -> Result<()> {
        self.inner.push(msg).await
    }

    pub async fn pop(&mut self) -> Option<Message> {
        self.inner.pop().await
    }
}

impl Deref for CacheWrapper {
    type Target = CacheEnum;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
