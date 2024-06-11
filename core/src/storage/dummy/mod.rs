use std::{collections::HashMap, sync::Arc};

use super::{storage::StorageOperation, TopicOperation};
use crate::{
    cache::{cache::CacheWrapper, CACHE_TYPE_MEM},
    message::Message,
};
use anyhow::Result;
use common::global::Guard;

pub struct Dummy {
    buf: usize,
    topics: HashMap<String, TopicDummy>,
}

impl Dummy {
    pub fn new(buf: usize) -> Self {
        Dummy {
            buf,
            topics: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl StorageOperation for Dummy {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn push(&self, msg: Message) -> Result<()> {
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn get_or_create_topic(&self, topic_name: &str) -> Result<Arc<Box<dyn TopicOperation>>> {
        Ok(Arc::new(Box::new(Guard::new(TopicDummyBase::new(
            topic_name, 100,
        )))))
    }
}

type TopicDummy = Guard<TopicDummyBase>;

pub struct TopicDummyBase {
    name: String,
    instant: CacheWrapper,
    defer: CacheWrapper,
}

impl TopicDummyBase {
    pub fn new(name: &str, buf: usize) -> Self {
        TopicDummyBase {
            name: name.to_string(),
            instant: CacheWrapper::new(CACHE_TYPE_MEM, buf),
            defer: CacheWrapper::new(CACHE_TYPE_MEM, buf),
        }
    }
}

#[async_trait::async_trait]
impl TopicOperation for TopicDummy {
    fn name(&self) -> &str {
        self.get().name.as_str()
    }

    async fn next_defer(&self, _: bool) -> Result<Option<Message>> {
        Ok(self.get().defer.pop().await)
    }

    async fn next_instant(&self, _: bool) -> Result<Option<Message>> {
        Ok(self.get().instant.pop().await)
    }
}
