use super::{PersistStorageOperation, PersistTopicOperation};
use crate::{
    cache::{CacheWrapper, CACHE_TYPE_MEM},
    message::Message,
};
use anyhow::Result;
use common::global::Guard;
use dashmap::DashMap;
use std::sync::Arc;

pub struct Dummy {
    buf: usize,
    topics: DashMap<String, TopicDummy>,
}

impl Dummy {
    pub fn new(buf: usize) -> Self {
        Dummy {
            buf,
            topics: DashMap::with_capacity(buf),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.topics.contains_key(key)
    }

    pub fn insert(&self, key: &str) {
        self.topics
            .insert(key.to_string(), Guard::new(TopicDummyBase::new(key, 1000)));
    }

    fn get_or_create_topic_dummy(&self, topic_name: &str) -> Result<TopicDummy> {
        if let Some(topic_dummy) = self.topics.get(topic_name) {
            return Ok(topic_dummy.value().clone());
        }

        let topic_dummy = Guard::new(TopicDummyBase::new(topic_name, 100));
        self.topics
            .insert(topic_name.to_owned(), topic_dummy.clone());

        Ok(topic_dummy)
    }
}

#[async_trait::async_trait]
impl PersistStorageOperation for Dummy {
    async fn init(&self, _: bool) -> Result<()> {
        Ok(())
    }

    async fn push(&self, msg: Message) -> Result<()> {
        let topic = self.get_or_create_topic_dummy(msg.get_topic())?;
        topic.get().push(msg).await?;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// get a topic.
    async fn get(&self, topic_name: &str) -> Result<Option<Arc<Box<dyn PersistTopicOperation>>>> {
        if let Some(t) = self.topics.get(topic_name) {
            return Ok(Some(Arc::new(Box::new(t.value().clone()))));
        }

        Ok(None)
    }

    async fn get_or_create_topic(
        &self,
        topic_name: &str,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>> {
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
            instant: CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2),
            defer: CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2),
        }
    }

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            self.defer.push(msg).await?;
        } else {
            self.instant.push(msg).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PersistTopicOperation for TopicDummy {
    fn name(&self) -> &str {
        self.get().name.as_str()
    }

    async fn seek_defer(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().defer.seek(block).await;
        Ok(msg)
    }

    async fn next_defer(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().defer.pop(block).await;
        Ok(msg)
    }

    async fn seek_instant(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().instant.seek(block).await;
        Ok(msg)
    }

    async fn next_instant(&self, block: bool) -> Result<Option<Message>> {
        Ok(self.get().instant.pop(block).await)
    }
}
