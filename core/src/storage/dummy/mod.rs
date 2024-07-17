use super::{PersistStorageOperation, PersistTopicOperation};
use crate::cache::{CacheWrapper, CACHE_TYPE_MEM};
use anyhow::{anyhow, Error, Result};
use common::global::Guard;
use dashmap::DashMap;
use protocol::{
    error::{
        ProtError, ERR_TOPIC_PROHIBIT_DEFER, ERR_TOPIC_PROHIBIT_INSTANT, ERR_TOPIC_PROHIBIT_TYPE,
    },
    message::Message,
};
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

    pub fn insert(&self, key: &str, prohibit_instant: bool, prohibit_defer: bool) {
        self.topics.insert(
            key.to_string(),
            Guard::new(TopicDummyBase::new(
                key,
                prohibit_defer,
                prohibit_instant,
                1000,
            )),
        );
    }

    fn get_or_create_topic_dummy(
        &self,
        topic_name: &str,
        prohibit_instant: bool,
        prohibit_defer: bool,
    ) -> Result<TopicDummy> {
        if let Some(topic_dummy) = self.topics.get(topic_name) {
            return Ok(topic_dummy.value().clone());
        }

        // create
        if prohibit_defer && prohibit_instant {
            return Err(Error::from(ProtError::new(ERR_TOPIC_PROHIBIT_TYPE)));
        }

        let topic_dummy = Guard::new(TopicDummyBase::new(
            topic_name,
            prohibit_defer,
            prohibit_instant,
            100,
        ));
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
        let topic = self.get_or_create_topic_dummy(
            msg.get_topic(),
            msg.prohibit_instant(),
            msg.prohibit_defer(),
        )?;
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
        prohibit_instant: bool,
        prohibit_defer: bool,
        _defer_message_format: &str,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>> {
        let topic = self.get_or_create_topic_dummy(topic_name, prohibit_instant, prohibit_defer)?;
        Ok(Arc::new(Box::new(topic)))
    }
}

type TopicDummy = Guard<TopicDummyBase>;

pub struct TopicDummyBase {
    name: String,
    prohibit_defer: bool,
    defer: CacheWrapper,
    prohibit_instant: bool,
    instant: CacheWrapper,
}

impl TopicDummyBase {
    pub fn new(name: &str, prohibit_defer: bool, prohibit_instant: bool, buf: usize) -> Self {
        let defer = if prohibit_defer {
            CacheWrapper::new(CACHE_TYPE_MEM, 0, 0)
        } else {
            CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2)
        };

        let instant = if prohibit_defer {
            CacheWrapper::new(CACHE_TYPE_MEM, 0, 0)
        } else {
            CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2)
        };

        TopicDummyBase {
            name: name.to_string(),
            prohibit_defer,
            defer,
            prohibit_instant,
            instant,
        }
    }

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            if self.prohibit_defer {
                return Err(Error::from(ProtError::new(ERR_TOPIC_PROHIBIT_DEFER)));
            }
            self.defer.push(msg).await?;
            return Ok(());
        }

        if self.prohibit_instant {
            return Err(Error::from(ProtError::new(ERR_TOPIC_PROHIBIT_INSTANT)));
        }
        self.instant.push(msg).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PersistTopicOperation for TopicDummy {
    fn name(&self) -> &str {
        self.get().name.as_str()
    }

    fn prohibit_defer(&self) -> bool {
        self.get().prohibit_defer
    }

    async fn seek_defer(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().defer.seek(block).await;
        Ok(msg)
    }

    async fn next_defer(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().defer.pop(block).await;
        Ok(msg)
    }

    fn prohibit_instant(&self) -> bool {
        self.get().prohibit_instant
    }

    async fn seek_instant(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().instant.seek(block).await;
        Ok(msg)
    }

    async fn next_instant(&self, block: bool) -> Result<Option<Message>> {
        Ok(self.get().instant.pop(block).await)
    }
}
