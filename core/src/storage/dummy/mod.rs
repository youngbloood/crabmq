use super::{PersistStorageOperation, PersistTopicOperation, TopicMeta};
use crate::cache::{CacheWrapper, CACHE_TYPE_MEM};
use anyhow::{Error, Result};
use common::global::Guard;
use dashmap::DashMap;
use protocol::{
    error::{
        ProtError, ERR_TOPIC_PROHIBIT_DEFER, ERR_TOPIC_PROHIBIT_INSTANT, ERR_TOPIC_PROHIBIT_TYPE,
    },
    message::Message,
    ProtocolHead,
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

    pub fn insert(&self, key: &str, head: &ProtocolHead) -> Result<TopicDummy> {
        match head {
            ProtocolHead::V1(v1) => {
                let meta = TopicMeta::new(
                    v1.prohibit_instant(),
                    v1.prohibit_defer(),
                    v1.get_defer_msg_format().to_string(),
                    v1.get_max_msg_num_per_file(),
                    v1.get_max_size_per_file(),
                    v1.get_compress_type(),
                    v1.get_subscribe_type(),
                    v1.get_record_num_per_file(),
                    v1.get_record_size_per_file(),
                    v1.get_fd_cache_size(),
                );

                let topic_dummy = Guard::new(TopicDummyBase::new(key, meta));
                self.topics.insert(key.to_owned(), topic_dummy.clone());
                Ok(topic_dummy)
            }
        }
    }

    fn get_or_create_topic_dummy(
        &self,
        topic_name: &str,
        head: &ProtocolHead,
    ) -> Result<TopicDummy> {
        if let Some(topic_dummy) = self.topics.get(topic_name) {
            return Ok(topic_dummy.value().clone());
        }

        // create
        if head.prohibit_defer() && head.prohibit_instant() {
            return Err(Error::from(ProtError::new(ERR_TOPIC_PROHIBIT_TYPE)));
        }

        self.insert(topic_name, head)
    }
}

#[async_trait::async_trait]
impl PersistStorageOperation for Dummy {
    async fn init(&self, _: bool) -> Result<()> {
        Ok(())
    }

    async fn push(&self, msg: Message) -> Result<()> {
        let topic = self.get_or_create_topic_dummy(msg.get_topic(), &msg.get_head())?;
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
        head: &ProtocolHead,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>> {
        let topic = self.get_or_create_topic_dummy(topic_name, head)?;
        Ok(Arc::new(Box::new(topic)))
    }
}

type TopicDummy = Guard<TopicDummyBase>;

pub struct TopicDummyBase {
    name: String,
    defer: CacheWrapper,
    instant: CacheWrapper,
    meta: TopicMeta,
}

impl TopicDummyBase {
    pub fn new(name: &str, meta: TopicMeta) -> Self {
        let buf = 100;
        let defer = if meta.prohibit_defer {
            CacheWrapper::new(CACHE_TYPE_MEM, 0, 0)
        } else {
            CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2)
        };

        let instant = if meta.prohibit_defer {
            CacheWrapper::new(CACHE_TYPE_MEM, 0, 0)
        } else {
            CacheWrapper::new(CACHE_TYPE_MEM, buf, buf / 2)
        };

        TopicDummyBase {
            name: name.to_string(),
            defer,
            instant,
            meta,
        }
    }

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            if self.meta.prohibit_defer {
                return Err(Error::from(ProtError::new(ERR_TOPIC_PROHIBIT_DEFER)));
            }
            self.defer.push(msg).await?;
            return Ok(());
        }

        if self.meta.prohibit_instant {
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
        self.get().meta.prohibit_defer
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
        self.get().meta.prohibit_instant
    }

    async fn seek_instant(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().instant.seek(block).await;
        Ok(msg)
    }

    async fn next_instant(&self, block: bool) -> Result<Option<Message>> {
        Ok(self.get().instant.pop(block).await)
    }

    fn get_meta(&self) -> Result<TopicMeta> {
        Ok(self.get().meta.clone())
    }
}
