mod config;
mod defer;
mod instant;
mod message_manager;
mod record;

use super::{PersistStorageOperation, PersistTopicOperation};
use crate::message::Message;
use anyhow::{anyhow, Result};
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_dir, check_exist, interval},
};
use dashmap::DashMap;
use defer::Defer;
use instant::Instant;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';
const HEAD_SIZE_PER_FILE: u64 = 8;

pub fn gen_filename(factor: u64) -> String {
    format!("{:0>20}", factor)
}

pub struct StorageDisk {
    dir: PathBuf,

    /// topic_name: TopicMessage
    topics: DashMap<String, TopicMessage>,

    max_msg_num_per_file: u64,
    max_size_per_file: u64,

    /// 取消信号
    cancel: CancellationToken,
}

impl StorageDisk {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        StorageDisk {
            dir,
            topics: DashMap::new(),
            cancel: CancellationToken::new(),
            max_msg_num_per_file,
            max_size_per_file,
        }
    }

    async fn get_topic_from_dir(&self, dir_name: PathBuf) -> Result<()> {
        let topic_name = dir_name.file_name().unwrap().to_str().unwrap();
        info!("LAZY: load topic[{topic_name}]...");
        let defer = Defer::new(dir_name.clone(), "{daily}/{hourly}/{minutely:5}")?;
        defer.load().await?;

        let instant = Instant::new(dir_name.clone())?;
        instant.load().await?;

        self.topics.insert(
            topic_name.to_string(),
            TopicMessage::new(TopicMessageBase::new(topic_name, instant, defer)),
        );
        Ok(())
    }

    async fn get_topic(&self, topic_name: &str) -> Result<Option<TopicMessage>> {
        if !self.topics.contains_key(topic_name) {
            let parent = self.dir.join(topic_name);
            if !check_exist(&parent) {
                return Ok(None);
            }

            info!("LAZY: load topic[{topic_name}]...");
            self.get_topic_from_dir(parent).await?;
        }
        let topic = self.topics.get(topic_name).unwrap();
        Ok(Some(topic.value().clone()))
    }

    async fn get_or_create_topic_inner(&self, topic_name: &str) -> Result<TopicMessage> {
        if !self.topics.contains_key(topic_name) {
            let parent = Path::new(self.dir.to_str().unwrap()).join(topic_name);
            debug!("StorageDisk: load topic: {}", topic_name);

            // init the topic-message
            let defer = Defer::new(parent.join("defer"), "{daily}/{hourly}/{minutely:5}")?;
            let instant = Instant::new(parent)?;

            let mut topic_mb = TopicMessageBase::new(topic_name, instant, defer);
            topic_mb.load().await?;

            let topic = TopicMessage::new(topic_mb);
            self.topics.insert(topic_name.to_string(), topic.clone());
            return Ok(topic);
        }
        let topic = self.topics.get(topic_name).unwrap();
        Ok(topic.value().clone())
    }
}

#[async_trait::async_trait]
impl PersistStorageOperation for StorageDisk {
    async fn init(&self, validate: bool) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        if !validate {
            return Ok(());
        }
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                continue;
            }
            self.get_topic_from_dir(entry.path()).await?;
        }
        Ok(())
    }

    async fn push(&self, msg: Message) -> Result<()> {
        let topic = self.get_or_create_topic_inner(msg.get_topic()).await?;
        topic.push(msg).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let iter = self.topics.iter();
        for topic in iter {
            topic
                .guard
                .get()
                .instant
                .flush()
                .await
                .expect("flush instant failed");
            topic
                .guard
                .get()
                .defer
                .flush()
                .await
                .expect("flush defer failed");
            // handles.push(topic.clone().get().instant.flush());
            // handles.push(topic.clone().get().defer.flush());
        }
        // join!(handles)?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }

    /// get a topic.
    async fn get(&self, topic_name: &str) -> Result<Option<Arc<Box<dyn PersistTopicOperation>>>> {
        if let Some(topic) = self.get_topic(topic_name).await? {
            return Ok(Some(Arc::new(
                Box::new(topic) as Box<dyn PersistTopicOperation>
            )));
        }

        Ok(None)
    }

    async fn get_or_create_topic(
        &self,
        topic_name: &str,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>> {
        let topic = self.get_or_create_topic_inner(topic_name).await?;
        Ok(Arc::new(Box::new(topic) as Box<dyn PersistTopicOperation>))
    }
}

// type TopicMessage = Guard<TopicMessageBase>;
struct TopicMessage {
    guard: Guard<TopicMessageBase>,
}

impl TopicMessage {
    fn new(topic_msg_base: TopicMessageBase) -> Self {
        TopicMessage {
            guard: Guard::new(topic_msg_base),
        }
    }

    fn clone(&self) -> Self {
        TopicMessage {
            guard: self.guard.clone(),
        }
    }

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            self.guard.get().defer.handle_msg(msg).await?;
            return Ok(());
        }
        self.guard.get().instant.handle_msg(msg).await?;
        Ok(())
    }
}

struct TopicMessageBase {
    name: String,
    instant: Instant,
    defer: Defer,
}

impl TopicMessageBase {
    fn new(name: &str, instant: Instant, defer: Defer) -> Self {
        TopicMessageBase {
            name: name.to_string(),
            instant,
            defer,
        }
    }

    async fn load(&mut self) -> Result<()> {
        self.instant.load().await?;
        self.defer.load().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl PersistTopicOperation for TopicMessage {
    fn name(&self) -> &str {
        self.guard.get().name.as_str()
    }

    async fn seek_defer(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_millis(300)).await;

        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return Err(anyhow!("process stopped"));
                }

                msg = self.guard.get().defer.seek() => {
                    match msg {
                        Ok(msg) => {
                            match msg {
                                Some(msg) => {
                                    return Ok(Some(msg));
                                }
                                None => {
                                    if !block {
                                        return Ok(None);
                                    }
                                    ticker.tick().await;
                                    continue;
                                }
                            }
                        },

                        Err(e) => return Err(anyhow!(e)),
                    }
                }
            }
        }
    }

    async fn next_defer(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() =>{
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.guard.get().defer.pop().await {
                        Ok(msg) => {
                            match msg {
                                Some(msg) => {
                                    if !(msg.is_deleted() || msg.is_consumed() || msg.is_not_ready()) {
                                        return Ok(Some(msg));
                                    }
                                    Ok(None)
                                }
                                None => {
                                    if !block{
                                        return Ok(None)
                                    }
                                    select! {
                                        _ = CANCEL_TOKEN.cancelled() =>{
                                            Err(anyhow!("process stopped"))
                                        }
                                        _ = ticker.tick() => {
                                            Ok(None)
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            Err(anyhow!(e))
                        }
                    }
                } => {
                    match msg {
                        Ok(msg) => {
                            match msg {
                                Some(m) => {
                                    return Ok(Some(m));
                                }
                                None => {
                                    if !block{
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                }
            }
        }
    }

    async fn seek_instant(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_millis(300)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return Err(anyhow!("process stopped"))
                }

                msg = self.guard.get().instant.seek() => {
                    match msg {
                        Ok(msg) => {
                            match msg {
                                Some(msg) => {
                                    return Ok(Some(msg));
                                }
                                None => {
                                    if !block {
                                        return Ok(None)
                                    }
                                    ticker.tick().await;
                                    continue;
                                }
                            }
                        }

                        Err(e) => return Err(anyhow!(e)),
                    }
                }
            }
        }
    }

    async fn next_instant(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.guard.get().instant.pop().await {
                        Ok(msg) => {
                            match msg {
                                Some(msg) => {
                                    if !(msg.is_deleted() || msg.is_consumed() || msg.is_not_ready()) {
                                        return Ok(Some(msg));
                                    }
                                    Ok(None)
                                }
                                None => {
                                    if !block {
                                        return Ok(None)
                                    }
                                    select! {
                                        _ = CANCEL_TOKEN.cancelled() =>{
                                            Err(anyhow!("process stopped"))
                                        }
                                        _ = ticker.tick() => {
                                            Ok(None)
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            Err(anyhow!(e))
                        }
                    }
                } => {
                    match msg {
                        Ok(msg) => {
                            match msg {
                                Some(m) => {
                                    return Ok(Some(m));
                                }
                                None => {
                                    if !block{
                                        return Ok(None);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                }
            }
        }
    }

    // async fn push(&self, msg: Message) -> Result<()> {
    //     if msg.is_defer() {
    //         self.get().defer.push(msg).await?;
    //         return Ok(());
    //     }
    //     self.get().instant.push(msg).await?;
    //     Ok(())
    // }
}
