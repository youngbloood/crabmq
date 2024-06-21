use anyhow::{anyhow, Result};
use semver::Op;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::select;

use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_dir, interval},
};
use defer::Defer;
use instant::Instant;
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::message::Message;

use super::{StorageOperation, TopicOperation};

mod defer;
mod instant;
mod message_manager;
mod record;

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';
const HEAD_SIZE_PER_FILE: u64 = 8;

pub fn gen_filename(factor: u64) -> String {
    format!("{:0>20}", factor)
}

pub struct StorageDisk {
    dir: PathBuf,

    /// topic_name: TopicMessage
    topics: RwLock<HashMap<String, TopicMessage>>,

    max_msg_num_per_file: u64,
    max_size_per_file: u64,

    /// 取消信号
    cancel: CancellationToken,
}

// unsafe impl Send for StorageDisk {}
// unsafe impl Sync for StorageDisk {}

impl StorageDisk {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        StorageDisk {
            dir,
            topics: RwLock::new(HashMap::new()),
            cancel: CancellationToken::new(),
            max_msg_num_per_file,
            max_size_per_file,
        }
    }
}

#[async_trait::async_trait]
impl StorageOperation for StorageDisk {
    async fn init(&self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;

        for entry in fs::read_dir(self.dir.to_str().unwrap())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let topic_name = entry.file_name();
            let parent = Path::new(self.dir.to_str().unwrap()).join(topic_name.to_str().unwrap());
            debug!("StorageLocal: load topic: {}", topic_name.to_str().unwrap());

            let defer = Defer::new(parent.join("defer"), "{daily}/{hourly}/{minutely:5}")?;
            defer.load().await?;

            let instant = Instant::new(parent);
            instant.load().await?;

            let mut wg = self.topics.write();
            wg.insert(
                topic_name.to_str().unwrap().to_string(),
                Guard::new(TopicMessageBase::new(
                    topic_name.to_str().unwrap(),
                    instant,
                    defer,
                )),
            );
        }

        Ok(())
    }

    async fn push(&self, msg: Message) -> Result<()> {
        // if msg.is_defer() {
        //     self.get().defer.push(msg).await?;
        //     return Ok(());
        // }
        // self.get().instant.push(msg).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let rg = self.topics.read();
        let iter = rg.iter();
        // let mut handles = Vec::with_capacity(rg.len() * 2);
        for (_, topic) in iter {
            topic.clone().get().instant.flush().await?;
            topic.clone().get().defer.flush().await?;
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

    async fn get_or_create_topic(&self, topic_name: &str) -> Result<Arc<Box<dyn TopicOperation>>> {
        if !self.topics.read().contains_key(topic_name) {
            let parent = Path::new(self.dir.to_str().unwrap()).join(topic_name);
            debug!("StorageLocal: load topic: {}", topic_name);

            // init the topic-message
            let defer = Defer::new(parent.join("defer"), "{daily}/{hourly}/{minutely:5}")?;
            let instant = Instant::new(parent);

            let mut topic_mm = TopicMessageBase::new(topic_name, instant, defer);
            topic_mm.load().await?;

            self.topics
                .write()
                .insert(topic_name.to_string(), Guard::new(topic_mm));
        }
        let rg = self.topics.read();
        let topic = rg.get(topic_name).unwrap();
        Ok(Arc::new(Box::new(topic.clone()) as Box<dyn TopicOperation>))
    }
}

type TopicMessage = Guard<TopicMessageBase>;

struct TopicMessageBase {
    name: String,
    instant: Instant,
    defer: Defer,
}

unsafe impl Send for TopicMessageBase {}
unsafe impl Sync for TopicMessageBase {}

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
impl TopicOperation for TopicMessage {
    fn name(&self) -> &str {
        self.get().name.as_str()
    }

    async fn seek_defer(&self, block: bool) -> Result<Option<Message>> {
        let msg = self.get().defer.seek().await?;
        Ok(msg)
    }

    async fn next_defer(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() =>{
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.get().defer.pop().await {
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
        let msg = self.get().instant.seek().await?;
        Ok(msg)
    }

    async fn next_instant(&self, block: bool) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.get().instant.pop().await {
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
