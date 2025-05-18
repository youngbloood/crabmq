pub mod config;
mod defer;
mod instant;
mod message_manager;
mod record;

use super::{PersistStorageOperation, PersistTopicOperation, TopicMeta};
use anyhow::{anyhow, Error, Result};
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_dir, check_exist, interval},
};
use config::DiskConfig;
use dashmap::DashMap;
use defer::Defer;
use instant::Instant;
use protocol::{
    error::{ProtError, E_TOPIC_PROHIBIT_DEFER, E_TOPIC_PROHIBIT_INSTANT, E_TOPIC_PROHIBIT_TYPE},
    message::{Message, MessageOperation as _},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    ops::Deref,
    path::PathBuf,
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
    cfg: DiskConfig,
    /// topic_name: TopicMessage
    topics: DashMap<String, TopicMessage>,

    /// 取消信号
    cancel: CancellationToken,
}

impl StorageDisk {
    pub fn new(cfg: DiskConfig) -> Self {
        StorageDisk {
            cfg,
            topics: DashMap::new(),
            cancel: CancellationToken::new(),
        }
    }

    async fn get_topic_from_dir(&self, dir_name: PathBuf) -> Result<()> {
        let topic_name = dir_name.file_name().unwrap().to_str().unwrap();
        info!("LAZY: load topic[{topic_name}]...");

        let meta = TopicMetaWrapper::read_from(dir_name.join("meta"))?;

        let defer = Defer::new(
            dir_name.clone(),
            meta.defer_message_format.as_str(),
            meta.max_msg_num_per_file,
            meta.max_size_per_file,
            meta.fd_cache_size as _,
        )?;
        defer.load().await?;

        let instant = Instant::new(
            dir_name.clone(),
            meta.max_msg_num_per_file,
            meta.max_size_per_file,
            meta.record_num_per_file,
            meta.record_size_per_file,
            meta.fd_cache_size as _,
        )?;
        instant.load().await?;

        self.topics.insert(
            topic_name.to_string(),
            TopicMessage::new(TopicMessageBase::new(
                topic_name, // topic_meta.prohibit_instant,
                instant,    // topic_meta.prohibit_defer,
                defer, meta,
            )),
        );
        Ok(())
    }

    async fn get_topic(&self, topic_name: &str) -> Result<Option<TopicMessage>> {
        if !self.topics.contains_key(topic_name) {
            let parent = self.cfg.storage_dir.join(topic_name);
            if !check_exist(&parent) {
                return Ok(None);
            }

            info!("LAZY: load topic[{topic_name}]...");
            self.get_topic_from_dir(parent).await?;
        }
        let topic = self.topics.get(topic_name).unwrap();
        Ok(Some(topic.value().clone()))
    }

    async fn get_or_create_topic_inner(
        &self,
        topic_name: &str,
        meta: &TopicMeta,
    ) -> Result<TopicMessage> {
        let cfg = self.cfg.mix_with_topicmeta(meta);

        if !self.topics.contains_key(topic_name) {
            // validate
            // TODO: 移到协议端校验
            if meta.prohibit_defer && meta.prohibit_instant {
                return Err(Error::from(ProtError::new(E_TOPIC_PROHIBIT_TYPE)));
            }

            let parent = cfg.storage_dir.join(topic_name);
            check_and_create_dir(&parent)?;
            debug!("StorageDisk: load topic: {}", topic_name);

            // init the topic-message
            let defer = Defer::new(
                parent.join("defer"),
                &cfg.default_defer_message_format,
                cfg.default_max_msg_num_per_file,
                cfg.default_max_size_per_file,
                cfg.default_fd_cache_size as _,
            )?;
            let instant = Instant::new(
                parent.clone(),
                cfg.default_max_msg_num_per_file,
                cfg.default_max_size_per_file,
                cfg.default_record_num_per_file,
                cfg.default_record_size_per_file,
                cfg.default_fd_cache_size as _,
            )?;

            let topic_meta = TopicMetaWrapper::new(
                parent.join("meta"),
                meta.prohibit_instant,
                meta.prohibit_defer,
                cfg.default_defer_message_format.to_string(),
                cfg.default_max_msg_num_per_file,
                cfg.default_max_size_per_file,
                cfg.default_compress_type,
                cfg.default_subscribe_type,
                cfg.default_record_num_per_file,
                cfg.default_record_size_per_file,
                cfg.default_fd_cache_size,
            );
            topic_meta.persist()?;

            let mut topic_mb = TopicMessageBase::new(
                topic_name, // prohibit_instant,
                instant,    // prohibit_defer,
                defer, topic_meta,
            );
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
        check_and_create_dir(&self.cfg.storage_dir)?;
        if !validate {
            return Ok(());
        }
        for entry in fs::read_dir(&self.cfg.storage_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                continue;
            }
            self.get_topic_from_dir(entry.path()).await?;
        }
        Ok(())
    }

    async fn push(&self, msgs: Vec<Message>, meta: &TopicMeta) -> Result<()> {
        let topic = self
            .get_or_create_topic_inner(msgs[0].get_topic(), meta)
            .await?;
        topic.push_msgs(msgs).await?;
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
        meta: &TopicMeta,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>> {
        let topic = self.get_or_create_topic_inner(topic_name, meta).await?;
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
        if msg.defer_time() != 0 {
            if self.guard.get().meta.prohibit_defer {
                return Err(Error::from(ProtError::new(E_TOPIC_PROHIBIT_DEFER)));
            }
            self.guard.get().defer.handle_msg(msg).await?;
            return Ok(());
        }

        if self.guard.get().meta.prohibit_instant {
            return Err(Error::from(ProtError::new(E_TOPIC_PROHIBIT_INSTANT)));
        }
        self.guard.get().instant.handle_msg(msg).await?;
        Ok(())
    }

    async fn push_msgs(&self, msgs: Vec<Message>) -> Result<()> {
        for msg in msgs {
            self.push(msg).await?;
        }
        Ok(())
    }
}

struct TopicMetaWrapper {
    filename: PathBuf,
    meta: TopicMeta,
}

impl Deref for TopicMetaWrapper {
    type Target = TopicMeta;

    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

impl TopicMetaWrapper {
    #[allow(clippy::too_many_arguments)]
    fn new(
        filename: PathBuf,
        prohibit_instant: bool,
        prohibit_defer: bool,
        defer_message_format: String,
        max_msg_num_per_file: u64,
        max_size_per_file: u64,
        compress_type: u8,
        subscribe_type: u8,
        record_num_per_file: u64,
        record_size_per_file: u64,
        fd_cache_size: u64,
    ) -> Self {
        TopicMetaWrapper {
            filename,
            meta: TopicMeta::new(
                prohibit_instant,
                prohibit_defer,
                defer_message_format,
                max_msg_num_per_file,
                max_size_per_file,
                compress_type,
                subscribe_type,
                record_num_per_file,
                record_size_per_file,
                fd_cache_size,
            ),
        }
    }

    fn read_from(filename: PathBuf) -> Result<Self> {
        let out = fs::read_to_string(&filename)?;
        let meta: TopicMeta = serde_json::from_str(&out)?;
        Ok(TopicMetaWrapper { filename, meta })
    }

    fn persist(&self) -> Result<()> {
        let mut fd = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&self.filename)?;
        let out = serde_json::to_string_pretty(&self.meta)?;
        fd.write_all(&out.as_bytes())?;
        Ok(())
    }
}

struct TopicMessageBase {
    name: String,
    instant: Instant,
    defer: Defer,
    meta: TopicMetaWrapper,
}

impl TopicMessageBase {
    fn new(name: &str, instant: Instant, defer: Defer, meta: TopicMetaWrapper) -> Self {
        TopicMessageBase {
            name: name.to_string(),
            instant,
            defer,
            meta,
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

    fn prohibit_defer(&self) -> bool {
        self.guard.get().meta.prohibit_defer
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

                        Err(e) => return Err(anyhow!("{e}")),
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
                                    if !(msg.is_deleted() || msg.is_consumed() || msg.is_notready()) {
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
                            Err(anyhow!("{e}"))
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

    fn prohibit_instant(&self) -> bool {
        self.guard.get().meta.prohibit_instant
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

                        Err(e) => return Err(anyhow!("{e}")),
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
                                    if !(msg.is_deleted() || msg.is_consumed() || msg.is_notready()) {
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
                            Err(anyhow!("{e}"))
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
                            return Err(anyhow!("{e}"));
                        }
                    }
                }
            }
        }
    }

    fn get_meta(&self) -> Result<TopicMeta> {
        Ok(self.guard.get().meta.meta.clone())
    }

    async fn update_consume(&self, id: &str, consume: bool) -> Result<()> {
        self.guard.get().instant.update_consume(id, consume).await?;
        self.guard.get().defer.update_consume(id, consume).await?;
        Ok(())
    }

    async fn update_delete(&self, id: &str, delete: bool) -> Result<()> {
        self.guard.get().instant.update_delete(id, delete).await?;
        self.guard.get().defer.update_delete(id, delete).await?;
        Ok(())
    }

    async fn update_notready(&self, id: &str, notready: bool) -> Result<()> {
        self.guard
            .get()
            .instant
            .update_notready(id, notready)
            .await?;
        self.guard.get().defer.update_notready(id, notready).await?;
        Ok(())
    }
}
