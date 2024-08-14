/*!
 * Persist Messages into Storage and parse Messages from Storage
 */

pub mod disk;
mod dummy;

use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use disk::config::DiskConfig;
use disk::StorageDisk;
use dummy::Dummy;
use enum_dispatch::enum_dispatch;
use protocol::consts::ACTION_TOUCH;
use protocol::error::{ProtError, E_PUBLISH_FAILED, E_TOPIC_CREATE_FAILED, E_TOPIC_GET_FAILED};
use protocol::message::{Message, MessageOperation as _, TopicCustom};
use protocol::protocol::v1::reply::ReplyBuilder;
use protocol::protocol::{Protocol, ProtocolOperation as _};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::{sync::atomic::AtomicU64, time::Duration};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};
use tracing::error;

use crate::error::ProtocolError;

pub const STORAGE_TYPE_DUMMY: &str = "dummy";
pub const STORAGE_TYPE_LOCAL: &str = "local";

#[async_trait::async_trait]
#[enum_dispatch]
pub trait PersistStorageOperation {
    /// init the Storage Media.
    async fn init(&self, validate: bool) -> Result<()>;

    /// get a topic.
    async fn get(&self, topic_name: &str) -> Result<Option<Arc<Box<dyn PersistTopicOperation>>>>;

    /// get or create a topic.
    async fn get_or_create_topic(
        &self,
        topic_name: &str,
        meta: &TopicMeta,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>>;

    /// flush the messages to Storage Media.
    async fn flush(&self) -> Result<()>;

    /// push a message into topic.
    async fn push(&self, msg: Vec<Message>, meta: &TopicMeta) -> Result<()>;

    /// stop the Storage Media.
    async fn stop(&self) -> Result<()>;

    // TODO: 增加如下接口
    // comsume(&mut self,id:&str)->Result<()>;
    // ready(&mut self,id:&str)->Result<()>;
    // delete(&mut self,id:&str)->Result<()>;
}

#[async_trait::async_trait]
pub trait PersistTopicOperation: Send + Sync {
    /// return the topic name.
    fn name(&self) -> &str;

    ///weather this topic is prohibit defer message
    fn prohibit_defer(&self) -> bool;

    /// return the seek defer Message from Storage Media.
    ///
    /// If there is no seek Message return. This function weather block decide by [`block`].
    async fn seek_defer(&self, block: bool) -> Result<Option<Message>>;

    /// return the next defer Message from Storage Media.
    ///
    /// If there is no message return. This function weather block decide by [`block`].
    ///
    /// It should not return message that has been consumed, deleted, or not ready.
    async fn next_defer(&self, block: bool) -> Result<Option<Message>>;

    ///weather this topic is prohibit instant message
    fn prohibit_instant(&self) -> bool;

    /// return the seek instant Message from Storage Media.
    ///
    /// If there is no message return. This function weather block decide by [`block`].
    async fn seek_instant(&self, block: bool) -> Result<Option<Message>>;

    /// return the next instant Message from Storage Media.
    ///
    /// If there is no message return. This function weather block decide by [`block`].
    ///
    /// It should not return message that has been consumed, deleted, or not ready.
    async fn next_instant(&self, block: bool) -> Result<Option<Message>>;

    /// get the topic meta info.
    fn get_meta(&self) -> Result<TopicMeta>;
    // mark the message of id has been consumed.
    // async fn comsume(&self, id: &str) -> Result<()>;
    // TODO: 增加如下接口
    // ready(&mut self,id:&str)->Result<()>;
    // delete(&mut self,id:&str)->Result<()>;

    async fn update_consume(&self, id: &str, consume: bool) -> Result<()>;

    async fn update_delete(&self, id: &str, delete: bool) -> Result<()>;

    async fn update_notready(&self, id: &str, notready: bool) -> Result<()>;
}

#[enum_dispatch(PersistStorageOperation)]
pub enum StorageEnum {
    Disk(StorageDisk),
    Memory(Dummy),
}

pub struct StorageWrapper {
    /// 持久化周期，单位：ms
    persist_period: u64,

    persist_factor: u64,
    persist_factor_now: AtomicU64,

    persist_singal_tx: Sender<()>,
    persist_singla_rx: Receiver<()>,

    storage: StorageEnum,
    dummy: Dummy,
}

impl StorageWrapper {
    fn new(inner: StorageEnum, persist_period: u64, persist_factor: u64) -> Self {
        let (singal_tx, singal_rx) = mpsc::channel(1);
        StorageWrapper {
            persist_period,
            persist_factor,
            persist_factor_now: AtomicU64::new(0),
            persist_singal_tx: singal_tx,
            persist_singla_rx: singal_rx,

            storage: inner,
            dummy: Dummy::new(10),
        }
    }

    pub async fn get_or_create_topic(
        &self,
        topic_name: &str,
        ephemeral: bool,
        meta: &TopicMeta,
    ) -> Result<(bool, Arc<Box<dyn PersistTopicOperation>>)> {
        // 已经存在，直接返回
        if self.dummy.contains_key(topic_name) {
            return Ok((true, self.dummy.get(topic_name).await?.unwrap().clone()));
        }

        match self.storage.get(topic_name).await {
            Ok(topic) => {
                if let Some(topic) = topic {
                    return Ok((false, topic));
                }
            }
            Err(e) => {
                error!("get topic[{topic_name}] from storage failed: {e:?}");
                return Err(ProtError::new(E_TOPIC_GET_FAILED).into());
            }
        }

        // 不存在，则直接插入
        if ephemeral {
            match self.dummy.insert(topic_name, meta) {
                Ok(_) => return Ok((true, self.dummy.get(topic_name).await?.unwrap().clone())),
                Err(_) => return Err(ProtError::new(E_TOPIC_CREATE_FAILED).into()),
            }
        }

        match self.storage.get_or_create_topic(topic_name, meta).await {
            Ok(topic) => Ok((false, topic)),
            Err(_) => Err(ProtError::new(E_TOPIC_CREATE_FAILED).into()),
        }
    }

    pub async fn send_persist_singal(&self) -> Result<()> {
        self.persist_singal_tx.send(()).await?;
        Ok(())
    }

    pub async fn recv_persist_singal(&mut self) -> Option<()> {
        self.persist_singla_rx.recv().await
    }

    pub async fn push(
        &self,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        prot: Protocol,
        epehemral: bool,
    ) -> Result<()> {
        let msgs = prot.convert_to_message()?;
        if msgs.is_empty() {
            return Ok(());
        }
        let topic_name = msgs[0].get_topic();
        let meta = covert_protocol_to_topicmeta(&prot);

        let (ephemeral, _) = self
            .get_or_create_topic(topic_name, epehemral, &meta)
            .await?;

        if ephemeral {
            if let Err(e) = self.dummy.push(msgs, &meta).await {
                error!("push to dummy err: {e:?}");
                return Err(ProtError::new(E_PUBLISH_FAILED).into());
            }
        } else if let Err(e) = self.storage.push(msgs, &meta).await {
            error!("push to storage err: {e:?}");
            return Err(ProtError::new(E_PUBLISH_FAILED).into());
        }

        self.persist_factor_now.fetch_add(1, Relaxed);
        if self.persist_factor_now.load(Relaxed) == u64::MAX {
            self.persist_factor_now.store(0, Relaxed)
        }
        Ok(())
    }
}

async fn storage_wrapper_loop(guard: Guard<StorageWrapper>) {
    let mut ticker = interval(Duration::from_millis(guard.get().persist_period));
    loop {
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return ;
            }

            _ = ticker.tick() => {
                let _ = guard.get().send_persist_singal().await;
            }

            _ = guard.get_mut().recv_persist_singal() => {
                // info!("persist all message to storaga media.");
                if let Err(e) = guard.get_mut().storage.flush().await{
                    error!("persist MessageQueueDisk err: {e:?}");
                }
            }
        }
    }
}

pub async fn new_storage_wrapper(
    storage_type: &str,
    disk_cfg: DiskConfig,
) -> Result<Guard<StorageWrapper>> {
    let persist_period = disk_cfg.persist_period;
    let persist_factor = disk_cfg.persist_factor;

    let storage = match storage_type {
        STORAGE_TYPE_LOCAL => StorageEnum::Disk(StorageDisk::new(disk_cfg)),

        STORAGE_TYPE_DUMMY => StorageEnum::Memory(Dummy::new(100)),

        _ => StorageEnum::Disk(StorageDisk::new(disk_cfg)),
    };
    storage.init(false).await?;

    let storage_wrapper = StorageWrapper::new(storage, persist_period, persist_factor);
    let guard = Guard::new(storage_wrapper);
    tokio::spawn(storage_wrapper_loop(guard.clone()));
    Ok(guard)
}

/**
 * [`TopicMeta`] should compatable with all [`TopicCustom`]
 */
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TopicMeta {
    pub prohibit_instant: bool,
    pub prohibit_defer: bool,
    pub defer_message_format: String,
    pub max_msg_num_per_file: u64,
    pub max_size_per_file: u64,
    pub compress_type: u8,
    pub subscribe_type: u8,
    pub record_num_per_file: u64,
    pub record_size_per_file: u64,
    pub fd_cache_size: u64,
}

impl TopicMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
        TopicMeta {
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
        }
    }
}

pub fn covert_protocol_to_topicmeta(prot: &Protocol) -> TopicMeta {
    match prot {
        Protocol::V1(v1) => match v1.get_action() {
            ACTION_TOUCH => {
                if let Some(touch) = v1.get_touch() {
                    return TopicMeta::new(
                        touch.prohibit_instant(),
                        touch.prohibit_defer(),
                        touch.get_defer_msg_format().to_string(),
                        touch.get_max_msg_num_per_file(),
                        touch.get_max_size_per_file(),
                        touch.get_compress_type(),
                        touch.get_subscribe_type(),
                        touch.get_record_num_per_file(),
                        touch.get_record_size_per_file(),
                        touch.get_fd_cache_size(),
                    );
                }
                TopicMeta::default()
            }
            _ => TopicMeta::default(),
        },
        _ => TopicMeta::default(),
    }
}

// fn covert_message_to_topicmeta(msg: &Message) -> TopicMeta {
//     todo!()
// }
