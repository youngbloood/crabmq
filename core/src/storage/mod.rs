/*!
 * Persist Messages into Storage and parse Messages from Storage
 */

mod disk;
mod dummy;

use crate::message::{convert_to_resp, Message};
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use disk::StorageDisk;
use dummy::Dummy;
use enum_dispatch::enum_dispatch;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::{path::PathBuf, sync::atomic::AtomicU64, time::Duration};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};
use tracing::info;

pub const STORAGE_TYPE_DUMMY: &str = "dummy";
pub const STORAGE_TYPE_LOCAL: &str = "local";

#[async_trait::async_trait]
#[enum_dispatch]
pub trait PersistStorageOperation {
    /// init the Storage Media.
    async fn init(&self) -> Result<()>;

    /// get a topic.
    async fn get(&self, topic_name: &str) -> Option<Arc<Box<dyn PersistTopicOperation>>>;

    /// get or create a topic.
    async fn get_or_create_topic(
        &self,
        topic_name: &str,
    ) -> Result<Arc<Box<dyn PersistTopicOperation>>>;

    /// flush the messages to Storage Media.
    async fn flush(&self) -> Result<()>;

    /// push a message into topic.
    async fn push(&self, msg: Message) -> Result<()>;

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

    // mark the message of id has been consumed.
    // async fn comsume(&self, id: &str) -> Result<()>;
    // TODO: 增加如下接口
    // ready(&mut self,id:&str)->Result<()>;
    // delete(&mut self,id:&str)->Result<()>;
}

#[enum_dispatch(PersistStorageOperation)]
pub enum StorageEnum {
    Local(StorageDisk),
    Memory(Dummy),
}

pub struct StorageWrapper {
    storage: StorageEnum,

    /// 持久化周期，单位：ms
    persist_period: u64,

    persist_factor: u64,
    persist_factor_now: AtomicU64,

    persist_singal_tx: Sender<()>,
    persist_singla_rx: Receiver<()>,

    dummy: Dummy,
    // ephemeral_topics: RwLock<HashMap<String, Arc<Box<dyn PersistTopicOperation>>>>,
}

impl StorageWrapper {
    fn new(inner: StorageEnum, persist_period: u64, persist_factor: u64) -> Self {
        let (singal_tx, singal_rx) = mpsc::channel(1);
        StorageWrapper {
            storage: inner,
            persist_period,
            persist_factor,
            persist_factor_now: AtomicU64::new(0),
            persist_singal_tx: singal_tx,
            persist_singla_rx: singal_rx,

            dummy: Dummy::new(10),
            // ephemeral_topics: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_or_create_topic(
        &self,
        topic_name: &str,
        ephemeral: bool,
    ) -> Result<(bool, Arc<Box<dyn PersistTopicOperation>>)> {
        // 已经存在，直接返回
        if self.dummy.contains_key(topic_name) {
            return Ok((true, self.dummy.get(topic_name).await.unwrap().clone()));
        }
        if let Some(topic) = self.storage.get(topic_name).await {
            return Ok((false, topic));
        }

        // 不存在，则直接插入
        if ephemeral {
            self.dummy.insert(topic_name);
            return Ok((true, self.dummy.get(topic_name).await.unwrap().clone()));
        }
        let topic = self.storage.get_or_create_topic(topic_name).await?;
        Ok((false, topic))
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
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) -> Result<()> {
        let topic_name = msg.get_topic();
        let (ephemeral, _) = self
            .get_or_create_topic(topic_name, msg.topic_ephemeral())
            .await?;

        if ephemeral {
            self.dummy.push(msg.clone()).await?;
        } else {
            self.storage.push(msg.clone()).await?;
        }

        // 可能client已经关闭，忽略该err
        let _ = out_sender
            .send((addr.to_string(), convert_to_resp(msg)))
            .await;
        self.persist_factor_now.fetch_add(1, SeqCst);
        if self.persist_factor_now.load(SeqCst) == u64::MAX - 1 {
            self.persist_factor_now.store(0, SeqCst)
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
                    eprintln!("persist MessageQueueDisk err: {e:?}");
                }
            }
        }
    }
}

pub async fn new_storage_wrapper(
    storage_type: &str,
    dir: PathBuf,
    max_msg_num_per_file: u64,
    max_size_per_file: u64,
    persist_factor: u64,
    persist_period: u64,
) -> Result<Guard<StorageWrapper>> {
    let storage = match storage_type {
        STORAGE_TYPE_LOCAL => StorageEnum::Local(StorageDisk::new(
            dir,
            max_msg_num_per_file,
            max_size_per_file,
        )),

        STORAGE_TYPE_DUMMY => StorageEnum::Memory(Dummy::new(100)),

        _ => StorageEnum::Local(StorageDisk::new(
            dir,
            max_msg_num_per_file,
            max_size_per_file,
        )),
    };
    storage.init().await?;

    let storage_wrapper = StorageWrapper::new(storage, persist_period, persist_factor);
    let guard = Guard::new(storage_wrapper);
    tokio::spawn(storage_wrapper_loop(guard.clone()));
    Ok(guard)
}
