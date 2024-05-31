use super::STORAGE_TYPE_DUMMY;
use super::{dummy::Dummy, local::StorageLocal, STORAGE_TYPE_LOCAL};
use crate::message::Message;
use crate::tsuixuq::Tsuixuq;
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use enum_dispatch::enum_dispatch;
use std::sync::atomic::Ordering::SeqCst;
use std::{path::PathBuf, sync::atomic::AtomicU64, time::Duration};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};
use tracing::error;

// #[async_trait::async_trait]
#[enum_dispatch]
pub trait StorageOperation {
    /// init the Storage Media.
    async fn init(&mut self) -> Result<()>;

    /// return the next defer Message from Storage Media.
    async fn next_defer(&self) -> Result<Option<Message>>;

    /// return the next instant Message from Storage Media.
    async fn next_instant(&self) -> Result<Option<Message>>;

    /// push a message into Storage.
    async fn push(&mut self, _: Message) -> Result<()>;

    /// flush the messages to Storage Media.
    async fn flush(&self) -> Result<()>;

    /// stop the Storage Media.
    async fn stop(&self) -> Result<()>;

    // fn topic_create(&self)->Result<()>;

    // TODO: 增加如何接口
    // comsume(&mut self,id:&str)->Result<()>;
    // ready(&mut self,id:&str)->Result<()>;
    // delete(&mut self,id:&str)->Result<()>;
}

#[enum_dispatch(StorageOperation)]
pub enum StorageEnum {
    Local(StorageLocal),
    Memory(Dummy),
}

pub struct StorageWrapper {
    inner: StorageEnum,

    /// 持久化周期，单位：ms
    persist_period: u64,

    persist_factor: u64,
    persist_factor_now: AtomicU64,

    persist_singal_tx: Sender<()>,
    persist_singla_rx: Receiver<()>,
}

impl StorageWrapper {
    fn new(inner: StorageEnum, persist_period: u64, persist_factor: u64) -> Self {
        let (singal_tx, singal_rx) = mpsc::channel(1);
        StorageWrapper {
            inner,
            persist_period,
            persist_factor,
            persist_factor_now: AtomicU64::new(0),
            persist_singal_tx: singal_tx,
            persist_singla_rx: singal_rx,
        }
    }

    pub async fn push(&mut self, msg: Message) -> Result<()> {
        self.persist_factor_now.fetch_add(1, SeqCst);
        if self.persist_factor_now.load(SeqCst) >= self.persist_factor {
            let _ = self.send_persist_singal().await;
        }
        self.inner.push(msg).await
    }

    pub async fn next_defer(&self) -> Option<Message> {
        match self.inner.next_defer().await {
            Ok(msg) => msg,
            Err(e) => {
                error!("get defer message failed: {e:?}");
                None
            }
        }
    }

    pub async fn next_instant(&self) -> Option<Message> {
        match self.inner.next_instant().await {
            Ok(msg) => msg,
            Err(e) => {
                error!("get instant message failed: {e:?}");
                None
            }
        }
    }

    pub async fn send_persist_singal(&self) -> Result<()> {
        self.persist_singal_tx.send(()).await?;
        Ok(())
    }

    pub async fn recv_persist_singal(&mut self) -> Option<()> {
        self.persist_singla_rx.recv().await
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
                if let Err(e) = guard.get_mut().inner.flush().await{
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
    let mut storage = match storage_type {
        STORAGE_TYPE_LOCAL => StorageEnum::Local(StorageLocal::new(
            dir,
            max_msg_num_per_file,
            max_size_per_file,
        )),

        STORAGE_TYPE_DUMMY => StorageEnum::Memory(Dummy::new(100)),

        _ => StorageEnum::Local(StorageLocal::new(
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
