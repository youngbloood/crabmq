use super::{dummy::Dummy, local::StorageLocal, StorageOperation, STORAGE_TYPE_LOCAL};
use crate::message::Message;
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use std::{path::PathBuf, sync::atomic::AtomicU64, time::Duration};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time::interval,
};

pub enum StorageEnum {
    Local(StorageLocal),
    Memory(Dummy),
}

impl StorageOperation for StorageEnum {
    async fn init(&mut self) -> Result<()> {
        todo!()
    }

    async fn next(&mut self) -> Option<Message> {
        todo!()
    }

    async fn push(&mut self, _: Message) -> Result<()> {
        todo!()
    }

    async fn flush(&mut self) -> Result<()> {
        todo!()
    }

    async fn stop(&mut self) -> Result<()> {
        todo!()
    }
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
    pub async fn push(&self, msg: Message) -> Result<()> {
        Ok(())
    }

    pub async fn next(&self) -> Option<Message> {
        None
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

pub fn new_storage_wrapper(
    storage_type: &str,
    dir: PathBuf,
    max_msg_num_per_file: u64,
    max_size_per_file: u64,
    persist_factor: u64,
    persist_period: u64,
) -> Guard<StorageWrapper> {
    let storage = match storage_type {
        STORAGE_TYPE_LOCAL => StorageEnum::Local(StorageLocal::new(
            dir,
            max_msg_num_per_file,
            max_size_per_file,
        )),

        _ => StorageEnum::Local(StorageLocal::new(
            dir,
            max_msg_num_per_file,
            max_size_per_file,
        )),
    };

    let (singal_tx, singal_rx) = mpsc::channel(1);
    let storage_wrapper = StorageWrapper {
        inner: storage,
        persist_period,
        persist_factor,
        persist_factor_now: AtomicU64::new(0),
        persist_singal_tx: singal_tx,
        persist_singla_rx: singal_rx,
    };

    let guard = Guard::new(storage_wrapper);
    tokio::spawn(storage_wrapper_loop(guard.clone()));
    guard
}
