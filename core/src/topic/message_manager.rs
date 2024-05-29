use std::path::Path;

use crate::{
    cache::cache::CacheWrapper,
    message::Message,
    storage::{
        storage::{new_storage_wrapper, StorageWrapper},
        STORAGE_TYPE_DUMMY,
    },
    tsuixuq::TsuixuqOption,
};
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use tokio::select;
use tracing::debug;

pub struct MessageManager {
    opt: Guard<TsuixuqOption>,
    cache: Option<CacheWrapper>,
    storage: Guard<StorageWrapper>,
}

impl MessageManager {
    pub fn new(opt: Guard<TsuixuqOption>, ephemeral: bool) -> Self {
        if ephemeral {
            return MessageManager {
                opt: opt.clone(),
                cache: None,
                storage: new_storage_wrapper(
                    STORAGE_TYPE_DUMMY,
                    Path::new(opt.get().message_dir.as_str()).join(""),
                    opt.get().max_num_per_file,
                    opt.get().max_size_per_file,
                    opt.get().persist_message_factor,
                    opt.get().persist_message_period,
                ),
            };
        }

        let storage = new_storage_wrapper(
            opt.get().message_storage_type.as_str(),
            Path::new(opt.get().message_dir.as_str()).join(""),
            opt.get().max_num_per_file,
            opt.get().max_size_per_file,
            opt.get().persist_message_factor,
            opt.get().persist_message_period,
        );

        if opt.get().message_storage_type.as_str() == STORAGE_TYPE_DUMMY {
            return MessageManager {
                opt,
                cache: None,
                storage: storage,
            };
        }

        MessageManager {
            opt: opt.clone(),
            cache: Some(CacheWrapper::new(
                opt.get().message_cache_type.as_str(),
                100,
            )),
            storage: storage,
        }
    }

    pub async fn push(&self, msg: Message) -> Result<()> {
        self.storage.get_mut().push(msg).await
    }

    pub async fn pop(&mut self) -> Option<Message> {
        if let Some(cache) = self.cache.as_mut() {
            return cache.pop().await;
        }

        None
    }
}

async fn message_manager_loop(guard: Guard<MessageManager>) {
    if guard.get().cache.is_none() {
        return;
    }

    loop {
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return;
            }

            msg = guard.get_mut().storage.get_mut().next() => {
                if msg.is_none(){
                    continue;
                }
                let msg = msg.unwrap();
                let _ = guard.get_mut().cache.as_mut().unwrap().push(msg).await;
            }
        }
    }
}

pub fn new_message_manager(opt: Guard<TsuixuqOption>, ephemeral: bool) -> Guard<MessageManager> {
    let mm = MessageManager::new(opt, ephemeral);
    let guard = Guard::new(mm);
    tokio::spawn(message_manager_loop(guard.clone()));
    guard
}
