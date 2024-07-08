use std::{sync::Arc, time::Duration};

use crate::{
    cache::{CacheWrapper, CACHE_TYPE_MEM},
    message::Message,
    storage::{PersistTopicOperation, STORAGE_TYPE_DUMMY},
    topic::Topic,
    tsuixuq::TsuixuqOption,
};
use anyhow::Result;
use chrono::Local;
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::interval,
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub struct TopicBus {
    name: String,
    opt: Guard<TsuixuqOption>,
    use_memory: bool,
    start_message_loop: bool,
    start_defer_message_loop: bool,

    /// 缓存已经准备消费的消息buffer
    ready_queue: CacheWrapper,

    /// 缓存从storage中超前加载的defer message，不一定到期
    defer_cache: CacheWrapper,

    /// 内存中的topic
    pub topic: Guard<Topic>,

    /// 真实存储message的地方，可能是memory
    storage_topic: Arc<Box<dyn PersistTopicOperation>>,

    stop: CancellationToken,
}

impl TopicBus {
    pub async fn new(
        opt: Guard<TsuixuqOption>,
        topic: Guard<Topic>,
        s: Arc<Box<dyn PersistTopicOperation>>,
    ) -> Result<Self> {
        if opt.get().message_storage_type.as_str() == STORAGE_TYPE_DUMMY {
            return Ok(TopicBus {
                opt,
                name: topic.get().name.as_str().to_string(),
                use_memory: true,
                start_message_loop: false,
                start_defer_message_loop: false,
                topic,
                ready_queue: CacheWrapper::new(CACHE_TYPE_MEM, 100, 100),
                defer_cache: CacheWrapper::new(CACHE_TYPE_MEM, 100, 100),
                storage_topic: s,
                stop: CancellationToken::new(),
            });
        }

        Ok(TopicBus {
            opt: opt.clone(),
            name: topic.get().name.as_str().to_string(),
            use_memory: false,
            start_message_loop: false,
            start_defer_message_loop: false,
            topic,
            ready_queue: CacheWrapper::new(CACHE_TYPE_MEM, 100, 100),
            defer_cache: CacheWrapper::new(CACHE_TYPE_MEM, 100, 100),
            storage_topic: s,
            stop: CancellationToken::new(),
        })
    }

    /// push 至storage中进行持久化存储
    // pub async fn push(
    //     &self,
    //     out_sender: Sender<(String, Message)>,
    //     addr: &str,
    //     msg: Message,
    // ) -> Result<()> {
    //     if !self.use_memory {
    //         return self.storage_topic.push(msg).await;
    //     }
    //     if msg.is_defer() {
    //         return self.defer_cache.push(msg).await;
    //     }
    //     return self.ready_queue.push(msg).await;
    // }

    /// 从cache中pop出一个Message
    pub async fn pop(&mut self, block: bool) -> Option<Message> {
        self.ready_queue.pop(block).await
    }
}

pub async fn topic_message_loop(guard: Guard<TopicBus>) {
    if guard.get().start_message_loop {
        return;
    }

    let mut ticker = interval(Duration::from_millis(300)).await;
    guard.get_mut().start_message_loop = true;
    let topic_name = guard.get().name.as_str();
    loop {
        info!("LOOP: load topic[{topic_name}] message from storage...");
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return;
            }

            _ = guard.get().stop.cancelled() => {
                let topic_name = guard.get().name.as_str();
                info!("topic[{topic_name}] stopped. then will exit the 'topic_message_loop'");
                return;
            }

            // 不断获取defer message，并发送至defer_cache中
            msg = guard.get().storage_topic.seek_defer(true) => {
                match msg {
                    Ok(msg) => {
                        if msg.is_none() {
                            continue;
                        }
                        debug!("topic[{topic_name}] get defer msg: {msg:?}");
                        let msg = msg.unwrap();
                        match guard.get().defer_cache.try_push(msg).await {
                            Ok(_) => {
                                let _ = guard.get().storage_topic.next_defer(false).await;
                            }
                            Err(e) => {
                                error!("push defer message to defer_cache failed: {e:?}");
                            }
                        }
                    }

                    Err(e) => {
                        error!("next instant err:{e}");
                    }
                }
            }

            // 不断获取instant message，并发送至ready_queue中
            msg = guard.get().storage_topic.seek_instant(true) => {
                match msg {
                    Ok(msg) => {
                        if msg.is_none(){
                            continue;
                        }
                        debug!("topic[{topic_name}] get instant msg: {msg:?}");
                        let msg = msg.unwrap();
                        match  guard.get().ready_queue.try_push(msg).await {
                            Ok(_) => {
                                let _ = guard.get().storage_topic.next_instant(false).await;
                            }
                            Err(e) => {
                                error!("push instant message to ready_queue failed: {e:?}");
                            }
                        }
                    }

                    Err(e) => {
                        error!("next instant err:{e}");
                    }
                }
            }

            // 不断从ready_queue中取得消息，下发至topic下的channel中
            msg = guard.get().ready_queue.pop(true) => {
                if msg.is_none(){
                    continue;
                }
                debug!("topic[{topic_name}] get ready msg: {msg:?}");
                let msg = msg.unwrap();
                let _ = guard.get().topic.get().deliver_message(msg).await;
            }

            // 检查是否topic是否应该退出
            _ = async {
                loop {
                    if guard.get().topic.get().is_client_empty().await {
                        guard.get().stop.cancel();
                        return;
                    }else{
                        ticker.tick().await;
                    }
                }
            } => {}
        }
    }
}

pub async fn topic_message_loop_defer(guard: Guard<TopicBus>) {
    if guard.get().start_defer_message_loop {
        return;
    }
    guard.get_mut().start_defer_message_loop = true;
    let topic_name = guard.get().name.as_str();
    loop {
        info!("LOOP: load topic[{topic_name}] defer message from storage...");
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return;
            }

            _ = guard.get().stop.cancelled() => {
                let topic_name = guard.get().name.as_str();
                info!("topic[{topic_name}] stopped. then will exit the 'topic_message_loop_defer'");
                return;
            }

            // 处理defer message
            msg = guard.get().defer_cache.pop(true) => {
                if msg.is_none(){
                    continue;
                }
                let msg = msg.unwrap();
                if msg.defer_time() == 0 || msg.defer_time() <= Local::now().timestamp() as u64 {
                    let _ = guard.get().ready_queue.push(msg).await;
                    continue;
                }

                let diff = Local::now().timestamp() as u64 - msg.defer_time();
                let mut ticker = interval(Duration::from_secs(diff)).await;

                select!{
                    _ = CANCEL_TOKEN.cancelled() => {
                        return;
                    }

                    _ = guard.get().stop.cancelled() => {
                        return;
                    }
                    // send the expired-defer-message to ready_buffer
                    _ = ticker.tick() => {
                        let _ = guard.get().ready_queue.push(msg).await;
                    }
                }
            }
        }
    }
}

pub async fn new_topic_message(
    opt: Guard<TsuixuqOption>,
    topic: Guard<Topic>,
    s: Arc<Box<dyn PersistTopicOperation>>,
) -> Result<Guard<TopicBus>> {
    let tm = TopicBus::new(opt, topic, s).await?;
    let guard = Guard::new(tm);
    Ok(guard)
}
