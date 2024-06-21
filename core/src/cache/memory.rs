use crate::message::Message;
use anyhow::{anyhow, Result};
use common::global::Guard;
use dynamic_queue::{DynamicQueue, FlowControl, Queue};
use parking_lot::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::CacheOperation;

pub struct MessageCache {
    sender: UnboundedSender<Message>,
    recver: Guard<UnboundedReceiver<Message>>,
}

impl MessageCache {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        MessageCache {
            sender: tx,
            recver: Guard::new(rx),
        }
    }
}

#[async_trait::async_trait]
impl Queue for MessageCache {
    type Item = Message;

    async fn push(&self, msg: Message) -> Result<()> {
        self.sender.send(msg)?;
        Ok(())
    }

    async fn pop(&self) -> Option<Message> {
        self.recver.get_mut().recv().await
    }

    fn resize(&self, _: usize) {}
}

pub struct MessageCacheSlidingWindows {
    /// 流量控制
    ctrl: FlowControl,

    /// 滑动窗口大小
    slide_win: AtomicUsize,

    /// 读取msgs的pos指针
    read_ptr: AtomicUsize,

    /// msgs消息列表
    msgs: RwLock<Vec<MessageWrapper>>,
}

struct MessageWrapper {
    msg: Message,
    consumed: bool,
}

impl MessageWrapper {
    pub fn new(msg: Message) -> Self {
        MessageWrapper {
            msg,
            consumed: false,
        }
    }

    pub fn clone(&self) -> Self {
        MessageWrapper {
            msg: self.msg.clone(),
            consumed: self.consumed,
        }
    }
}

impl MessageCacheSlidingWindows {
    pub fn new(cap: usize, slide_win: usize) -> Self {
        let mut _slide_win = slide_win;
        if _slide_win > cap {
            _slide_win = cap;
        }
        MessageCacheSlidingWindows {
            ctrl: FlowControl::new(cap),
            slide_win: AtomicUsize::new(_slide_win),
            read_ptr: AtomicUsize::default(),
            msgs: RwLock::default(),
        }
    }
}

impl CacheOperation for MessageCacheSlidingWindows {
    #[doc = " push a message into cache. it will be block if the cache is filled."]
    async fn try_push(&self, msg: Message) -> Result<()> {
        if self.ctrl.has_permit() {
            return Err(anyhow!("not has permit to push msg"));
        }
        self.push(msg).await
    }

    #[doc = " push a message into cache. it will be block if the cache is filled."]
    async fn push(&self, msg: Message) -> Result<()> {
        self.ctrl.grant(1).await;
        let mut wd = self.msgs.write();
        wd.push(MessageWrapper::new(msg));
        Ok(())
    }

    #[doc = " pop a message from cache. if it is defer message, cache should control it pop when it\'s expired. or pop the None"]
    async fn pop(&self) -> Option<Message> {
        if self.read_ptr.load(SeqCst) >= self.slide_win.load(SeqCst) {
            return None;
        }
        if let Some(msg_wrapper) = self.msgs.read().get(self.read_ptr.load(SeqCst)) {
            return Some(msg_wrapper.msg.clone());
        }
        self.read_ptr.fetch_add(1, SeqCst);
        None
    }

    async fn consume(&self, id: &str) -> Option<Message> {
        let mut wd = self.msgs.write();
        let mut index = -1;
        for (i, msg_wrapper) in wd.iter_mut().enumerate() {
            if id != msg_wrapper.msg.id() {
                continue;
            }
            msg_wrapper.consumed = true;
            index = i as i32;
        }
        // 消费确认了index=0的消息，则该消息需要出队列
        if index == 0 {
            if let Some(msg_wrapper) = wd.pop() {
                self.ctrl.revert(1).await;
                if self.read_ptr.load(SeqCst) >= 1 {
                    self.read_ptr.fetch_sub(1, SeqCst);
                }

                return Some(msg_wrapper.msg);
            }
        }
        None
    }

    #[doc = " resize the buffer length in cache."]
    async fn resize(&self, cap: usize, slide_win: usize) {
        self.ctrl.resize(cap);
        let mut _slide_win = slide_win;
        if _slide_win > cap {
            _slide_win = cap;
        }
        self.slide_win
            .store(_slide_win, std::sync::atomic::Ordering::SeqCst);
        self.slide_win.store(_slide_win, SeqCst);
    }
}
