use crate::message::Message;
use anyhow::{anyhow, Result};
use common::global::Guard;
use dynamic_queue::{DynamicQueue, FlowControl, Queue};
use parking_lot::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub type Memory = DynamicQueue<Message>;

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
    ctrl: FlowControl,

    read_index: AtomicUsize,
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
    pub fn new(size: usize) -> Self {
        MessageCacheSlidingWindows {
            ctrl: FlowControl::new(size),
            read_index: AtomicUsize::default(),
            msgs: RwLock::default(),
        }
    }

    pub fn seek_first(&self) -> Option<MessageWrapper> {
        if self.read_index.load(SeqCst) >= self.msgs.read().len() {
            return None;
        }
        let rd = self.msgs.read();
        if let Some(first_wrapper) = rd.first() {
            return Some(first_wrapper.clone());
        }
        None
    }

    pub fn seek_next(&self) -> Option<Message> {
        if self.read_index.load(SeqCst) >= self.msgs.read().len() {
            return None;
        }
        if let Some(msg_wrapper) = self.msgs.read().get(self.read_index.load(SeqCst)) {
            return Some(msg_wrapper.msg.clone());
        }
        self.read_index.fetch_add(1, SeqCst);
        None
    }

    pub async fn consume(&self, id: &str) -> Option<Message> {
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
                return Some(msg_wrapper.msg);
            }
        }
        None
    }

    async fn push(&self, msg: Message) -> Result<()> {
        self.ctrl.grant(1).await;
        let mut wd = self.msgs.write();
        wd.push(MessageWrapper::new(msg));
        Ok(())
    }

    async fn try_push(&self, msg: Message) -> Result<()> {
        if self.ctrl.has_permit() {
            return Err(anyhow!("not has permit to push msg"));
        }
        self.push(msg).await
    }
}
