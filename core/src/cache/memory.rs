use crate::message::Message;
use anyhow::Result;
use common::global::Guard;
use dynamic_queue::{DynamicQueue, Queue};
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
}
