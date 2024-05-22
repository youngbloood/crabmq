use super::{MessageManager, MessageQueue};
use crate::message::Message;
use anyhow::Result;
use common::global::Guard;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct MessageQueueDummy {
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

unsafe impl Send for MessageQueueDummy {}
unsafe impl Sync for MessageQueueDummy {}

impl MessageQueueDummy {
    pub fn new(buf_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buf_size);
        MessageQueueDummy { tx, rx }
    }
}

impl MessageQueue for MessageQueueDummy {
    async fn push(&mut self, msg: Message) -> Result<()> {
        self.tx.send(msg).await?;
        Ok(())
    }

    async fn pop(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    fn stop(&mut self) {}
}

pub fn build_dummy_queue(buf_size: usize) -> MessageManager {
    let dummy_queue = MessageQueueDummy::new(buf_size);

    MessageManager::new(Some(Guard::new(dummy_queue)), None)
}
