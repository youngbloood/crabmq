pub mod disk;
pub mod dummy;

use crate::message::Message;
use anyhow::Result;
use common::global::Guard;
use tracing::debug;

use self::{disk::MessageQueueDisk, dummy::MessageQueueDummy};

pub trait MessageQueue: Send + Sync {
    /// push a message into MessageQueue.
    async fn push(&mut self, msg: Message) -> Result<()>;

    /// pop a message from MessageQueue.
    async fn pop(&mut self) -> Option<Message>;

    /// notify the queue stop.
    fn stop(&mut self);
}

pub struct MessageManager {
    /// dummy 和 disk互斥
    dummy: Option<Guard<MessageQueueDummy>>,

    /// dummy 和 disk互斥
    disk: Option<Guard<MessageQueueDisk>>,
}

impl MessageManager {
    pub fn new(
        dummy: Option<Guard<MessageQueueDummy>>,
        disk: Option<Guard<MessageQueueDisk>>,
    ) -> Self {
        if dummy.is_some() {
            return MessageManager { dummy, disk: None };
        }
        MessageManager { dummy: None, disk }
    }

    pub async fn push(&self, msg: Message) -> Result<()> {
        debug!("push msg to MessageManager: {msg:?}");
        if let Some(guard) = self.dummy.as_ref() {
            guard.get_mut().push(msg.clone()).await?;
        }
        if let Some(guard) = self.disk.as_ref() {
            guard.get_mut().push(msg.clone()).await?;
        }

        Ok(())
    }

    pub async fn pop(&self) -> Option<Message> {
        if let Some(guard) = self.dummy.as_ref() {
            return guard.get_mut().pop().await;
        }
        if let Some(guard) = self.disk.as_ref() {
            return guard.get_mut().pop().await;
        }

        None
    }

    pub fn stop(&self) {
        if let Some(guard) = self.dummy.as_ref() {
            guard.get_mut().stop();
        }
        if let Some(guard) = self.disk.as_ref() {
            guard.get_mut().stop();
        }
    }
}
