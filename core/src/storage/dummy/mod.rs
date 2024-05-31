use super::storage::StorageOperation;
use crate::message::Message;
use anyhow::Result;
use dynamic_queue::{DefaultQueue, Queue};

pub struct Dummy(DefaultQueue<Message>);

impl Dummy {
    pub fn new(buf: usize) -> Self {
        Dummy {
            0: DefaultQueue::new(buf),
        }
    }
}

impl StorageOperation for Dummy {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next_defer(&self) -> Result<Option<Message>> {
        Ok(self.0.pop().await)
    }

    async fn next_instant(&self) -> Result<Option<Message>> {
        Ok(self.0.pop().await)
    }

    async fn push(&mut self, msg: Message) -> Result<()> {
        self.0.push(msg).await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
