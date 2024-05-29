use super::CacheOperation;
use crate::message::Message;
use anyhow::Result;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Memory {
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Memory {
    pub fn new(size: usize) -> Self {
        let (tx, rx) = mpsc::channel(size);
        Memory { tx, rx }
    }
}

impl CacheOperation for Memory {
    async fn push(&mut self, msg: Message) -> Result<()> {
        self.tx.send(msg).await?;
        Ok(())
    }

    async fn pop(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}
