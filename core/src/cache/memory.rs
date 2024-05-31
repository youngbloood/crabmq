use std::vec;

use crate::message::Message;
use anyhow::Result;
use chrono::Local;
use dynamic_queue::{DynamicQueue, Queue};
use parking_lot::RwLock;

pub type Memory = DynamicQueue<Message>;

pub struct MessageCache {
    defer: RwLock<Vec<Message>>,
    instant: RwLock<Vec<Message>>,
}

impl MessageCache {
    pub fn new(defer_len: usize, instant_len: usize) -> Self {
        MessageCache {
            defer: RwLock::new(Vec::with_capacity(defer_len)),
            instant: RwLock::new(Vec::with_capacity(instant_len)),
        }
    }
}

#[async_trait::async_trait]
impl Queue for MessageCache {
    type Item = Message;

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            let mut wg = self.defer.write();
            wg.push(msg);
            return Ok(());
        }
        let mut wg = self.instant.write();
        wg.push(msg);
        Ok(())
    }

    async fn pop(&self) -> Option<Message> {
        let mut should_pop_defer: bool = false;
        {
            let rg = self.defer.read();
            if rg.len() != 0 {
                let seek_msg = rg.get(0).unwrap();
                let now = Local::now().timestamp();
                if seek_msg.defer_time() <= now as u64 {
                    should_pop_defer = true
                }
            }
        }

        if should_pop_defer {
            let mut wg = self.defer.write();
            return wg.pop();
        }

        let mut wg = self.instant.write();
        wg.pop()
    }
}
