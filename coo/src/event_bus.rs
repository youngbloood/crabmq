// 新增事件总线模块 src/event_bus.rs
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct EventBus<T: Clone> {
    subscriptions: Arc<DashMap<String, mpsc::UnboundedSender<T>>>,
}

impl<T: Clone + Send + 'static> EventBus<T> {
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self, id: String) -> Result<mpsc::UnboundedReceiver<T>> {
        // 需要考虑重复问题吗？
        if self.subscriptions.contains_key(&id) {
            return Err(anyhow!(format!(
                "EventBus Key[{}] has been subscribed",
                &id
            )));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscriptions.insert(id, tx);
        Ok(rx)
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subscriptions.remove(id);
    }

    pub async fn broadcast(&self, event: T) {
        for entry in self.subscriptions.iter() {
            let _ = entry.value().send(event.clone());
        }
    }
}
