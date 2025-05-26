// 新增事件总线模块 src/event_bus.rs
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct EventBus<T: Clone> {
    subscriptions: Arc<DashMap<String, mpsc::Sender<T>>>,
    timeout: Duration,
}

impl<T: Clone + Send + 'static> EventBus<T> {
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(DashMap::new()),
            timeout: Duration::from_millis(10),
        }
    }

    pub fn subscribe(&self, id: String) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(12);
        self.subscriptions.insert(id, tx.clone());
        (tx, rx)
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subscriptions.remove(id);
    }

    pub async fn broadcast(&self, event: T) {
        for entry in self.subscriptions.iter() {
            let _ = entry
                .value()
                .send_timeout(event.clone(), self.timeout)
                .await;
        }
    }
}
