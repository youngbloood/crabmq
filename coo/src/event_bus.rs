// 新增事件总线模块 src/event_bus.rs
use dashmap::DashMap;
use log::error;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct EventBus<T: Clone> {
    subscriptions: Arc<DashMap<String, mpsc::Sender<T>>>,
    timeout: Duration,
    event_bus_buffer_size: usize,
}

impl<T: Clone + Send + 'static> EventBus<T> {
    pub fn new(event_bus_buffer_size: usize) -> Self {
        Self {
            subscriptions: Arc::new(DashMap::new()),
            timeout: Duration::from_millis(10),
            event_bus_buffer_size,
        }
    }

    pub fn subscribe(&self, id: String) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        let (tx, rx) = mpsc::channel(self.event_bus_buffer_size);
        self.subscriptions.insert(id, tx.clone());
        (tx, rx)
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subscriptions.remove(id);
    }

    pub async fn broadcast(&self, event: T) {
        for entry in self.subscriptions.iter() {
            if let Err(e) = entry
                .value()
                .send_timeout(event.clone(), self.timeout)
                .await
            {
                error!("event_bus broadcast to {} err: {e:?}", entry.key());
            }
        }
    }
}
