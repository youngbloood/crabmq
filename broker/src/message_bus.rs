use anyhow::{Result, anyhow};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct MessageBus<T: Clone> {
    subscriptions: Arc<DashMap<String, mpsc::UnboundedSender<T>>>,
}

impl<T> MessageBus<T>
where
    T: Clone,
{
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(DashMap::default()),
        }
    }

    pub fn subscribe(&self, id: String) -> Result<mpsc::UnboundedReceiver<T>> {
        if self.subscriptions.contains_key(&id) {
            return Err(anyhow!(format!(
                "MessageBus Key[{}] has been subscribed",
                &id
            )));
        }

        let (tx, rx) = mpsc::unbounded_channel();
        self.subscriptions.insert(id, tx);
        Ok(rx)
    }

    pub fn unsubscribe(&self, id: String) {
        self.subscriptions.remove(&id);
    }

    pub fn broadcast(&self, data: T) {
        for entry in self.subscriptions.iter() {
            let _ = entry.value().send(data.clone());
        }
    }
}
