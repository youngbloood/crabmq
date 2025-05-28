use bytes::Bytes;
use dashmap::DashMap;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;

type MessageBusKey = (String, u32);
type MessageBusValue = Arc<DashMap<String, mpsc::Sender<Bytes>>>;

#[derive(Debug, Clone)]
pub struct MessageBus {
    timeout: Duration,
    // 生产者总线：topic-partition  -> ModuleName -> sender
    producers: Arc<DashMap<MessageBusKey, MessageBusValue>>,
    // 消费者总线：topic-partition ->  ModuleName -> sender
    consumers: Arc<DashMap<MessageBusKey, MessageBusValue>>,

    message_bus_producer_buffer_size: usize,
    message_bus_consumer_buffer_size: usize,
}

impl MessageBus {
    pub fn new(
        message_bus_producer_buffer_size: usize,
        message_bus_consumer_buffer_size: usize,
    ) -> Self {
        Self {
            timeout: Duration::from_millis(10),
            producers: Arc::new(DashMap::new()),
            consumers: Arc::new(DashMap::new()),
            message_bus_producer_buffer_size,
            message_bus_consumer_buffer_size,
        }
    }

    /// 订阅 生产者消息
    /// 从 pub 客户端来的消息，会先发送到 Storage 落盘存储后，会调用 broadcast_producer_message 将消息广播至该“生产消息订阅者”
    pub fn subscribe_producer(
        &self,
        module_name: String,
        topic: &str,
        partition: u32,
    ) -> mpsc::Receiver<Bytes> {
        let key = (topic.to_string(), partition);
        let (tx, rx) = mpsc::channel(self.message_bus_producer_buffer_size);
        self.producers.entry(key).and_modify(|v| {
            v.insert(module_name.clone(), tx.clone());
        });

        rx
    }

    /// 取消某个模块订阅 生产者消息
    pub fn unsubscribe_producer(&self, module_name: &str, topic: &str, partition: u32) {
        let key = (topic.to_string(), partition);
        self.producers.entry(key).and_modify(|v| {
            v.remove(module_name);
        });
    }

    // 生产者发送消息
    pub async fn broadcast_producer_message(&self, topic: &str, partition: u32, payload: Bytes) {
        let entry = self.producers.get(&(topic.to_string(), partition)).or(None);
        if let Some(entry) = entry {
            let list: Vec<_> = entry.iter().enumerate().collect();
            for v in list {
                v.1.value()
                    .send_timeout(payload.clone(), self.timeout)
                    .await;
            }
        }
    }

    /// 订阅 消费者消息
    /// 从 storage 中获取的消息，每个 session 将消息分发至 client 后，会调用 broadcast_consumer_message 将消息广播至该订阅者
    /// 同 module_name 重复订阅会造成之前的 rx 失效
    pub fn subscribe_consumer(
        &self,
        module_name: String,
        topic: &str,
        partition: u32,
    ) -> mpsc::Receiver<Bytes> {
        let key = (topic.to_string(), partition);
        let (tx, rx) = mpsc::channel(self.message_bus_consumer_buffer_size);
        self.consumers.entry(key).and_modify(|v| {
            v.insert(module_name.clone(), tx.clone());
        });

        rx
    }

    /// 取消某个模块订阅 消费者消息
    pub fn unsubscribe_consumer(&self, module_name: &str, topic: &str, partition: u32) {
        let key = (topic.to_string(), partition);
        self.consumers.entry(key).and_modify(|v| {
            v.remove(module_name);
        });
    }

    pub async fn broadcast_consumer_message(&self, topic: &str, partition: u32, payload: Bytes) {
        let entry = self.consumers.get(&(topic.to_string(), partition)).or(None);
        if let Some(entry) = entry {
            let list: Vec<_> = entry.iter().enumerate().collect();
            for v in list {
                v.1.value()
                    .send_timeout(payload.clone(), self.timeout)
                    .await;
            }
        }
    }
}
