use dashmap::DashMap;
use grpcx::clientbrokersvc::{Ack, FlowControl, Message};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use storagev2::Storage;
use tokio::sync::Semaphore;
use tokio::time;
use tonic::Status;

#[derive(Debug, Clone)]
pub struct ConsumerGroup<T>
where
    T: Storage,
{
    // <(Topic, Partition), ClientAddr>
    assignments: Arc<DashMap<(String, u32), String>>,

    // <ClientAddr, SubSession>
    sessions: Arc<DashMap<String, SubSession<T>>>,

    // <ClientAddr, Instant>
    heartbeats: Arc<DashMap<String, Instant>>,
}

impl<T> ConsumerGroup<T>
where
    T: Storage,
{
    pub fn new() -> Self {
        let group = Self {
            assignments: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            heartbeats: Arc::new(DashMap::new()),
        };

        // 启动心跳检测任务
        tokio::spawn(Self::heartbeat_checker(group.clone()));
        group
    }

    pub async fn acquire_partition(
        &self,
        topic: &str,
        partition: u32,
        sess: SubSession<T>,
    ) -> anyhow::Result<()> {
        let key = (topic.to_string(), partition);
        if let Some(owner) = self.assignments.get(&key) {
            if owner.eq(&sess.client_addr) {
                return Err(anyhow::anyhow!("Partition already occupied"));
            }
        }

        let client_addr = sess.client_addr.clone();
        self.assignments.insert(key, client_addr.clone());
        self.sessions.insert(client_addr.clone(), sess);
        self.heartbeats.insert(client_addr, Instant::now());
        Ok(())
    }

    pub fn has_subscription(&self, topic: &str, partition: u32) -> bool {
        self.assignments
            .contains_key(&(topic.to_string(), partition))
    }

    pub fn get_session(&self, client_addr: &str) -> Option<SubSession<T>> {
        self.sessions
            .get(client_addr)
            .map(|entry| entry.value().clone())
    }

    async fn heartbeat_checker(group: Self) {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let now = Instant::now();

            // 清理过期消费者
            group
                .heartbeats
                .retain(|_, ts| now - *ts < Duration::from_secs(30));

            // 清理无心跳的分配
            group.assignments.retain(|_, v| {
                if let Some(sess) = group.get_session(v) {
                    group.sessions.remove(v);
                    sess.close();
                }
                group.heartbeats.contains_key(v)
            });
        }
    }
}

// TODO: 轮训从 Storage 中获取 message
#[derive(Debug, Clone)]
pub struct SubSession<T: Storage> {
    topic: String,
    partition: u32,
    // 以链接至 broker 的客户端地址作为 session_id
    client_addr: String,
    offset: Arc<AtomicI64>,
    window: Arc<Semaphore>,
    storage: T,
    closed: Arc<AtomicBool>,
}

impl<T: Storage> SubSession<T> {
    pub fn new(topic: String, partition: u32, client_addr: String, storage: T) -> Self {
        Self {
            topic,
            partition,
            client_addr,
            offset: Arc::new(AtomicI64::new(0)),
            window: Arc::new(Semaphore::new(10)),
            storage,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }

    pub fn handle_ack(&self, ack: Ack) {}

    pub fn handle_flow(&self, flow: FlowControl) {}

    pub async fn next(&self) -> Result<Message, Status> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(tonic::Status::new(
                tonic::Code::DeadlineExceeded,
                "Client timeout",
            ));
        }

        let _permit = self.window.acquire().await;

        let offset = self.offset.load(Ordering::SeqCst);
        let data = self
            .storage
            .next(&self.topic, self.partition)
            .await
            .map_err(|_| super::BrokerError::StorageFailure)?;

        self.offset.fetch_add(1, Ordering::SeqCst);

        Ok(Message {
            message_id: "".into(),
            topic: self.topic.clone(),
            partition: self.partition,
            offset,
            payload: data.to_vec(),
            metadata: Default::default(),
        })
    }
}
