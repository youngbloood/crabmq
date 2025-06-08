use anyhow::Result;
use dashmap::DashMap;
use grpcx::brokercoosvc::SyncConsumerAssignmentsResp;
use grpcx::clientbrokersvc::{self, Ack, FlowControl, Message};
use grpcx::topic_meta;
use log::error;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storagev2::{StorageReader, StorageReaderSession};
use tokio::sync::{Semaphore, mpsc};
use tokio::{select, time};
use tokio_util::sync::CancellationToken;
use tonic::Status;

#[derive(Clone)]
pub struct ConsumerGroupManager<SR>
where
    SR: StorageReader,
{
    // group_id: Vec<commonsvc::TopicPartitionMeta>
    consumers: Arc<DashMap<String, Vec<topic_meta::TopicPartitionDetail>>>,

    // <ClientAddr, SubSession>
    sessions: Arc<DashMap<String, SubSession>>,

    // <ClientAddr, Instant>
    heartbeats: Arc<DashMap<String, Instant>>,

    subscriber_timeout: u64,

    storage_reader: Arc<SR>,
}

impl<SR> ConsumerGroupManager<SR>
where
    SR: StorageReader,
{
    pub fn new(subscriber_timeout: u64, sr: SR) -> Self {
        let group = Self {
            consumers: Arc::default(),
            sessions: Arc::new(DashMap::new()),
            heartbeats: Arc::new(DashMap::new()),
            subscriber_timeout,
            storage_reader: Arc::new(sr),
        };

        // 启动心跳检测任务
        // tokio::spawn(Self::heartbeat_checker(group.clone()));
        group
    }

    pub async fn apply_consumergroup(&self, res: SyncConsumerAssignmentsResp) {
        res.list.iter().map(|v| {
            let tm = topic_meta::TopicPartitionDetail::from(v.clone());
            for member_id in &tm.sub_member_ids {
                self.consumers
                    .entry(member_id.to_string())
                    .and_modify(|entry: &mut Vec<topic_meta::TopicPartitionDetail>| {
                        entry.push(tm.clone());
                    })
                    .or_insert(vec![tm.clone()]);
            }
        });
    }

    pub async fn check_consumer(
        &self,
        // group_id: u32,
        member_id: &str,
        topic: &str,
        partition_id: u32,
    ) -> bool {
        if !self.consumers.contains_key(member_id) {
            return false;
        }
        for pm in self.consumers.get(member_id).unwrap().iter() {
            if pm.topic == topic && pm.partition_id == partition_id {
                return true;
            }
        }
        false
    }

    pub async fn new_sesssion(
        &self,
        group_id: u32,
        sub_topics: Vec<clientbrokersvc::subscription::SubTopic>,
        client_addr: String,
        tx: mpsc::Sender<Result<Message, Status>>,
    ) -> Result<()> {
        let srs = self.storage_reader.new_session(group_id).await?;
        let ss = SubSession::new(group_id, sub_topics, client_addr.clone(), srs, tx);
        self.sessions.insert(client_addr, ss);
        Ok(())
    }

    pub fn get_session(&self, client_addr: &str) -> Option<SubSession> {
        if let Some(entry) = self.sessions.get(client_addr) {
            return Some(entry.value().clone());
        }
        None
    }

    async fn heartbeat_checker(group: Self) {
        let mut interval = time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let now = Instant::now();

            // 清理过期消费者
            group
                .heartbeats
                .retain(|_, ts| now - *ts < Duration::from_secs(group.subscriber_timeout));

            // 清理无心跳的分配
            // group.assignments.retain(|_, v| {
            //     if let Some(sess) = group.get_session(v) {
            //         group.sessions.remove(v);
            //         sess.close();
            //     }
            //     group.heartbeats.contains_key(v)
            // });
        }
    }
}

// SubSession 从 Storage 中获取消息
// 流量控制等
#[derive(Clone)]
pub struct SubSession {
    group_id: u32,
    sub_topics: Arc<Vec<clientbrokersvc::subscription::SubTopic>>,
    // 以链接至 broker 的客户端地址作为 session_id
    client_addr: String,

    window: Arc<Semaphore>,
    storage: Arc<Box<dyn StorageReaderSession>>,
    stop_signal: CancellationToken,
}

impl SubSession {
    pub fn new(
        group_id: u32,
        sub_topics: Vec<clientbrokersvc::subscription::SubTopic>,
        client_addr: String,
        storage: Box<dyn StorageReaderSession>,
        tx: mpsc::Sender<Result<Message, Status>>,
    ) -> Self {
        let (tx_storage, mut rx_storage) = mpsc::channel(1024);
        let ss = Self {
            group_id,
            sub_topics: Arc::new(sub_topics),
            client_addr,

            window: Arc::new(Semaphore::new(10)),
            storage: Arc::new(storage),
            stop_signal: CancellationToken::new(),
        };

        let stop = ss.stop_signal.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop.cancelled() => {
                        tx.send(Err(tonic::Status::cancelled("cancelled"))).await;
                        return;
                    }

                    msg = rx_storage.recv() => {
                        match msg {
                            Some(msg) => {
                                let _ = tx.send(Ok(msg)).await;
                            }
                            None => {
                                tx.send(Err(tonic::Status::data_loss("get none from tx_storage"))).await;
                            },
                        }
                    }
                }
            }
        });

        for st in ss.sub_topics.iter() {
            let reader = ss.storage.clone();
            let topic = st.topic.clone();
            let partition_id = st.partition.clone();
            let offset = st.offset;
            let _tx = tx_storage.clone();
            let stop = ss.stop_signal.clone();
            tokio::spawn(async move {
                loop {
                    match reader.next(&topic, partition_id, stop.clone()).await {
                        Ok(msg) => {
                            _tx.send(Message {
                                message_id: String::new(),
                                topic: topic.clone(),
                                partition: partition_id,
                                offset: 0,
                                payload: msg.to_vec(),
                                metadata: HashMap::new(),
                                ..Default::default()
                            })
                            .await;
                        }
                        Err(e) => {
                            error!(
                                "get the topic[{topic}]-partition[{partition_id}] next failed: {e:?}"
                            );
                            break;
                        }
                    }
                }
            });
        }

        ss
    }

    fn close(&self) {
        self.stop_signal.cancel();
    }

    pub fn handle_ack(&self, ack: Ack) {}

    pub fn handle_flow(&self, flow: FlowControl) {}

    // pub async fn next(&mut self) -> Result<Message, Status> {
    //     select! {
    //         _ = self.stop_signal.cancelled() => {
    //             return Err(tonic::Status::new(
    //                 tonic::Code::DeadlineExceeded,
    //                 "Client timeout",
    //             ));
    //         }

    //         msg = self.rx.recv() => {
    //             match msg {
    //                 Some(msg) => {
    //                     return Ok(msg);
    //                 }
    //                 None => {
    //                     return Err(tonic::Status::new(
    //                     tonic::Code::DeadlineExceeded,
    //                     "tx closed",
    //                     ));
    //                 },
    //             }
    //         }
    //     }
    // }
}
