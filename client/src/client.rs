use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use grpcx::{
    clientbrokersvc::{
        Message, PublishReq, PublishResp, client_broker_service_client::ClientBrokerServiceClient,
    },
    clientcoosvc::{
        self, AddPartitionsReq, GroupJoinRequest, NewTopicReq, PullReq,
        client_coo_service_client::ClientCooServiceClient, group_join_response,
    },
    commonsvc,
    smart_client::{SmartClient, SmartReqStream, repair_addr_with_http},
    topic_meta::{self, TopicPartitionDetail},
};
use indexmap::IndexMap;
use log::{error, warn};
use murmur3::murmur3_32;
use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    select,
    sync::{Mutex, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

#[derive(Debug, Clone, Default)]
struct PartitionAssignment {
    // 分区 ID -> PartitionInfo
    pub partitions: IndexMap<u32, topic_meta::TopicPartitionDetail>,
    // Key -> 分区 ID（仅 HashKey 策略）
    pub key_to_partition: Arc<DashMap<String, Vec<u32>>>,
    // term 号
    pub term: u64,
    // 用于轮询发送消息
    pub next_partition: Arc<AtomicUsize>,
    pub max_partition: Arc<AtomicUsize>,
}

impl PartitionAssignment {
    // 返回下一个 partition -> broker_addr
    fn next(&self) -> Option<&topic_meta::TopicPartitionDetail> {
        if self.partitions.is_empty() {
            return None;
        }

        if self.next_partition.load(Ordering::Relaxed) > self.max_partition.load(Ordering::Relaxed)
            || self.next_partition.load(Ordering::Relaxed) >= self.partitions.len()
        {
            self.next_partition.store(0, Ordering::Relaxed);
        }

        let part = &self.partitions[self.next_partition.load(Ordering::Relaxed)];
        self.next_partition.fetch_add(1, Ordering::Relaxed);
        return Some(part);
    }

    fn has_key(&self, k: &str) -> bool {
        self.key_to_partition.contains_key(k)
    }

    fn by_key(&self, k: &str) -> Option<&topic_meta::TopicPartitionDetail> {
        if k.is_empty() {
            return self.next();
        }
        if self.partitions.is_empty() || self.key_to_partition.is_empty() || !self.has_key(k) {
            return None;
        }
        let partition_ids = self.key_to_partition.get(k).unwrap();
        for id in partition_ids.value() {
            if let Some(part) = self.partitions.get(id) {
                return Some(part);
            }
        }

        None
    }

    fn hash_key(&self, key: &str) -> Option<&topic_meta::TopicPartitionDetail> {
        let key_binding =
            self.key_to_partition
                .entry(key.to_string())
                .or_insert_with(|| -> Vec<u32> {
                    if let Ok(p) = key_hash_partition(key.to_string(), self.partitions.len() as u32)
                    {
                        return vec![p];
                    }
                    vec![self.next().unwrap().partition_id]
                });
        let pids = key_binding.value();

        for pid in pids {
            if let Some(pi) = self.partitions.get(pid) {
                return Some(pi);
            }
        }
        None
    }
}

fn key_hash_partition(key: String, num_partitions: u32) -> Result<u32> {
    let hash = murmur3_32(&mut Cursor::new(key), 0)?; // 种子为 0，与 Kafka 一致
    Ok((hash % num_partitions) as u32)
}

#[derive(Debug, Clone, Default)]
struct TopicAssignment {
    // Topic -> PartitionAssignment
    assignments: Arc<DashMap<String, PartitionAssignment>>,
}

impl TopicAssignment {
    fn has_topic(&self, topic: &str) -> bool {
        self.assignments.contains_key(topic)
    }

    fn has_topic_key(&self, topic: &str, key: &str) -> bool {
        if !self.has_topic(topic) {
            return false;
        }
        let entry = self.assignments.get(topic).unwrap();
        let part = entry.value();
        part.has_key(key)
    }

    fn apply_topics(&self, tpr: commonsvc::TopicPartitionResp) {
        for tpm in &tpr.list {
            self.assignments
                .entry(tpm.topic.clone())
                .and_modify(|pa| {
                    pa.partitions.insert(
                        tpm.partition_id,
                        topic_meta::TopicPartitionDetail::from(tpm),
                    );
                    pa.max_partition = Arc::new(AtomicUsize::new(pa.partitions.len() as _));
                })
                .or_insert_with(|| -> PartitionAssignment {
                    PartitionAssignment {
                        partitions: IndexMap::from([(
                            tpm.partition_id,
                            topic_meta::TopicPartitionDetail::from(tpm),
                        )]),
                        key_to_partition: Arc::default(),
                        term: 0,
                        next_partition: Arc::default(),
                        max_partition: Arc::new(AtomicUsize::new(1)),
                    }
                });
        }
    }
}

#[derive(Clone)]
pub struct Client {
    topics: TopicAssignment,
    smart_coo_client: SmartClient,
}

impl Client {
    pub fn new(coos: Vec<String>) -> Client {
        let sc: SmartClient = SmartClient::new(coos);

        let client = Self {
            topics: TopicAssignment::default(),
            smart_coo_client: sc,
        };
        let _client = client.clone();
        tokio::spawn(async move {
            _client.run().await;
        });

        client
    }

    pub fn publisher(&self, topic: String) -> Arc<Publisher> {
        Arc::new(Publisher {
            topic,
            client: Arc::new(self.clone()),
            broker_clients: Arc::default(),
            partitions_sender: Arc::default(),
            stop: CancellationToken::new(),
            new_topic_lock: Mutex::default(),
        })
    }

    pub fn subscriber(&self, group_id: u32, topics: Vec<String>) -> Arc<Subscriber> {
        Arc::new(Subscriber {
            group_id,
            topics,
            client: self.clone(),
            member_id: String::new(),
        })
    }

    fn apply_topic_list(&self, tpr: commonsvc::TopicPartitionResp) {
        self.topics.apply_topics(tpr);
    }

    fn get_partition(
        &self,
        topic: &str,
        key: &str,
        auto_map_key: bool, /* 自动映射 key 至 partition */
    ) -> Option<topic_meta::TopicPartitionDetail> {
        if !self.topics.has_topic(topic) {
            return None;
        }
        let topic_assignment = self.topics.assignments.get(topic).unwrap();

        if key.is_empty() {
            return topic_assignment.next().cloned();
        }

        if !topic_assignment.has_key(key) {
            if !auto_map_key {
                return None;
            }
            return topic_assignment.hash_key(key).cloned();
        }
        topic_assignment.by_key(key).cloned()
    }

    fn has_topic(&self, topic: &str) -> bool {
        self.topics.has_topic(topic)
    }

    fn has_topic_key(&self, topic: &str, key: &str) -> bool {
        self.has_topic(topic) && self.topics.has_topic_key(topic, key)
    }

    async fn run(&self) {
        let sc = self.smart_coo_client.clone();
        let refresh_handle = tokio::spawn(async move {
            sc.refresh_coo_endpoints(|chan| async {
                ClientCooServiceClient::new(chan)
                    .list(commonsvc::CooListReq { id: "".to_string() })
                    .await
            })
            .await;
        });

        let sc = self.smart_coo_client.clone();
        let client = self.clone();
        let topic_info_handle = tokio::spawn(async move {
            let c = sc
                .open_sstream(
                    PullReq::default(),
                    |chan, req, addr| async { ClientCooServiceClient::new(chan).pull(req).await },
                    |res| async {
                        client.apply_topic_list(res);
                        Ok(())
                    },
                    CancellationToken::new(),
                )
                .await;
        });

        tokio::try_join!(refresh_handle, topic_info_handle);
    }
}

pub struct Publisher {
    topic: String,
    client: Arc<Client>,
    broker_clients: Arc<DashMap<String, Channel>>,
    partitions_sender: Arc<DashMap<u32, mpsc::Sender<PublishReq>>>,
    stop: CancellationToken,

    new_topic_lock: Mutex<()>,
}

impl Publisher {
    async fn new_topic(&self, topic: &str, partition_num: u32) -> Result<()> {
        self.new_topic_lock.lock().await;
        let resp = self
            .client
            .smart_coo_client
            .execute_unary(
                NewTopicReq {
                    topic: topic.to_string(),
                    partitio_num: partition_num,
                },
                |chan, req, _addr| async { ClientCooServiceClient::new(chan).new_topic(req).await },
            )
            .await?;
        let resp = resp.into_inner();
        if !resp.success {
            return Err(anyhow!(resp.error));
        }
        if let Some(detail) = resp.detail {
            self.client.apply_topic_list(detail);
        }

        Ok(())
    }

    pub async fn add_partition(&self, add_partition_num: u32) -> Result<()> {
        let resp = self
            .client
            .smart_coo_client
            .execute_unary(
                AddPartitionsReq {
                    topic: self.topic.clone(),
                    partitio_num: add_partition_num,
                    key: "".to_string(),
                },
                |chan, req, _addr| async {
                    ClientCooServiceClient::new(chan).add_partitions(req).await
                },
            )
            .await?;
        let resp = resp.into_inner();
        if !resp.success {
            return Err(anyhow!(resp.error));
        }
        self.client.apply_topic_list(resp.detail.unwrap());
        Ok(())
    }

    // pub async fn add_key(&self, topic: &str, key: &str) -> Result<()> {
    //     if self.client.get_partition(topic, key)?.is_some() {
    //         return Err(anyhow!("topic[{topic}]-key[{key}] has been created"));
    //     }
    //     let resp = self
    //         .client
    //         .smart_coo_client
    //         .execute_unary(
    //             AddPartitionsReq {
    //                 topic: topic.to_string(),
    //                 partitio_num: 0,
    //                 key: key.to_string(),
    //             },
    //             |chan, req, _addr| async {
    //                 ClientCooServiceClient::new(chan).add_partitions(req).await
    //             },
    //         )
    //         .await?;
    //     let resp = resp.into_inner();
    //     if !resp.success {
    //         return Err(anyhow!(resp.error));
    //     }
    //     self.client.apply_topic_list(resp.detail.unwrap());
    //     Ok(())
    // }
    pub fn publish<HR, HRFut>(
        self: &Arc<Self>,
        mut rx: mpsc::Receiver<(String, Bytes)>, /* 当 rx 对端的 tx close/drop 时，publisher 的后台 task 结束 */
        handle_resp: HR,
        auto_create: bool,
    ) where
        HR: Fn(Result<PublishResp>) -> HRFut + Send + 'static + Clone,
        HRFut: std::future::Future<Output = ()> + Send,
    {
        let topic = self.topic.clone();
        let publisher = self.clone();

        tokio::spawn(async move {
            while let Some((key, payload)) = rx.recv().await {
                // 获取分区信息
                let partition = match publisher.get_partition_with_retry(&key, auto_create).await {
                    Ok(partition) => partition,
                    Err(e) => {
                        // 错误处理
                        let _ = handle_resp(Err(e)).await;
                        continue;
                    }
                };

                // 获取分区发送器
                let sender = match publisher
                    .get_or_create_partition_sender(&partition, handle_resp.clone())
                    .await
                {
                    Ok(sender) => sender,
                    Err(e) => {
                        error!("get_or_create_partition_sender err: {:?}", e);
                        let _ = handle_resp(Err(e)).await;
                        continue;
                    }
                };

                if let Err(e) = sender
                    .send(PublishReq {
                        topic: topic.clone(),
                        partition: partition.partition_id,
                        payload: payload.to_vec(),
                        message_id: None,
                    })
                    .await
                {
                    error!(
                        "Failed to send message to partition {}: {:?}",
                        partition.partition_id, e
                    );
                    let _ = handle_resp(Err(e.into())).await;
                }
            }
            publisher.stop.cancel();
        });
    }

    // 带重试的分区获取
    async fn get_partition_with_retry(
        self: &Arc<Self>,
        key: &str,
        auto_create: bool,
    ) -> Result<TopicPartitionDetail> {
        // 第一次尝试获取分区
        if let Some(partition) = self.client.get_partition(&self.topic, key, auto_create) {
            return Ok(partition);
        }

        // 如果启用了自动创建但分区不存在
        if auto_create {
            // 检查topic是否存在
            if !self.client.has_topic(&self.topic) {
                // 尝试创建topic
                self.new_topic(&self.topic, 0).await?;
            }

            // 再次尝试获取分区
            return self
                .client
                .get_partition(&self.topic, key, auto_create)
                .ok_or_else(|| anyhow!("Partition not found after auto-create attempt"));
        }

        Err(anyhow!("Partition not found and auto-create is disabled"))
    }

    // 获取或创建分区发送器
    async fn get_or_create_partition_sender<HR, HRFut>(
        self: &Arc<Self>,
        partition: &TopicPartitionDetail,
        handle_resp: HR,
    ) -> Result<mpsc::Sender<PublishReq>>
    where
        HR: Fn(Result<PublishResp>) -> HRFut + Send + 'static + Clone,
        HRFut: std::future::Future<Output = ()> + Send,
    {
        let partition_id: u32 = partition.partition_id;

        // 检查是否已有发送器
        if let Some(sender) = self.partitions_sender.get(&partition_id) {
            return Ok(sender.value().clone());
        }

        // 创建新连接
        let chan = self
            .get_or_create_channel(&partition.broker_leader_addr)
            .await?;

        // 创建通道和流
        let (tx_middleware, rx_middleware) = mpsc::channel(100); // 更大的缓冲区
        let req_stream = SmartReqStream::new(ReceiverStream::new(rx_middleware));
        // 创建gRPC客户端，发送请求并获取响应流
        let mut resp_stream = ClientBrokerServiceClient::new(chan)
            .publish(req_stream)
            .await?
            .into_inner();
        // 存储发送器
        self.partitions_sender
            .insert(partition_id, tx_middleware.clone());

        // 启动响应处理任务
        let partition_id_clone = partition_id;
        let broker_leader_addr = partition.broker_leader_addr.clone();
        let stop = self.stop.clone();
        let publisher = self.clone();
        tokio::spawn(async move {
            let cc = resp_stream.message().await;
            loop {
                select! {
                    resp = resp_stream.message() => {
                        match resp {
                            Ok(resp) => {
                                // 处理响应
                                let result = match resp {
                                    Some(res) => handle_resp(Ok(res)).await,
                                    None => break,
                                };
                            },
                            Err(e) => {
                                error!("receive broker[{broker_leader_addr}] err: {e:?}");
                                break;
                            },
                        }
                    }

                    _ = stop.cancelled() => {
                        warn!("tx peer has been dropped or closed");
                        break;
                    }
                }
            }

            // 当响应流结束时，移除发送器
            publisher.partitions_sender.remove(&partition_id_clone);
        });

        Ok(tx_middleware)
    }

    async fn get_or_create_channel(&self, addr: &str) -> Result<Channel> {
        if let Some(entry) = self.broker_clients.get(addr) {
            return Ok(entry.value().clone());
        }
        let sock = repair_addr_with_http(addr.to_string()).parse()?;
        let chan = Channel::builder(sock).connect().await?;
        self.broker_clients.insert(addr.to_string(), chan.clone());
        Ok(chan)
    }
}

pub struct Subscriber {
    group_id: u32,
    member_id: String,
    topics: Vec<String>,

    client: Client,
}

impl Subscriber {
    pub async fn join(&mut self, capacity: u32) -> Result<()> {
        let resp = self
            .client
            .smart_coo_client
            .execute_unary(
                GroupJoinRequest {
                    group_id: self.group_id,
                    topics: self.topics.clone(),
                    capacity,
                },
                |chan, req, _addr| async {
                    ClientCooServiceClient::new(chan).join_group(req).await
                },
            )
            .await?
            .into_inner();

        match resp.error() {
            group_join_response::ErrorCode::None => {
                self.member_id = resp.member_id;
                Ok(())
            }
            group_join_response::ErrorCode::InvalidTopic => Err(anyhow!(format!(
                "invalid topic, consumer group topics are: {:?}",
                resp.assigned_topics
            ))),
            group_join_response::ErrorCode::SubscriptionConflict => Err(anyhow!(format!(
                "subscription topics has conflict, consumer group topics are: {:?}",
                resp.assigned_topics
            ))),
            group_join_response::ErrorCode::InvalidGeneration => {
                Err(anyhow!(resp.error().as_str_name()))
            }
        }
    }

    pub fn subscribe<HM, HMFut>(&self, hm: HM) -> Result<()>
    where
        HM: Fn() -> HMFut,
        HMFut: std::future::Future<Output = ()>,
    {
        if self.member_id.is_empty() {
            return Err(anyhow!("must join fisrtly"));
        }

        Ok(())
    }
}
