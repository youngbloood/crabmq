use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use grpcx::{
    clientbrokersvc::{
        PublishReq, PublishResp, client_broker_service_client::ClientBrokerServiceClient,
    },
    clientcoosvc::{
        AddPartitionsReq, GroupJoinRequest, NewTopicReq, PullReq,
        client_coo_service_client::ClientCooServiceClient, group_join_response,
    },
    commonsvc::{self},
    smart_client::{SmartClient, SmartReqStream, repair_addr_with_http},
    topic_meta::{self, TopicPartitionDetail},
};
use indexmap::IndexMap;
use log::{error, info, warn};
use murmur3::murmur3_32;
use std::{
    collections::HashSet,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    select,
    sync::{Mutex, RwLock, mpsc},
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
        Some(part)
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

    fn apply_topics(&self, tpds: Vec<topic_meta::TopicPartitionDetail>) {
        for tpd in tpds {
            self.assignments
                .entry(tpd.topic.clone())
                .and_modify(|pa| {
                    pa.partitions.insert(tpd.partition_id, tpd.clone());
                    pa.max_partition = Arc::new(AtomicUsize::new(pa.partitions.len() as _));
                })
                .or_insert_with(|| -> PartitionAssignment {
                    PartitionAssignment {
                        partitions: IndexMap::from([(tpd.partition_id, tpd)]),
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
    new_topic_lock: Arc<Mutex<HashSet<String>>>,
}

impl Client {
    pub fn new(id: String, coos: Vec<String>) -> Client {
        let sc: SmartClient = SmartClient::new(id, coos);

        let client = Self {
            topics: TopicAssignment::default(),
            smart_coo_client: sc,
            new_topic_lock: Arc::default(),
        };
        let _client = client.clone();
        tokio::spawn(async move {
            _client.run().await;
        });

        client
    }

    pub async fn publisher(&self, topic: String) -> Arc<Publisher> {
        let _publisher = Arc::new(Publisher {
            topic: topic.clone(),
            client: Arc::new(self.clone()),
            broker_clients: Arc::default(),
            success_partitions_sender: Arc::default(),
            failed_partitions_sender: Arc::default(),
            stop: CancellationToken::new(),
            topic_created: Arc::default(),
        });

        for v in self.get_topic(&topic) {
            _publisher
                .get_or_create_partition_sender(&v, |_v| async {})
                .await;
        }

        _publisher
    }

    pub fn subscriber(&self, group_id: u32, topics: Vec<String>) -> Arc<Subscriber> {
        Arc::new(Subscriber {
            group_id,
            topics,
            client: self.clone(),
            member_id: String::new(),
        })
    }

    fn apply_topic_partition_resp(&self, tpr: commonsvc::TopicPartitionResp) {
        self.apply_topic_partition_detail(
            tpr.list.iter().map(TopicPartitionDetail::from).collect(),
        );
    }

    fn apply_topic_partition_detail(&self, tpds: Vec<TopicPartitionDetail>) {
        self.topics.apply_topics(tpds);
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
                        client.apply_topic_partition_resp(res);
                        Ok(())
                    },
                    CancellationToken::new(),
                )
                .await;
        });

        tokio::try_join!(refresh_handle, topic_info_handle);
    }

    fn get_topic(&self, topic: &str) -> Vec<TopicPartitionDetail> {
        if !self.has_topic(topic) {
            return vec![];
        }
        let assign = self.topics.assignments.get(topic).unwrap();
        let values = assign.value().partitions.values();
        values.into_iter().cloned().collect()
    }

    async fn new_topic(
        &self,
        topic: &str,
        partition_num: u32,
    ) -> Result<Vec<TopicPartitionDetail>> {
        if self.has_topic(topic) {
            return Ok(self.get_topic(topic));
        }
        let mut wl = self.new_topic_lock.lock().await;
        if wl.get(topic).is_some() {
            return Ok(self.get_topic(topic));
        }
        let resp = self
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
            wl.insert(topic.to_string());
            let tpds: Vec<TopicPartitionDetail> =
                detail.list.iter().map(TopicPartitionDetail::from).collect();
            self.apply_topic_partition_detail(tpds.clone());
            return Ok(tpds);
        }

        Err(anyhow!("not found detail in resp"))
    }
}

pub struct Publisher {
    topic: String,
    client: Arc<Client>,
    broker_clients: Arc<DashMap<String, (mpsc::Sender<PublishReq>, Channel)>>,
    // partition_id -> Sender(to Channel)
    success_partitions_sender: Arc<RwLock<IndexMap<u32, mpsc::Sender<PublishReq>>>>,
    // partition_id -> BrokerEndpointAddress
    failed_partitions_sender: Arc<RwLock<IndexMap<u32, String>>>,
    topic_created: Arc<AtomicBool>,
    stop: CancellationToken,
}

impl Publisher {
    pub async fn new_topic(self: &Arc<Self>, partition_num: u32) -> Result<()> {
        if self
            .topic_created
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let tpds = self
                .client
                .new_topic(&self.topic.clone(), partition_num)
                .await?;

            println!("tpds = {:?}", tpds);
            // println!("tpds = {:?}", tpds);
            for tpd in tpds {
                let _ = self
                    .get_or_create_partition_sender(&tpd, |_v| async {})
                    .await;
            }
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
        if let Some(detail) = resp.detail {
            self.client.apply_topic_partition_detail(
                detail.list.iter().map(TopicPartitionDetail::from).collect(),
            );
        }

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
    pub async fn publish<HR, HRFut>(
        self: &Arc<Self>,
        mut rx: mpsc::Receiver<(String, Bytes)>, /* 当 rx 对端的 tx close/drop 时，publisher 的后台 task 结束 */
        handle_resp: HR,
        auto_create: bool,
    ) -> Result<()>
    where
        HR: Fn(Result<PublishResp>) -> HRFut + Send + 'static + Clone,
        HRFut: std::future::Future<Output = ()> + Send,
    {
        let topic = self.topic.clone();
        let publisher = self.clone();
        if !self.client.has_topic(&self.topic) {
            if !auto_create {
                return Err(anyhow!("not found the topic"));
            }
            self.new_topic(0).await?;
        }

        tokio::spawn(async move {
            let mut index = 0;
            // TODO: 路由 key
            while let Some((key, payload)) = rx.recv().await {
                // 此 loop 是为了找到 !None 的 sender
                loop {
                    let rl = publisher.success_partitions_sender.read().await;
                    if index >= rl.len() {
                        index = 0;
                        continue;
                    }
                    let sender = rl.get_index(index);
                    if sender.is_none() {
                        index += 1;
                        continue;
                    }
                    // 已找到 !None 的 sender
                    let sender = sender.unwrap();
                    let partition_id = sender.0;
                    if let Err(e) = sender
                        .1
                        .send(PublishReq {
                            topic: topic.clone(),
                            partition: *partition_id,
                            payload: payload.to_vec(),
                            message_id: None,
                        })
                        .await
                    {
                        error!(
                            "Failed to send message to partition {}: {:?}",
                            partition_id, e
                        );
                        let _ = handle_resp(Err(e.into())).await;
                    }
                    index += 1;
                    break;
                }
            }
            publisher.stop.cancel();
        });
        Ok(())
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
                self.client.new_topic(&self.topic, 0).await?;
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

        {
            // 检查是否已有发送器
            if let Some(sender) = self
                .success_partitions_sender
                .read()
                .await
                .get(&partition_id)
            {
                return Ok(sender.clone());
            }
        }

        let broker_endpoint_addr = partition.broker_leader_addr.clone();
        // 创建通道和流
        let (tx_middleware, rx_middleware) = mpsc::channel(100);
        // 创建新连接
        let chan = self
            .get_or_create_channel(&broker_endpoint_addr, tx_middleware.clone())
            .await;

        if let Err(e) = chan {
            self.failed_partitions_sender
                .write()
                .await
                .insert(partition_id, broker_endpoint_addr.clone());
            return Err(e);
        }
        let (res_tx_middleware, chan, exist) = chan.unwrap();
        if exist {
            self.success_partitions_sender
                .write()
                .await
                .insert(partition_id, res_tx_middleware.clone());
            return Ok(res_tx_middleware);
        }
        // 链接至 broker 的 Channel 不存在，根据 rx_middleware 建立一个新的发送流
        let req_stream = SmartReqStream::new(ReceiverStream::new(rx_middleware));
        // 创建gRPC客户端，发送请求并获取响应流
        match ClientBrokerServiceClient::new(chan)
            .publish(req_stream)
            .await
        {
            Ok(resp_strm) => {
                // 存储发送器
                self.success_partitions_sender
                    .write()
                    .await
                    .insert(partition_id, tx_middleware.clone());
                self.failed_partitions_sender
                    .write()
                    .await
                    .swap_remove(&partition_id);

                // 启动响应处理任务
                let broker_leader_addr = partition.broker_leader_addr.clone();
                let stop = self.stop.clone();
                let publisher = self.clone();
                tokio::spawn(async move {
                    let mut resp_strm = resp_strm.into_inner();
                    loop {
                        select! {
                            resp = resp_strm.message() => {
                                match resp {
                                    Ok(resp) => {
                                        // 处理响应
                                        match resp {
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
                    publisher
                        .success_partitions_sender
                        .write()
                        .await
                        .swap_remove(&partition_id);
                });
            }
            Err(_) => {
                self.failed_partitions_sender
                    .write()
                    .await
                    .insert(partition_id, broker_endpoint_addr);
            }
        }

        Ok(tx_middleware)
    }

    async fn get_or_create_channel(
        &self,
        addr: &str,
        tx: mpsc::Sender<PublishReq>,
    ) -> Result<(mpsc::Sender<PublishReq>, Channel, bool)> {
        if let Some(entry) = self.broker_clients.get(addr) {
            let _entry = entry.value().clone();
            return Ok((_entry.0, _entry.1, true));
        }
        let sock = repair_addr_with_http(addr.to_string()).parse()?;
        let chan = Channel::builder(sock).connect().await?;
        self.broker_clients
            .insert(addr.to_string(), (tx.clone(), chan.clone()));
        Ok((tx, chan, false))
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
