use anyhow::{Result, anyhow};
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use grpcx::{
    clientbrokersvc::{
        self, Ack, Commit, Fetch, Heartbeat, MessageBatch, PublishReq, PublishResp, SegmentOffset,
        SubscribeReq, Subscription, client_broker_service_client::ClientBrokerServiceClient,
        subscribe_req::Request,
    },
    clientcoosvc::{
        AddPartitionsReq, GroupJoinRequest, GroupJoinSubTopic, NewTopicReq, PullReq,
        SyncConsumerAssignmentsReq, client_coo_service_client::ClientCooServiceClient,
        group_join_response,
    },
    commonsvc::{self},
    smart_client::{SmartClient, SmartReqStream, repair_addr_with_http},
    topic_meta::{self, TopicPartitionDetail},
};
use indexmap::IndexMap;
use log::{error, info, warn};
use murmur3::murmur3_32;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{Mutex, RwLock, mpsc},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Status, transport::Channel};

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
    id: String,
    topics: TopicAssignment,
    smart_coo_client: SmartClient,
    new_topic_lock: Arc<Mutex<HashSet<String>>>,
}

impl Client {
    pub fn new(id: String, coos: Vec<String>) -> Client {
        let sc: SmartClient = SmartClient::new(id.clone(), coos);

        let client = Self {
            id,
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

    pub fn subscriber(&self, conf: SubscriberConfig) -> Result<Arc<Subscriber>> {
        conf.validate()?;
        Ok(Arc::new(Subscriber {
            conf: Arc::new(conf),
            client: self.clone(),
            member_id: Arc::default(),
            stop: CancellationToken::new(),
            // iter_count: Arc::new(AtomicU64::new(1)),
            chans: Arc::default(),
            broker_endpoints: Arc::default(),
            subscribe_controll_manager: Arc::default(),
        }))
    }

    pub fn subscriber_with_default(
        &self,
        group_id: u32,
        topics: Vec<String>,
    ) -> Result<Arc<Subscriber>> {
        let conf = with_subscriber_config(group_id, topics);
        Ok(Arc::new(Subscriber {
            conf: Arc::new(conf),
            client: self.clone(),
            member_id: Arc::default(),
            stop: CancellationToken::new(),
            // iter_count: Arc::new(AtomicU64::new(1)),
            chans: Arc::default(),
            broker_endpoints: Arc::default(),
            subscribe_controll_manager: Arc::default(),
        }))
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
        let id = self.id.clone();
        let refresh_handle = tokio::spawn(async move {
            sc.refresh_coo_endpoints(|chan| {
                let id = id.clone();
                async move {
                    ClientCooServiceClient::new(chan)
                        .list(commonsvc::CooListReq { id })
                        .await
                }
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
        let (tx_middleware, rx_middleware) = mpsc::channel(1);
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

#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    pub group_id: u32,
    pub topics: Vec<(String, u32)>,
    // 某些 broker 连不上时是否退出
    pub break_when_broker_disconnect: bool,
    /// 流量控制
    // 每个分区单次获取消息的最大bytes数
    // 为 0 时以 max_all_fetch_bytes/{分区数} 为准
    // 同 `max_partition_fetch_count` 满足其一即返回
    pub max_partition_fetch_bytes: Arc<AtomicU64>,

    // 每个分区单批次获取最大消息数量
    // 同 `max_partition_fetch_bytes` 满足其一即返回
    pub max_partition_batch_count: Arc<AtomicU64>,

    // 所有分区获取消息的最大bytes数
    // 为 0 时以 max_partition_fetch_bytes * {分区数} 为准
    pub max_all_fetch_bytes: Arc<AtomicU64>,

    // 每个分区的 left_fetch_bytes 大于该值时，也会进行 fetch
    pub min_fetch_size: Arc<AtomicU64>,

    /// 频率控制
    // 当服务端返回某个分区 fetch 的消息为空时，下次执行 fetch 的间隔，单位: s
    pub next_fetch_interval_when_empty: Arc<AtomicU64>,
}

pub fn with_subscriber_config(group_id: u32, topics: Vec<String>) -> SubscriberConfig {
    SubscriberConfig {
        group_id,
        topics: topics.iter().map(|v| (v.clone(), 0_u32)).collect(),
        break_when_broker_disconnect: false,
        max_partition_fetch_bytes: Arc::new(AtomicU64::new(10000)),
        max_partition_batch_count: Arc::new(AtomicU64::new(10000)),
        max_all_fetch_bytes: Arc::new(AtomicU64::new(100000)),
        min_fetch_size: Arc::new(AtomicU64::new(100)),
        next_fetch_interval_when_empty: Arc::new(AtomicU64::new(10)),
    }
}

impl SubscriberConfig {
    fn validate(&self) -> Result<()> {
        for topic in &self.topics {
            if topic.1 != 0 && topic.1 != 1 {
                return Err(anyhow!(format!(
                    "topic[{}] offset must be one of [0, 1]",
                    topic.0
                )));
            }
        }

        if self
            .max_partition_fetch_bytes
            .fetch_max(0, Ordering::Relaxed)
            == 0
        {
            return Err(anyhow!(
                "max_partition_fetch_bytes must be greate than zero",
            ));
        }

        if self
            .max_partition_batch_count
            .fetch_max(0, Ordering::Relaxed)
            == 0
        {
            return Err(anyhow!(
                "max_partition_fetch_count must be greate than zero",
            ));
        }

        Ok(())
    }

    fn re_calc(&self, partition_num: u64) {
        if partition_num == 0 {
            return;
        }
        let bytes_per_partition = self.max_all_fetch_bytes.load(Ordering::Relaxed) / partition_num;
        if bytes_per_partition == 0 {
            return;
        }
        self.max_partition_fetch_bytes.store(
            self.max_partition_fetch_bytes
                .load(Ordering::Relaxed)
                .min(bytes_per_partition),
            Ordering::Relaxed,
        );
    }
}

#[derive(Clone)]
pub struct Subscriber {
    conf: Arc<SubscriberConfig>,
    member_id: Arc<RefCell<String>>,
    client: Client,
    // iter_count: Arc<AtomicU64>,
    // 该 member 消费的 (broker_addr, broker_id) : Vec<SubscriberPartitionBuffer>
    chans: Arc<DashMap<(String, u32), Arc<Vec<SubscriberPartitionBuffer>>>>,
    // 订阅时的消息控制器，可用于发送 ack, commit, heartbeat 等消息
    // (broker_addr, broker_id): mpsc::Sender<SubscribeReq>
    subscribe_controll_manager: Arc<DashMap<(String, u32), mpsc::Sender<SubscribeReq>>>,
    broker_endpoints: Arc<DashMap<String, Channel>>,
    stop: CancellationToken,
}

// 每个分区单独 fetch 消息至自己独立的 buffer 中，等待消费
#[derive(Clone, Debug)]
struct SubscriberPartitionBuffer {
    conf: Arc<SubscriberConfig>,
    tpd: Arc<TopicPartitionDetail>,
    buf: Arc<SegQueue<MessageBatch>>,
    // iter_count: Arc<AtomicU64>,
    // 剩余可以拉去的bytes数
    left_fetch_bytes: Arc<AtomicU64>,
    next_fetch_after: Arc<AtomicU64>,
}

impl SubscriberPartitionBuffer {
    fn new(tpd: TopicPartitionDetail, conf: Arc<SubscriberConfig>) -> Self {
        Self {
            tpd: Arc::new(tpd),
            buf: Arc::default(),
            // iter_count: Arc::default(),
            left_fetch_bytes: Arc::new(AtomicU64::new(
                conf.max_partition_fetch_bytes.load(Ordering::Relaxed),
            )),
            conf,
            next_fetch_after: Arc::default(),
        }
    }

    fn push(&self, msg: MessageBatch) {
        if msg.messages.is_empty() {
            self.next_fetch_after.store(
                chrono::Local::now().timestamp() as u64
                    + self
                        .conf
                        .next_fetch_interval_when_empty
                        .load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            return;
        }
        let total_size = msg.messages.iter().map(|v| v.payload.len()).sum::<usize>() as u64;
        if total_size == 0 {
            self.next_fetch_after.store(
                chrono::Local::now().timestamp() as u64
                    + self
                        .conf
                        .next_fetch_interval_when_empty
                        .load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            return;
        }
        self.buf.push(msg);
        if self
            .left_fetch_bytes
            .fetch_min(total_size, Ordering::Relaxed)
            == total_size
        {
            self.left_fetch_bytes
                .fetch_sub(total_size, Ordering::Relaxed);
        } else {
            self.left_fetch_bytes.store(0, Ordering::Relaxed);
        }
    }
}

unsafe impl Send for Subscriber {}
unsafe impl Sync for Subscriber {}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Subscriber {
    pub fn close(self: Arc<Self>) {
        self.stop.cancel();
    }

    pub async fn join(self: Arc<Self>) -> Result<()> {
        if !self.member_id.borrow().is_empty() {
            return Err(anyhow!("this subscriber has been joined"));
        }
        let resp = self
            .client
            .smart_coo_client
            .execute_unary(
                GroupJoinRequest {
                    group_id: self.conf.group_id,
                    sub_topics: self
                        .conf
                        .topics
                        .iter()
                        .map(|v| GroupJoinSubTopic {
                            topic: v.0.clone(),
                            offset: v.1,
                        })
                        .collect(),
                },
                |chan, req, _addr| async {
                    ClientCooServiceClient::new(chan).join_group(req).await
                },
            )
            .await?
            .into_inner();

        match resp.error() {
            group_join_response::ErrorCode::None => {
                println!("join resp = {:?}", &resp);
                *self.member_id.borrow_mut() = resp.member_id;

                Self::apply_consumer_assignments(self.chans.clone(), &resp.list, self.conf.clone());
                println!("self.chans = {:?}", self.chans);

                let sub = self.clone();
                tokio::spawn(sub.sync_consumer_assignments());
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

    async fn sync_consumer_assignments(self: Arc<Self>) {
        let stop = self.stop.clone();
        let member_id = self.member_id.borrow().clone();
        let group_id = self.conf.group_id;

        if let Err(e) = self
            .client
            .smart_coo_client
            .open_sstream(
                SyncConsumerAssignmentsReq {
                    group_id,
                    member_id: member_id.clone(),
                    generation_id: 0,
                },
                |chan, req, _addr| async {
                    ClientCooServiceClient::new(chan)
                        .sync_consumer_assignments(req)
                        .await
                },
                |res| {
                    let member_id = member_id.clone();
                    let chans = self.chans.clone();
                    let conf = self.conf.clone();
                    async move {
                        if !&res.member_id.eq(&member_id) || !&res.group_id.eq(&group_id) {
                            warn!("this res not belong the client");
                            return Err(Status::invalid_argument("this res not belong the client"));
                        }
                        Self::apply_consumer_assignments(chans, &res.list, conf);

                        Ok(())
                    }
                },
                stop,
            )
            .await
        {
            error!("subscriber sync consumer assignments err: {e:?}");
        }
    }

    fn apply_consumer_assignments(
        chans: Arc<DashMap<(String, u32), Arc<Vec<SubscriberPartitionBuffer>>>>,
        tpms: &Vec<commonsvc::TopicPartitionMeta>,
        conf: Arc<SubscriberConfig>,
    ) {
        let mut m = HashMap::new();

        for tpm in tpms.iter() {
            let tpd = TopicPartitionDetail::from(tpm);
            let key = (tpd.broker_leader_addr.clone(), tpd.broker_leader_id);
            let spb = SubscriberPartitionBuffer::new(tpd, conf.clone());

            m.entry(key)
                .and_modify(|v: &mut Vec<SubscriberPartitionBuffer>| {
                    v.push(spb.clone());
                })
                .or_insert(vec![spb.clone()]);
        }

        for (k, v) in m {
            chans.insert(k, Arc::new(v));
        }
    }

    async fn get_or_create_broker_endpoint(&self, broker_endpoint: String) -> Result<Channel> {
        let sock = repair_addr_with_http(broker_endpoint.clone()).parse()?;
        let chan = Channel::builder(sock).connect().await?;
        self.broker_endpoints.insert(broker_endpoint, chan.clone());
        Ok(chan)
    }

    pub fn subscribe<HM, HMFut>(self: Arc<Self>, hm: HM) -> Result<()>
    where
        HM: Fn((String, u32), clientbrokersvc::MessageBatch, AckCommitHandle) -> HMFut
            + Send
            + Sync
            + 'static,
        HMFut: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.member_id.borrow().is_empty() {
            return Err(anyhow!("must join fisrtly"));
        }

        // 1. 启动分区拉取任务
        let subscriber = self.clone();
        tokio::spawn(async move {
            subscriber.spawn_fetch_tasks().await;
        });

        // 2. 启动消费任务
        let subscriber = self.clone();
        tokio::spawn(async move {
            subscriber.spawn_consumer_task(hm);
        });

        // 3. 启动心跳任务
        let subscriber = self.clone();
        tokio::spawn(async move {
            subscriber.spawn_heartbeat_task().await;
        });

        Ok(())
    }

    async fn spawn_fetch_tasks(self: Arc<Self>) {
        // 根据 broker 进行分组，每个 partition 向其所属的 broker 获取消息
        // 开启 task 数量 = self.chans.len() * 2
        let has_monitor = Arc::new(DashSet::new());
        while !self.stop.is_cancelled() {
            let partition_num = self.chans.iter().map(|v| v.value().len()).sum::<usize>() as u64;
            self.conf.re_calc(partition_num);
            for entry in self.chans.iter() {
                let key = entry.key().clone();
                if has_monitor.contains(&key) {
                    time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
                has_monitor.insert(key.clone());
                let subscriber = self.clone();
                let chans = self.chans.clone();
                let memeber_id = self.member_id.borrow().clone();
                let stop = self.stop.clone();
                let break_when_broker_disconnect = self.conf.break_when_broker_disconnect;
                let has_monitor = has_monitor.clone();
                tokio::spawn(async move {
                    let broker_addr = key.0.clone();
                    let broker_id = key.1;
                    let broker_chan = subscriber
                        .get_or_create_broker_endpoint(broker_addr.clone())
                        .await;
                    if let Err(e) = broker_chan {
                        error!("Failed to connect to broker {}: {}", broker_addr, e);
                        has_monitor.remove(&key);
                        return;
                    }
                    let broker_chan = broker_chan.unwrap();
                    let mut client = ClientBrokerServiceClient::new(broker_chan);
                    let (tx_req, rx_req) = mpsc::channel(10);
                    let _ = tx_req
                        .send(SubscribeReq {
                            request: Some(clientbrokersvc::subscribe_req::Request::Sub(
                                Subscription {
                                    member_id: memeber_id,
                                },
                            )),
                        })
                        .await;
                    subscriber
                        .subscribe_controll_manager
                        .insert(key.clone(), tx_req.clone());

                    let _chans = chans.clone();
                    let _has_monitor = has_monitor.clone();
                    let _broker_addr = broker_addr.clone();
                    let _key = key.clone();
                    let _stop = stop.clone();
                    let _break_when_broker_disconnect = break_when_broker_disconnect;
                    tokio::spawn(async move {
                        let resp = client
                            .subscribe(SmartReqStream::new(ReceiverStream::new(rx_req)))
                            .await;
                        println!("订阅成功？ resp = {:?}", resp);
                        match resp {
                            Ok(resp) => {
                                let mut resp_strm = resp.into_inner();
                                while let Ok(Some(res)) = resp_strm.message().await {
                                    if res.messages.is_empty() {
                                        continue;
                                    }
                                    if let Some(spbs) = _chans.get(&_key) {
                                        let spbs = spbs.value();
                                        for spb in spbs.iter() {
                                            if spb.tpd.topic == res.topic
                                                && spb.tpd.partition_id == res.partition_id
                                            {
                                                spb.push(res);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                _has_monitor.remove(&_key);
                                error!(
                                    "subscribe from broker[{}:{:?}] err: {e:?}",
                                    broker_id, &_broker_addr,
                                );
                                if _break_when_broker_disconnect {
                                    _stop.cancel();
                                }
                            }
                        }
                    });

                    // fetch 消息
                    loop {
                        if stop.is_cancelled() {
                            break;
                        }

                        let spbs = chans.get(&key);
                        if spbs.is_none() {
                            has_monitor.remove(&key);
                            break;
                        }
                        let spbs = spbs.unwrap();
                        for spb in spbs.value().iter().filter(|v| {
                            let now = chrono::Local::now().timestamp() as u64;
                            v.next_fetch_after.fetch_max(0, Ordering::Relaxed) == 0
                                || v.next_fetch_after.fetch_max(now, Ordering::Relaxed) == now
                        }) {
                            if stop.is_cancelled() {
                                break;
                            }
                            println!(
                                "循环fetch消息:left_fetch_bytes: {}, max_partition_fetch_bytes: {} ",
                                spb.left_fetch_bytes.load(Ordering::Relaxed),
                                spb.conf.max_partition_fetch_bytes.load(Ordering::Relaxed)
                            );

                            let left_bytes = spb.left_fetch_bytes.load(Ordering::Relaxed);
                            let max_bytes =
                                spb.conf.max_partition_fetch_bytes.load(Ordering::Relaxed);
                            // min_fetch_size <= left_bytes <= max_bytes 时，才进行 fetch
                            if left_bytes < spb.conf.min_fetch_size.load(Ordering::Relaxed)
                                || left_bytes > max_bytes
                            {
                                warn!(
                                    "skip fetch: broker[{}:{:?}] topic-partition[{}-{}], left_bytes = {}, max_bytes = {}",
                                    broker_id,
                                    &broker_addr,
                                    &spb.tpd.topic,
                                    spb.tpd.partition_id,
                                    left_bytes,
                                    max_bytes
                                );
                                continue;
                            }
                            info!(
                                "fetch broker[{}:{:?}] topic-partition[{}-{}] message: left_fetch_bytes: {}, max_partition_batch_count: {}",
                                broker_id,
                                &broker_addr,
                                &spb.tpd.topic,
                                spb.tpd.partition_id,
                                left_bytes,
                                spb.conf.max_partition_batch_count.load(Ordering::Relaxed),
                            );
                            // 发送fetch获取消息
                            if let Err(e) = tx_req
                                .send(SubscribeReq {
                                    request: Some(clientbrokersvc::subscribe_req::Request::Fetch(
                                        Fetch {
                                            topic: spb.tpd.topic.clone(),
                                            partition_id: spb.tpd.partition_id,
                                            max_partition_bytes: spb
                                                .left_fetch_bytes
                                                .load(Ordering::Relaxed),
                                            max_partition_batch_count: spb
                                                .conf
                                                .max_partition_batch_count
                                                .load(Ordering::Relaxed),
                                        },
                                    )),
                                })
                                .await
                            {
                                error!(
                                    "fetch [{}-{}] from broker[{}:{:?}] send err: {e:?}",
                                    &spb.tpd.topic, spb.tpd.partition_id, broker_id, &broker_addr,
                                );
                                if break_when_broker_disconnect {
                                    stop.cancel();
                                }
                            }
                        }
                        time::sleep(Duration::from_millis(30)).await;
                    }
                });
            }
        }
    }

    fn spawn_consumer_task<HM, HMFut>(&self, handler: HM)
    where
        HM: Fn((String, u32), clientbrokersvc::MessageBatch, AckCommitHandle) -> HMFut
            + Send
            + Sync
            + 'static,
        HMFut: std::future::Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let ack_commit_manager = self.subscribe_controll_manager.clone();
        let stop = self.stop.clone();
        let chans = self.chans.clone();

        tokio::spawn(async move {
            // 轮询所有分区的缓冲区
            while !stop.is_cancelled() {
                let mut processed = false;

                for entry in chans.iter() {
                    let broker_info = entry.key();
                    let (_, spbs) = entry.pair();
                    for spb in spbs.iter() {
                        if let Some(messages) = spb.buf.pop() {
                            if messages.messages.is_empty() {
                                continue;
                            }
                            let key =
                                (spb.tpd.broker_leader_addr.clone(), spb.tpd.broker_leader_id);
                            let message_size = messages
                                .messages
                                .iter()
                                .map(|v| v.payload.len())
                                .sum::<usize>()
                                as u64;
                            if message_size == 0 {
                                continue;
                            }
                            processed = true;
                            // 创建ACK COMMIT句柄
                            let ack_handle = AckCommitHandle {
                                // topic: messages.topic.clone(),
                                // partition: messages.partition_id,
                                batch_id: messages.batch_id.clone(),
                                batch_message_size: message_size,
                                spb: spb.clone(),
                                last_offset: messages
                                    .messages
                                    .last()
                                    .unwrap()
                                    .offset
                                    .clone()
                                    .unwrap(),
                                notify: Arc::new(
                                    ack_commit_manager.get(&key).unwrap().value().clone(),
                                ),
                            };

                            // 调用用户处理函数
                            handler(broker_info.clone(), messages, ack_handle).await;
                        }
                    }
                }

                // 如果没有处理任何消息，短暂休眠避免CPU空转
                if !processed {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });
    }

    async fn spawn_heartbeat_task(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        while !self.stop.is_cancelled() {
            interval.tick().await;

            for entry in self.subscribe_controll_manager.iter() {
                let key = entry.key();
                let tx = entry.value().clone();
                if let Err(e) = tx
                    .send(SubscribeReq {
                        request: Some(Request::Heartbeat(Heartbeat { credit_request: 0 })),
                    })
                    .await
                {
                    error!(
                        "[Subscriber]: Heartbeat to broker[{}:{}] err: {e:?}",
                        key.1, key.0
                    );
                    if self.conf.break_when_broker_disconnect {
                        self.stop.cancel();
                    }
                }
            }
        }
    }
}

pub struct AckCommitHandle {
    // topic: String,
    // partition: u32,
    // 用于批次 ack
    batch_id: String,
    // 该批次消息大小
    batch_message_size: u64,
    // 用于commit
    last_offset: SegmentOffset,
    spb: SubscriberPartitionBuffer,
    notify: Arc<mpsc::Sender<SubscribeReq>>,
}

impl AckCommitHandle {
    pub async fn ack(self) -> Result<()> {
        self.notify
            .send(SubscribeReq {
                request: Some(clientbrokersvc::subscribe_req::Request::Ack(Ack {
                    batch_id: self.batch_id,
                })),
            })
            .await?;

        self.spb
            .left_fetch_bytes
            .fetch_add(self.batch_message_size, Ordering::Relaxed);

        println!(
            "调用 ack 并返回 bytes = {:?}, left_fetch_bytes = {:?}",
            self.batch_message_size,
            self.spb.left_fetch_bytes.load(Ordering::Relaxed)
        );
        Ok(())
    }

    pub fn nack(self) {
        // 如果需要重新投递，可以实现重新入队逻辑
        // 当前设计不提供NACK功能，需要用户自己处理失败
        // self.spb.push(msg);
    }

    pub async fn commit(self) -> Result<()> {
        self.notify
            .send(SubscribeReq {
                request: Some(clientbrokersvc::subscribe_req::Request::Commit(Commit {
                    commit_pos: Some(self.last_offset),
                })),
            })
            .await?;
        Ok(())
    }
}
