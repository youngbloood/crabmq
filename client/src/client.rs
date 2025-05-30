use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use grpcx::{
    clientbrokersvc::{
        PublishReq, PublishResp, client_broker_service_client::ClientBrokerServiceClient,
    },
    clientcoosvc::{
        AddPartitionsReq, NewTopicReq, PullReq, client_coo_service_client::ClientCooServiceClient,
    },
    commonsvc,
    smart_client::{SmartClient, SmartReqStream, repair_addr_with_http},
    topic_meta,
};
use indexmap::IndexMap;
use log::error;
use murmur3::murmur3_32;
use std::{
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{select, sync::mpsc};
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

    fn apply_topic_list(&self, tpr: commonsvc::TopicPartitionResp) {
        self.topics.apply_topics(tpr);
    }

    fn get_partition(
        &self,
        topic: &str,
        key: &str,
        auto_map: bool,
    ) -> Result<Option<topic_meta::TopicPartitionDetail>> {
        if !self.topics.has_topic(topic) {
            return Err(anyhow!("not found the topic"));
        }
        let topic_assignment = self.topics.assignments.get(topic).unwrap();
        if key.is_empty() {
            return Ok(topic_assignment.next().cloned());
        }

        if !topic_assignment.has_key(key) {
            if !auto_map {
                return Err(anyhow!("not found the topic-partition"));
            }
            return Ok(topic_assignment.hash_key(key).cloned());
        }
        if key.is_empty() {
            Ok(topic_assignment.next().cloned())
        } else {
            Ok(topic_assignment.by_key(key).cloned())
        }
    }

    fn has_topic(&self, topic: &str) -> bool {
        self.topics.has_topic(topic)
    }

    fn has_topic_key(&self, topic: &str, key: &str) -> bool {
        self.has_topic(topic) && self.topics.has_topic_key(topic, key)
    }

    pub fn publisher(&self, topic: String) -> Publisher {
        Publisher {
            topic,
            client: self,
            broker_clients: Arc::default(),
        }
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

pub struct Publisher<'a> {
    topic: String,
    client: &'a Client,
    broker_clients: Arc<DashMap<String, Channel>>,
}

impl Publisher<'_> {
    async fn new_topic(&self, topic: &str, partition_num: u32) -> Result<()> {
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
        self.client.apply_topic_list(resp.detail.unwrap());
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

    pub async fn publish<HR, HRFut>(
        &self,
        mut rx: mpsc::Receiver<(String, Bytes)>, /* payload 接收器，tx 端关闭即可停止向 broker 发送消息 */
        handle_resp: HR,                         /* 处理返回流中的 resp */
        auto_create: bool,                       /* 如果没有 topic 或 key 时自动创建 */
    ) -> Result<()>
    where
        HR: Fn(PublishResp) -> HRFut,
        HRFut: std::future::Future<Output = Result<()>>,
    {
        let (tx_middleware, rx_middleware) = mpsc::channel(1);
        let _topic = self.topic.clone();
        let client = self.client.clone();
        let publisher = self.clone();

        tokio::spawn(async move {
            loop {
                if rx.is_closed() {
                    break;
                }
                let bts = rx.recv().await;
                if bts.is_none() {
                    continue;
                }
                let (key, payload) = bts.unwrap();
                loop {
                    let partition =
                        publisher
                            .client
                            .get_partition(&publisher.topic, &key, auto_create)?;
                    if partition.is_none() {
                        if !auto_create {
                            error!("not found the partition");
                            break;
                            // return Err(anyhow!("not found the partition"));
                        }
                        if !publisher.client.has_topic(&publisher.topic) {
                            publisher.new_topic(&publisher.topic, 0).await?;
                        }
                    }
                    let partition = partition.unwrap();
                    let endpoint = publisher
                        .get_or_create_channel(&partition.broker_leader_addr)
                        .await?;

                    let partition_id = client
                        .get_partition(&_topic, &key, auto_create)
                        .unwrap()
                        .unwrap()
                        .partition_id;
                    let _ = tx_middleware
                        .send(PublishReq {
                            topic: _topic.clone(),
                            partition: partition_id,
                            payload: payload.to_vec(),
                            message_id: None,
                        })
                        .await;
                }
            }
        });

        let req_strm = SmartReqStream::new(ReceiverStream::new(rx_middleware));
        let resp = ClientBrokerServiceClient::new(endpoint)
            .publish(req_strm)
            .await?;

        let mut resp_strm = resp.into_inner();
        while let Ok(Some(resp)) = resp_strm.message().await {
            handle_resp(resp).await?;
        }
        Ok(())
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

fn key_hash_partition(key: String, num_partitions: u32) -> Result<u32> {
    let hash = murmur3_32(&mut Cursor::new(key), 0)?; // 种子为 0，与 Kafka 一致
    Ok((hash % num_partitions) as u32)
}
