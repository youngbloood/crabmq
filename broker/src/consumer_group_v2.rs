use anyhow::{Result, anyhow};
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use grpcx::brokercoosvc::{self, SyncConsumerAssignmentsResp};
use grpcx::clientbrokersvc::{Ack, Fetch, Message, MessageBatch, SegmentOffset};
use grpcx::topic_meta::TopicPartitionDetail;
use log::error;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storagev2::{ReadPosition, SegmentOffset as StorageSegmentOffset};
use storagev2::{StorageReader, StorageReaderSession};
use tokio::sync::{Semaphore, mpsc};
use tokio::time;
use tonic::Status;

pub struct GroupTopicMeta {
    topic: String,
    offset: u32,
    list: Vec<TopicPartitionDetail>,
}
struct GroupMeta {
    id: u32,
    group_topic_metas: Vec<GroupTopicMeta>,
}

impl GroupMeta {
    fn get_topic_read_positions(&self) -> Vec<(String, ReadPosition)> {
        let mut list = vec![];
        self.group_topic_metas.iter().for_each(|v| {
            let read_position = if v.offset == 1 {
                ReadPosition::Latest
            } else {
                ReadPosition::Begin
            };
            list.push((v.topic.clone(), read_position));
        });

        list
    }
}

impl From<brokercoosvc::GroupMeta> for GroupMeta {
    fn from(v: brokercoosvc::GroupMeta) -> Self {
        let mut topic_metas = vec![];
        for gtm in &v.group_topic_metas {
            let tpds = gtm.list.iter().map(TopicPartitionDetail::from).collect();
            topic_metas.push(GroupTopicMeta {
                topic: gtm.topic.clone(),
                offset: gtm.offset,
                list: tpds,
            })
        }

        GroupMeta {
            id: v.id,
            group_topic_metas: topic_metas,
        }
    }
}

impl From<&brokercoosvc::GroupMeta> for GroupMeta {
    fn from(v: &brokercoosvc::GroupMeta) -> Self {
        let mut topic_metas = vec![];
        for gtm in &v.group_topic_metas {
            let tpds = gtm.list.iter().map(TopicPartitionDetail::from).collect();
            topic_metas.push(GroupTopicMeta {
                topic: gtm.topic.clone(),
                offset: gtm.offset,
                list: tpds,
            })
        }

        GroupMeta {
            id: v.id,
            group_topic_metas: topic_metas,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConsumerGroupManager<SR>
where
    SR: StorageReader,
{
    // member_id: group_id
    consumers: Arc<DashMap<String, u32>>,
    // group_id: GroupMeta
    group: Arc<DashMap<u32, GroupMeta>>,

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
            group: Arc::default(),
        };

        // 启动心跳检测任务
        // tokio::spawn(Self::heartbeat_checker(group.clone()));
        group
    }

    pub async fn apply_consumergroup(&self, res: SyncConsumerAssignmentsResp) {
        let group_meta = res.group_meta.unwrap();
        for v in &group_meta.group_topic_metas {
            for tpm in &v.list {
                let tpd = TopicPartitionDetail::from(tpm);
                println!("TPD = {:?}", tpd);
                for member_id in tpd.sub_member_ids {
                    self.consumers.insert(member_id, group_meta.id);
                }
            }
        }

        self.group
            .insert(group_meta.id, GroupMeta::from(group_meta));
    }

    pub async fn check_consumer(&self, member_id: &str, topics: &[String]) -> bool {
        if !self.consumers.contains_key(member_id) {
            return false;
        }
        let group = self.consumers.get(member_id).unwrap();
        let group_id = group.value();

        let has_topics: Vec<String> = self
            .group
            .get(group_id)
            .unwrap()
            .group_topic_metas
            .iter()
            .map(|v| v.topic.clone())
            .collect();

        for topic in topics {
            if !has_topics.contains(&topic) {
                return false;
            }
        }
        true
    }

    pub async fn new_sesssion(&self, member_id: String, client_addr: String) -> Result<()> {
        let group = self.consumers.get(&member_id);
        if group.is_none() {
            return Err(anyhow!("group not found the member"));
        }
        let group = group.unwrap();
        let group_id = group.value();

        let sub_topics = DashMap::new();
        let group_meta = self.group.get(group_id).unwrap();
        group_meta.group_topic_metas.iter().for_each(|v| {
            v.list.iter().for_each(|tpd| {
                sub_topics.insert(
                    (tpd.topic.clone(), tpd.partition_id),
                    (v.offset, tpd.clone()),
                );
            });
        });

        let srs = self
            .storage_reader
            .new_session(*group_id, group_meta.get_topic_read_positions())
            .await?;

        let ss = SubSession::new(member_id, *group_id, sub_topics, client_addr.clone(), srs);
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
    member_id: String,
    group_id: u32,
    // 该客户端分配到的可消费的(topic, partition_id)
    // (topic, partition_id): (offset, TopicPartitionDetail)
    sub_topics: Arc<DashMap<(String, u32), (u32, TopicPartitionDetail)>>,
    // 以链接至 broker 的客户端地址作为 session_id
    client_addr: String,

    window: Arc<Semaphore>,
    storage: Arc<Box<dyn StorageReaderSession>>,

    buf: Arc<SegQueue<(Bytes, StorageSegmentOffset)>>,
    slide_window: SlideWindow,
}

#[derive(Clone)]
struct SlideWindow {}

impl SubSession {
    pub fn new(
        member_id: String,
        group_id: u32,
        sub_topics: DashMap<(String, u32), (u32, TopicPartitionDetail)>,
        client_addr: String,
        storage: Box<dyn StorageReaderSession>,
    ) -> Self {
        let ss = Self {
            member_id,
            group_id,
            sub_topics: Arc::new(sub_topics),
            client_addr,
            window: Arc::new(Semaphore::new(10)),
            storage: Arc::new(storage),
            buf: Arc::default(),
            slide_window: SlideWindow {},
        };
        ss
    }

    pub fn handle_ack(&self, ack: Ack) {}

    pub async fn handle_fetch(&self, f: Fetch, tx: mpsc::Sender<Result<MessageBatch, Status>>) {
        let mut left_bytes = f.max_partition_bytes;
        let mut message_batch = MessageBatch {
            batch_id: nanoid::nanoid!(),
            topic: f.topic.clone(),
            partition_id: f.partition_id,
            messages: Vec::new(),
        };

        let append_msg =
            |batch: &mut MessageBatch, _f: &Fetch, msg: (Bytes, StorageSegmentOffset)| {
                batch.messages.push(Message {
                    topic: _f.topic.clone(),
                    partition: _f.partition_id,
                    message_id: String::new(),
                    payload: msg.0.into(),
                    offset: Some(SegmentOffset {
                        segment: format!("{:?}", msg.1.filename),
                        offset: msg.1.offset,
                    }),
                    metadata: HashMap::new(),
                    credit_remaining: 0,
                    credit_consumed: 0,
                    recommended_next_bytes: 0,
                });
            };

        let mut has_full = false;
        // 先从 buf 中进行填充
        let mut message_count = 0;
        while let Some(msg) = self.buf.pop() {
            if message_count >= f.max_partition_batch_count {
                let _ = tx.send(Ok(message_batch)).await;
                return;
            }
            let msgs_size = msg.0.len() as u64;
            if msgs_size > left_bytes {
                let _ = tx.send(Ok(message_batch)).await;
                return;
            }
            left_bytes -= msgs_size;
            append_msg(&mut message_batch, &f, msg);
            message_count += 1;
        }

        while message_count < f.max_partition_batch_count {
            // buf 中已无缓存，在从 storage 中读取并填充
            match self
                .storage
                .next(&f.topic, f.partition_id, NonZero::new(1).unwrap())
                .await
            {
                Ok(msgs) => {
                    for msg in msgs {
                        message_count += 1;
                        if has_full {
                            // 将已经读出来的数据放入缓存
                            self.buf.push(msg);
                            continue;
                        }
                        let msgs_size = msg.0.len() as u64;
                        if msgs_size > left_bytes {
                            has_full = true;
                            self.buf.push(msg);
                            continue;
                        }
                        left_bytes -= msgs_size;
                        append_msg(&mut message_batch, &f, msg);
                    }
                    // 跳出外层循环
                    if has_full {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!("StorageReader err: {:?}", e))))
                        .await;
                }
            }
        }
        if let Err(e) = tx.send(Ok(message_batch)).await {
            error!("send message_batch to consumer err: {e:?}");
        }
    }

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
