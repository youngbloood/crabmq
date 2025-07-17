use anyhow::{Result, anyhow};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use grpcx::brokercoosvc::{self, SyncConsumerAssignmentsResp};
use grpcx::clientbrokersvc::{Commit, Fetch, MessageBatch, MessageResp, SegmentOffset};
use grpcx::topic_meta::TopicPartitionDetail;
use log::{error, warn};
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use storagev2::{MessagePayload, ReadPosition, SegmentOffset as StorageSegmentOffset};
use storagev2::{StorageReader, StorageReaderSession};
use tokio::sync::{Semaphore, mpsc};
use tokio::time;
use tonic::Status;

#[derive(Clone)]
pub struct GroupTopicMeta {
    topic: String,
    offset: u32,
    list: Vec<TopicPartitionDetail>,
}

#[derive(Clone)]
struct GroupMeta {
    id: u32,
    consumer_slide_window_size: u64,
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
            consumer_slide_window_size: v.consumer_slide_window_size,
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
            consumer_slide_window_size: v.consumer_slide_window_size,
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
    sessions: Arc<DashMap<String, ConsumerSession>>,

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

        let group_meta = self.group.get(group_id);
        if group_meta.is_none() {
            return Err(anyhow!("not found the group meta"));
        }
        let group_meta = group_meta.unwrap();
        let srs = self
            .storage_reader
            .new_session(*group_id, group_meta.get_topic_read_positions())
            .await
            .map_err(|e| anyhow!("Failed to new session: {:?}", e))?;

        let ss = ConsumerSession::new(
            member_id,
            *group_id,
            group_meta.value().clone(),
            // sub_topics,
            client_addr.clone(),
            srs,
        );
        self.sessions.insert(client_addr, ss);
        Ok(())
    }

    pub fn get_session(&self, client_addr: &str) -> Option<ConsumerSession> {
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

// ConsumerSession 从 Storage 中获取消息
// 流量控制等
#[derive(Clone)]
pub struct ConsumerSession {
    member_id: String,
    group_id: u32,
    // 该客户端分配到的可消费的(topic, partition_id)
    // (topic, partition_id): (offset, TopicPartitionDetail)
    sub_topics: Arc<DashMap<(String, u32), (u32, TopicPartitionDetail)>>,
    // 以链接至 broker 的客户端地址作为 session_id
    client_addr: String,

    window: Arc<Semaphore>,
    storage: Arc<Box<dyn StorageReaderSession>>,

    buf: Arc<SegQueue<(MessagePayload, u64, StorageSegmentOffset)>>,
    slide_window: SlideWindow,
}

#[derive(Clone)]
struct SlideWindow {
    // (topic,partition): Vec<SlideWindowBatchMessage>
    partition_offsets: Arc<DashMap<(String, u32), Vec<SlideWindowBatchMessage>>>,
    window_size: u64,
    left_size: Arc<AtomicU64>,
}

struct SlideWindowBatchMessage {
    last_offset: SegmentOffset,
    messge_size: u64,
}

impl SlideWindow {
    fn handle_resp_message_batch(&self, mb: &MessageBatch) {
        if mb.messages.is_empty() {
            return;
        }
        let last_offset = mb.messages.last().unwrap().offset.unwrap();
        let messge_size = mb.messages.iter().map(|v| v.payload.len()).sum::<usize>() as u64;
        let key = (mb.topic.clone(), mb.partition_id);
        self.partition_offsets
            .entry(key)
            .and_modify(|v| {
                v.push(SlideWindowBatchMessage {
                    last_offset,
                    messge_size,
                });
            })
            .or_insert(vec![SlideWindowBatchMessage {
                last_offset,
                messge_size,
            }]);

        if self.left_size.fetch_max(messge_size, Ordering::Relaxed) == messge_size {
            self.left_size.store(0, Ordering::Relaxed);
        } else {
            self.left_size.fetch_sub(messge_size, Ordering::Relaxed);
        }
    }

    fn handle_commit_offset(&self, topic: &str, partition_id: u32, mb: &SegmentOffset) {
        self.partition_offsets
            .entry((topic.to_string(), partition_id))
            .and_modify(|v| {
                for w in v.iter() {
                    if compare_segment_offest(mb, &w.last_offset) <= 0 {
                        break;
                    }
                    self.left_size.fetch_add(w.messge_size, Ordering::Relaxed);
                }
                v.retain(|x| compare_segment_offest(mb, &x.last_offset) > 0);
            });
    }
}

// if a > b; return 1
// if a == b; return 0
// if a < b; return -1
fn compare_segment_offest(a: &SegmentOffset, b: &SegmentOffset) -> i8 {
    if a.segment_id > b.segment_id {
        1
    } else if a.segment_id < b.segment_id {
        -1
    } else if a.offset > b.offset {
        1
    } else if a.offset < b.offset {
        -1
    } else {
        0
    }
}

impl ConsumerSession {
    pub fn new(
        member_id: String,
        group_id: u32,
        group_meta: GroupMeta,
        client_addr: String,
        storage: Box<dyn StorageReaderSession>,
    ) -> Self {
        let sub_topics = DashMap::new();
        group_meta.group_topic_metas.iter().for_each(|v| {
            v.list.iter().for_each(|tpd| {
                sub_topics.insert(
                    (tpd.topic.clone(), tpd.partition_id),
                    (v.offset, tpd.clone()),
                );
            });
        });

        let ss = Self {
            member_id,
            group_id,
            sub_topics: Arc::new(sub_topics),
            client_addr,
            window: Arc::new(Semaphore::new(10)),
            storage: Arc::new(storage),
            buf: Arc::default(),
            slide_window: SlideWindow {
                window_size: group_meta.consumer_slide_window_size,
                left_size: Arc::new(AtomicU64::new(group_meta.consumer_slide_window_size)),
                partition_offsets: Arc::default(),
            },
        };

        ss
    }

    pub async fn handle_fetch(&self, f: Fetch, tx: mpsc::Sender<Result<MessageBatch, Status>>) {
        // min(客户端:max_partition_bytes, slide_window:left_size)
        let mut left_bytes = self
            .slide_window
            .left_size
            .fetch_min(f.max_partition_bytes, Ordering::Relaxed);

        let mut message_batch = MessageBatch {
            batch_id: nanoid::nanoid!(),
            topic: f.topic.clone(),
            partition_id: f.partition_id,
            messages: Vec::new(),
        };

        let append_msg = |batch: &mut MessageBatch, msg: (MessagePayload, StorageSegmentOffset)| {
            // let mr: MessageReq = bincode::decode_from_slice(&msg.0, bincode::config::standard())
            //     .unwrap()
            //     .0;

            batch.messages.push(MessageResp {
                message_id: std::mem::take(&mut Some(msg.0.msg_id)),
                payload: msg.0.payload,
                metadata: msg.0.metadata,
                offset: Some(SegmentOffset {
                    segment_id: msg.1.segment_id,
                    offset: msg.1.offset,
                }),
            });
        };

        let mut has_full = false;

        // FIXME: 这里有问题，next剩余的消息放入到buf中，可能与下次fetch的不是同 topic-parition
        // 先从 buf 中进行填充
        let mut message_count = 0;
        while let Some(msg) = self.buf.pop() {
            if message_count >= f.max_partition_batch_count {
                let _ = tx.send(Ok(message_batch)).await;
                return;
            }
            let msgs_size = msg.1;
            if msgs_size > left_bytes {
                let _ = tx.send(Ok(message_batch)).await;
                return;
            }
            left_bytes -= msgs_size;
            append_msg(&mut message_batch, (msg.0, msg.2));
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
                        let msgs_size = msg.1;
                        if msgs_size > left_bytes {
                            has_full = true;
                            self.buf.push(msg);
                            continue;
                        }
                        left_bytes -= msgs_size;
                        append_msg(&mut message_batch, (msg.0, msg.2));
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

        self.slide_window.handle_resp_message_batch(&message_batch);
        if let Err(e) = tx.send(Ok(message_batch)).await {
            warn!("send message_batch to consumer err: {e:?}, maybe it close");
        }
    }

    pub async fn handle_commit(
        &self,
        commit: Commit,
        _tx: mpsc::Sender<Result<MessageBatch, Status>>,
    ) {
        let offset = StorageSegmentOffset {
            segment_id: commit.commit_pos.as_ref().unwrap().segment_id,
            offset: commit.commit_pos.as_ref().unwrap().offset,
        };
        if let Err(e) = self
            .storage
            .commit(&commit.topic, commit.partition_id, offset)
            .await
        {
            error!(
                "commit the topic-parittion[{}-{}] pos[{:?}] err: {e:?}",
                &commit.topic,
                commit.partition_id,
                commit.commit_pos.unwrap()
            );
            return;
        }

        self.slide_window.handle_commit_offset(
            &commit.topic,
            commit.partition_id,
            &commit.commit_pos.unwrap(),
        );
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
