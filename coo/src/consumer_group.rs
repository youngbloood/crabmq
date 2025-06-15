use crate::event_bus::EventBus;
use anyhow::Result;
use dashmap::DashMap;
use grpcx::brokercoosvc::{self, GroupMeta, GroupTopicMeta};
use grpcx::clientcoosvc::{
    self, GroupJoinRequestOption, GroupJoinResponse, GroupJoinSubTopic, group_join_response,
};
use grpcx::commonsvc::GroupStatus;
use grpcx::topic_meta::TopicPartitionDetail;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
struct ConsumerMember {
    // member_id: 由 coo 端生成
    member_id: String,
    topics: Arc<DashMap<String, u32>>,
    last_seen: u64, // timestamp
}

#[derive(Debug, Clone, Default)]
struct ConsumerGroup {
    id: u32,
    status: GroupStatus,

    auto_commit: bool,
    consumer_slide_window_size: u64,

    // 该消费者组订阅的 topics
    topics: Arc<DashMap<String, u32>>,
    // member_id -> ConsumerMember
    members: Arc<DashMap<String, ConsumerMember>>,
    // topic -> Vec<PartitionMeta>
    assignments: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
}

impl ConsumerGroup {
    fn clear_member(&self) {
        let _ = self.assignments.iter_mut().map(|mut v| {
            let _ = v.iter_mut().map(|pm| {
                pm.sub_member_ids.clear();
            });
        });
    }

    fn gen_option(&self) -> GroupJoinRequestOption {
        let mut sub_topics = Vec::with_capacity(self.topics.len());
        self.topics.iter().for_each(|t| {
            sub_topics.push(GroupJoinSubTopic {
                topic: t.key().clone(),
                offset: *t.value(),
            });
        });
        GroupJoinRequestOption {
            sub_topics,
            auto_commit: self.auto_commit,
            consumer_slide_window_size: self.consumer_slide_window_size,
        }
    }

    fn assign_all_partitions(
        &mut self,
        mut all_partitions: Vec<(String, Vec<TopicPartitionDetail>)>,
    ) {
        let member_id = self.members.iter().next().unwrap().key().clone();
        // 创建新的分配映射

        for (topic, partitions) in &mut all_partitions {
            for p in partitions.iter_mut() {
                p.sub_member_ids.push(member_id.clone());
            }
            self.assignments
                .insert(topic.clone(), partitions.clone().to_vec());
        }
    }

    fn trigger_rebalance(&mut self, mut all_partitions: Vec<(String, Vec<TopicPartitionDetail>)>) {
        let members: Vec<_> = self
            .members
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        // 计算总分区数
        let total_partitions = all_partitions
            .iter()
            .map(|(_, parts)| parts.len())
            .sum::<usize>();

        if total_partitions == 0 {
            // 没有分区需要分配
            self.assignments = Arc::new(DashMap::new());
            self.status = GroupStatus::Stable;
            return;
        }

        // 为每个分区分配成员
        let mut member_idx = 0;
        for (_, partitions) in all_partitions.iter_mut() {
            for partition in partitions.iter_mut() {
                // 获取成员ID
                let member_id = members[member_idx % members.len()].member_id.clone();
                // 更新分区的sub_member_ids（只分配一个成员）
                partition.sub_member_ids = vec![member_id];
                member_idx += 1;
            }
        }

        // 创建新的分配映射
        let new_assignments = DashMap::new();
        for (topic, partitions) in all_partitions {
            new_assignments.insert(topic, partitions);
        }

        // 更新组状态
        self.assignments = Arc::new(new_assignments);
        self.status = GroupStatus::Stable;
    }
}

#[derive(Clone)]
pub struct ConsumerGroupManager {
    // GroupID -> ConsumerGroup
    groups: Arc<DashMap<u32, ConsumerGroup>>,
    // Topic -> TopicAssignment
    // Shared with PartitionManager.all_topics
    all_topics: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
    // 消费者组变更事件总线
    broker_consumer_bus: EventBus<brokercoosvc::SyncConsumerAssignmentsResp>,
    client_consumer_bus: EventBus<clientcoosvc::SyncConsumerAssignmentsResp>,
}

impl ConsumerGroupManager {
    pub fn new(all_topics: Arc<DashMap<String, Vec<TopicPartitionDetail>>>) -> Self {
        Self {
            groups: Arc::default(),
            all_topics,
            broker_consumer_bus: EventBus::new(10),
            client_consumer_bus: EventBus::new(10),
        }
    }

    pub async fn apply_join(
        &self,
        req: &clientcoosvc::GroupJoinRequest,
    ) -> Result<GroupJoinResponse> {
        let mut group = self
            .groups
            .entry(req.group_id)
            .or_insert_with(|| -> ConsumerGroup {
                let topics = DashMap::new();
                if req.opt.is_some() {
                    req.opt.as_ref().unwrap().sub_topics.iter().for_each(|v| {
                        topics.insert(v.topic.clone(), v.offset);
                    });
                }
                ConsumerGroup {
                    id: req.group_id,
                    status: GroupStatus::Empty,
                    topics: Arc::new(topics),
                    ..Default::default()
                }
            });

        // 该group的首位成员
        if group.members.is_empty() {
            if req.opt.is_none() {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::FirstMemberMustSetConfig.into(),
                    ..Default::default()
                });
            }

            let opt = req.opt.as_ref().unwrap();
            let sub_topics = &opt.sub_topics;
            // 检查所有topic是否存在
            for st in sub_topics.iter() {
                if !self.all_topics.contains_key(&*st.topic) {
                    return Ok(GroupJoinResponse {
                        error: group_join_response::ErrorCode::InvalidTopic.into(),
                        ..Default::default()
                    });
                }
            }
            if opt.consumer_slide_window_size == 0 {
                group.consumer_slide_window_size = 1024 * 1024 * 10;
            } else {
                group.consumer_slide_window_size = opt.consumer_slide_window_size
            }
        } else if req.opt.is_some() {
            // 该group后续加入的成员
            let opt = req.opt.as_ref().unwrap();
            let sub_topics = &opt.sub_topics;
            if sub_topics.len() != group.topics.len() {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::InvalidTopic.into(),
                    ..Default::default()
                });
            }

            // 校验订阅一致性
            for st in sub_topics.iter() {
                if !group.topics.contains_key(&st.topic) {
                    return Ok(GroupJoinResponse {
                        error: group_join_response::ErrorCode::InvalidTopic.into(),
                        ..Default::default()
                    });
                }
                if *group.topics.get(&st.topic).unwrap().value() != st.offset {
                    return Ok(GroupJoinResponse {
                        error: group_join_response::ErrorCode::InvalidTopic.into(),
                        ..Default::default()
                    });
                }
            }

            if opt.auto_commit != group.auto_commit {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::AutoCommitConflict.into(),
                    ..Default::default()
                });
            }

            if opt.consumer_slide_window_size != 0
                && opt.consumer_slide_window_size != group.consumer_slide_window_size
            {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::SlideWindowConflict.into(),
                    ..Default::default()
                });
            }
        }

        // 生成成员ID
        let member_id = format!("{}-{}", req.group_id, nanoid::nanoid!());
        let member = ConsumerMember {
            member_id: member_id.clone(),
            topics: group.topics.clone(),
            last_seen: chrono::Local::now().timestamp_millis() as _,
        };

        // 首次成员分配全部分区
        if group.members.is_empty() {
            group.members.insert(member_id.clone(), member);
            group.clear_member();
            self.assign_all_partitions(&mut group).await;
            return self.build_join_response(&group, &member_id);
        }

        // 触发 Rebalance
        group.members.insert(member_id.clone(), member);
        group.status = GroupStatus::Rebalancing;
        self.trigger_rebalance(&mut group).await;
        self.build_join_response(&group, &member_id)
    }

    pub fn subscribe_client_consumer(
        &self,
        req: &clientcoosvc::SyncConsumerAssignmentsReq,
    ) -> mpsc::Receiver<clientcoosvc::SyncConsumerAssignmentsResp> {
        let (_, rx) = self
            .client_consumer_bus
            .subscribe(format!("{}-{}", req.group_id, req.member_id));
        rx
    }

    pub fn unsubscribe_client_consumer(&self, req: &clientcoosvc::SyncConsumerAssignmentsReq) {
        self.client_consumer_bus
            .unsubscribe(&format!("{}-{}", req.group_id, req.member_id));
    }

    pub fn subscribe_broker_consumer(
        &self,
        req: &brokercoosvc::SyncConsumerAssignmentsReq,
    ) -> mpsc::Receiver<brokercoosvc::SyncConsumerAssignmentsResp> {
        let (_, rx) = self
            .broker_consumer_bus
            .subscribe(format!("broker_subscribe_consumer_group_{}", req.broker_id));
        rx
    }

    pub fn unsubscribe_broker_consumer(&self, req: &brokercoosvc::SyncConsumerAssignmentsReq) {
        self.broker_consumer_bus.unsubscribe(&format!(
            "broker_subscribe_consumer_group_{}",
            req.broker_id
        ));
    }

    fn build_join_response(
        &self,
        cg: &ConsumerGroup,
        member_id: &str,
    ) -> Result<GroupJoinResponse> {
        let mut topics: Vec<String> = vec![];
        cg.topics.iter().for_each(|v| topics.push(v.to_string()));

        let mut tpms = vec![];
        for tpds in cg.assignments.iter() {
            for tpd in tpds.value() {
                if tpd.sub_member_ids.contains(&member_id.to_string()) {
                    tpms.push(tpd.convert_to_topic_partition_meta(tpds.key()));
                }
            }
        }

        Ok(GroupJoinResponse {
            group_id: cg.id,
            member_id: member_id.to_string(),
            opt: Some(cg.gen_option()),
            list: tpms,
            status: GroupStatus::Stable.into(),
            error: 0,
            error_msg: "".to_string(),
        })
    }

    async fn assign_all_partitions(&self, group: &mut ConsumerGroup) {
        let all_partitions = self.collect_partitions(group);
        group.assign_all_partitions(all_partitions);
        self.notify_clients(group, group.assignments.clone()).await;
        self.notify_brokers(group, group.assignments.clone()).await;
    }

    fn collect_partitions(
        &self,
        group: &ConsumerGroup,
    ) -> Vec<(String, Vec<TopicPartitionDetail>)> {
        let mut topic_pool = vec![];
        for topic in group.topics.iter() {
            if let Some(assignment) = self.all_topics.get(topic.key()) {
                topic_pool.push((topic.key().clone(), assignment.value().clone()));
            }
        }
        topic_pool
    }

    async fn trigger_rebalance(&self, group: &mut ConsumerGroup) {
        // 收集所有分区
        let all_partitions = self.collect_partitions(group);
        group.trigger_rebalance(all_partitions);
        // 7. 通知客户端和Broker
        self.notify_clients(group, group.assignments.clone()).await;
        self.notify_brokers(group, group.assignments.clone()).await;
    }

    async fn notify_clients(
        &self,
        group: &ConsumerGroup,
        partition_meta: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
    ) {
        let mut member_map = HashMap::new();

        // 遍历 DashMap
        for entry in partition_meta.iter() {
            let topic = entry.key();
            let partitions = entry.value();

            for partition in partitions {
                // 确保使用正确的共享访问方式
                for member_id in partition.sub_member_ids.iter() {
                    member_map
                        .entry(member_id.clone())
                        .and_modify(|resp: &mut clientcoosvc::SyncConsumerAssignmentsResp| {
                            resp.list
                                .push(partition.convert_to_topic_partition_meta(topic));
                        })
                        .or_insert_with(|| clientcoosvc::SyncConsumerAssignmentsResp {
                            list: vec![partition.convert_to_topic_partition_meta(topic)],
                            group_id: group.id,
                            member_id: member_id.clone(),
                            status: 0,
                        });
                }
            }
        }

        println!("广播client: {:?}", member_map);
        // 广播
        for v in member_map.values() {
            self.client_consumer_bus.broadcast(v.clone()).await;
        }
    }

    async fn notify_brokers(
        &self,
        group: &ConsumerGroup,
        partition_meta: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
    ) {
        let mut broker_map = HashMap::new();

        // 遍历 DashMap
        for entry in partition_meta.iter() {
            let topic = entry.key();
            let partitions = entry.value();

            for partition in partitions {
                broker_map
                    .entry(partition.broker_leader_id)
                    .and_modify(|resp: &mut brokercoosvc::SyncConsumerAssignmentsResp| {
                        resp.group_meta
                            .as_mut()
                            .unwrap()
                            .group_topic_metas
                            .push(GroupTopicMeta {
                                topic: topic.clone(),
                                offset: *group.topics.get(topic).unwrap(),
                                list: vec![partition.convert_to_topic_partition_meta(topic)],
                            });
                    })
                    .or_insert_with(|| -> brokercoosvc::SyncConsumerAssignmentsResp {
                        brokercoosvc::SyncConsumerAssignmentsResp {
                            group_meta: Some(GroupMeta {
                                id: group.id,
                                consumer_slide_window_size: group.consumer_slide_window_size,
                                group_topic_metas: vec![GroupTopicMeta {
                                    topic: topic.clone(),
                                    offset: *group.topics.get(topic).unwrap(),
                                    list: vec![partition.convert_to_topic_partition_meta(topic)],
                                }],
                            }),
                            broker_id: partition.broker_leader_id,
                            term: 0,
                        }
                    });
            }
        }

        println!("广播broker: {:?}", broker_map);
        // 广播
        for v in broker_map.values() {
            self.broker_consumer_bus.broadcast(v.clone()).await;
        }
    }
}
