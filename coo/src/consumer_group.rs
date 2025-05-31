use crate::event_bus::EventBus;
use anyhow::Result;
use dashmap::{DashMap, DashSet};
use grpcx::brokercoosvc;
use grpcx::clientcoosvc::{
    self, GroupJoinResponse, SyncConsumerAssignmentsResp, group_join_response,
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
    topics: Arc<DashSet<String>>,
    // 消费容量
    capacity: u32,
    last_seen: u64, // timestamp
}

#[derive(Debug, Clone, Default)]
struct ConsumerGroup {
    id: u32,
    status: GroupStatus,
    generation_id: i32,

    // 该消费者组订阅的 topics
    topics: Arc<DashSet<String>>,
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
        // 按容量排序成员（从高到低）
        let mut members: Vec<_> = self
            .members
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        members.sort_by(|a, b| b.capacity.cmp(&a.capacity));

        // 计算总容量
        let total_capacity: u32 = members.iter().map(|m| m.capacity).sum();
        if total_capacity == 0 || members.is_empty() {
            // 清空分配并返回
            self.assignments = Arc::new(DashMap::new());
            self.generation_id += 1;
            self.status = GroupStatus::Stable;
            return;
        }

        // 计算总分区数
        let total_partitions = all_partitions
            .iter()
            .map(|(_, parts)| parts.len())
            .sum::<usize>();

        if total_partitions == 0 {
            // 没有分区需要分配
            self.assignments = Arc::new(DashMap::new());
            self.generation_id += 1;
            self.status = GroupStatus::Stable;
            return;
        }

        // 为每个分区分配成员
        let mut global_partition_index = 0;

        for (_, partitions) in all_partitions.iter_mut() {
            for partition in partitions.iter_mut() {
                // 计算应分配的成员索引（基于容量的权重分配）
                let target_ratio = global_partition_index as f32 / total_partitions as f32;
                let mut accumulated = 0.0;
                let mut member_idx = 0;

                for (idx, member) in members.iter().enumerate() {
                    let ratio = member.capacity as f32 / total_capacity as f32;
                    accumulated += ratio;

                    if accumulated >= target_ratio {
                        member_idx = idx;
                        break;
                    }
                }

                // 获取成员ID
                let member_id = members[member_idx].member_id.clone();

                // 更新分区的sub_member_ids（只分配一个成员）
                partition.sub_member_ids = vec![member_id];

                global_partition_index += 1;
            }
        }

        // 创建新的分配映射
        let new_assignments = DashMap::new();
        for (topic, partitions) in all_partitions {
            new_assignments.insert(topic, partitions);
        }

        // 更新组状态
        self.assignments = Arc::new(new_assignments);
        self.generation_id += 1;
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
                let topics = DashSet::new();
                req.topics.iter().for_each(|v| {
                    topics.insert(v.to_string());
                });

                ConsumerGroup {
                    id: req.group_id,
                    status: GroupStatus::Empty,
                    generation_id: 0,
                    topics: Arc::new(topics),
                    ..Default::default()
                }
            });

        let topics = DashSet::new();
        req.topics.iter().for_each(|v| {
            topics.insert(v.to_string());
        });

        // 检查所有topic是否存在
        for topic in topics.iter() {
            if !self.all_topics.contains_key(&*topic) {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::InvalidTopic.into(),
                    ..Default::default()
                });
            }
        }

        // 校验订阅一致性
        if !group.topics.is_empty() {
            if group.topics.len() != topics.len() {
                return Ok(GroupJoinResponse {
                    error: group_join_response::ErrorCode::SubscriptionConflict.into(),
                    ..Default::default()
                });
            }
            for k in topics {
                if !group.topics.contains(&k) {
                    return Ok(GroupJoinResponse {
                        error: group_join_response::ErrorCode::SubscriptionConflict.into(),
                        ..Default::default()
                    });
                }
            }
        }

        // 首次加入初始化
        if group.status == GroupStatus::Empty {
            req.topics.iter().for_each(|v| {
                group.topics.insert(v.to_string());
            });
            group.status = GroupStatus::Stable;
        }

        // 生成成员ID
        let member_id = format!("{}-{}", req.group_id, nanoid::nanoid!());
        let member = ConsumerMember {
            member_id: member_id.clone(),
            topics: group.topics.clone(),
            capacity: req.capacity,
            last_seen: chrono::Local::now().timestamp_millis() as _,
        };

        // 首次成员分配全部分区
        if group.members.is_empty() {
            group.members.insert(member_id.clone(), member);
            group.clear_member();
            self.assign_all_partitions(&mut group);
            return self.build_join_response(&mut group, &member_id);
        }

        // 触发 Rebalance
        group.members.insert(member_id.clone(), member);
        group.status = GroupStatus::Rebalancing;
        self.trigger_rebalance(&mut group).await;
        self.build_join_response(&mut group, &member_id)
    }

    pub fn subscribe_client_consumer(
        &self,
        req: clientcoosvc::SyncConsumerAssignmentsReq,
    ) -> mpsc::Receiver<clientcoosvc::SyncConsumerAssignmentsResp> {
        let (_, rx) = self
            .client_consumer_bus
            .subscribe(format!("{}-{}", req.group_id, req.member_id));
        rx
    }

    pub fn unsubscribe_client_consumer(&self, req: clientcoosvc::SyncConsumerAssignmentsReq) {
        self.client_consumer_bus
            .unsubscribe(&format!("{}-{}", req.group_id, req.member_id));
    }

    pub fn subscribe_broker_consumer(
        &self,
        req: brokercoosvc::SyncConsumerAssignmentsReq,
    ) -> mpsc::Receiver<brokercoosvc::SyncConsumerAssignmentsResp> {
        let (_, rx) = self
            .broker_consumer_bus
            .subscribe(format!("broker_subscribe_consumer_group_{}", req.broker_id));
        rx
    }

    pub fn unsubscribe_broker_consumer(&self, req: brokercoosvc::SyncConsumerAssignmentsReq) {
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
        Ok(GroupJoinResponse {
            group_id: cg.id,
            member_id: member_id.to_string(),
            generation_id: 0,
            assigned_topics: topics,
            status: GroupStatus::Stable.into(),
            error: 0,
        })
    }

    fn assign_all_partitions(&self, group: &mut ConsumerGroup) {
        let all_partitions = self.collect_partitions(group);
        group.assign_all_partitions(all_partitions);
    }

    fn collect_partitions(
        &self,
        group: &ConsumerGroup,
    ) -> Vec<(String, Vec<TopicPartitionDetail>)> {
        let mut topic_pool = vec![];
        for topic in group.topics.iter() {
            if let Some(assignment) = self.all_topics.get(topic.key()) {
                topic_pool.push((topic.clone(), assignment.value().clone()));
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
                        resp.list
                            .push(partition.convert_to_topic_partition_meta(topic));
                    })
                    .or_insert_with(|| brokercoosvc::SyncConsumerAssignmentsResp {
                        list: vec![partition.convert_to_topic_partition_meta(topic)],
                        broker_id: partition.broker_leader_id,
                        term: 0,
                    });
            }
        }

        // 广播
        for v in broker_map.values() {
            self.broker_consumer_bus.broadcast(v.clone()).await;
        }
    }
}
