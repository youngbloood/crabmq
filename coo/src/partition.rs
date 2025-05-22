use crate::BrokerNode;
use dashmap::DashMap;
use std::sync::Arc;

// 分区分配策略
#[derive(Debug, Clone)]
pub struct PartitionPolicy {
    pub replication_factor: usize,
}

impl Default for PartitionPolicy {
    fn default() -> Self {
        Self {
            replication_factor: 2,
        }
    }
}

/// 分区管理器
///
/// 负责新 `topic` 的分区分配: 将分区分配至各个 `broker` 节点
///
/// 负责已有 `topic` 的增加分区分配: 将新增的分区分配至各个 `broker` 节点
#[derive(Debug, Clone)]
pub struct PartitionManager {
    // topic -> 分区分配信息
    assignments: Arc<DashMap<String, TopicAssignment>>,
    policy: PartitionPolicy,
}

#[derive(Debug, Clone)]
struct TopicAssignment {
    partitions: Vec<PartitionInfo>,
    version: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub id: u32,
    pub brokers: Vec<u32>, // 主副本 + 副本
}

impl PartitionManager {
    pub fn new(policy: PartitionPolicy) -> Self {
        Self {
            assignments: Arc::new(DashMap::new()),
            policy,
        }
    }

    // 分配新topic分区
    pub fn assign_new_topic(
        &self,
        topic: &str,
        num_partitions: u32,
        brokers: Arc<DashMap<u32, BrokerNode>>,
    ) -> Arc<Vec<PartitionInfo>> {
        let broker_ids = Self::select_available_brokers(brokers, self.policy.replication_factor);

        let partitions = (0..num_partitions)
            .map(|id| PartitionInfo {
                id,
                brokers: Self::select_replicas(&broker_ids, self.policy.replication_factor),
            })
            .collect();

        self.assignments.insert(
            topic.to_string(),
            TopicAssignment {
                partitions,
                version: 0,
            },
        );

        Arc::new(self.assignments.get(topic).unwrap().partitions.clone())
    }

    // 增加现有topic分区
    pub fn add_partitions(
        &self,
        topic: &str,
        additional: u32,
        brokers: Arc<DashMap<u32, BrokerNode>>,
    ) -> Arc<Vec<PartitionInfo>> {
        if !self.assignments.contains_key(topic) {
            return Arc::new(vec![]);
        }
        let mut assignment = self.assignments.get_mut(topic).unwrap();
        let start_id = assignment.partitions.len() as u32;

        let broker_ids = Self::select_available_brokers(brokers, self.policy.replication_factor);

        let new_partitions: Vec<_> = (start_id..start_id + additional)
            .map(|id| PartitionInfo {
                id,
                brokers: Self::select_replicas(&broker_ids, self.policy.replication_factor),
            })
            .collect();

        assignment.partitions.extend(new_partitions.clone());
        assignment.version += 1;

        Arc::new(new_partitions)
    }

    pub fn has_topic(&self, topic: &str) -> bool {
        self.assignments.contains_key(topic)
    }

    // 选择可用broker（基于负载）
    fn select_available_brokers(
        brokers: Arc<DashMap<u32, BrokerNode>>,
        required: usize,
    ) -> Vec<u32> {
        let mut candidates: Vec<_> = brokers
            .iter()
            .map(|entry| {
                let bn = entry.value();
                let load_score = bn.state.cpurat as f32 * 0.3
                    + bn.state.memrate as f32 * 0.3
                    + bn.state.diskrate as f32 * 0.4;
                (*entry.key(), load_score)
            })
            .collect();

        // 按负载排序，选择负载最低的
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        candidates
            .iter()
            .take(required)
            .map(|(id, _)| *id)
            .collect()
    }

    // 选择副本
    fn select_replicas(broker_ids: &[u32], replication_factor: usize) -> Vec<u32> {
        // 简单轮询选择，实际可实现更复杂策略
        broker_ids
            .iter()
            .cycle()
            .take(replication_factor)
            .cloned()
            .collect()
    }
}
