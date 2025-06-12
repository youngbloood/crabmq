use dashmap::DashMap;
use grpcx::{commonsvc, topic_meta::TopicPartitionDetail};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PartitionManager {
    broker_id: u32,
    // 当前节点负责的分区 topic: vec<partition_id>
    my_partitions: Arc<DashMap<String, Vec<u32>>>,
}

impl PartitionManager {
    pub fn new(broker_id: u32) -> Self {
        PartitionManager {
            broker_id,
            my_partitions: Arc::default(),
        }
    }

    pub fn is_my_partition(&self, topic: &str, partition_id: u32) -> bool {
        self.my_partitions.contains_key(topic)
            && self
                .my_partitions
                .get(topic)
                .unwrap()
                .contains(&partition_id)
    }

    pub fn is_my_topic(&self, topics: &[String]) -> bool {
        for topic in topics {
            if !self.my_partitions.contains_key(topic) {
                return false;
            }
        }
        true
    }

    pub fn apply_topic_infos(&self, tpms: Vec<TopicPartitionDetail>) {
        for tpm in &tpms {
            self.my_partitions
                .entry(tpm.topic.clone())
                .and_modify(|partitions| {
                    partitions.push(tpm.partition_id);
                })
                .or_insert(vec![tpm.partition_id]);
        }
    }
}
