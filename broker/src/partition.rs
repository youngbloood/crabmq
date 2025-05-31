use dashmap::DashMap;
use grpcx::{
    commonsvc::{self, TopicPartitionMeta},
    topic_meta::TopicPartitionDetail,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct PartitionManager {
    broker_id: u32,
    // 当前节点负责的分区 (topic, partition) -> bool
    my_partitions: Arc<DashMap<(String, u32), ()>>,
}

impl PartitionManager {
    pub fn new(broker_id: u32) -> Self {
        PartitionManager {
            broker_id,
            my_partitions: Arc::default(),
        }
    }

    pub fn is_my_partition(&self, topic: &str, partition: u32) -> bool {
        self.my_partitions
            .contains_key(&(topic.to_string(), partition))
    }

    pub fn apply_topic_infos(&self, tpms: Vec<TopicPartitionDetail>) {
        for tpm in &tpms {
            self.my_partitions
                .insert((tpm.topic.clone(), tpm.partition_id), ());
        }
    }
}
