use dashmap::DashMap;
use grpcx::commonsvc::{TopicList, topic_list};
use std::sync::Arc;

#[derive(Clone)]
pub struct PartitionManager {
    broker_id: u32,
    // 当前节点负责的分区 (topic, partition) -> bool
    my_partitions: Arc<DashMap<(String, u32), bool>>,
    // // 协调器客户端
    // client: Option<SmartClient>,
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

    pub fn apply_topic_infos(&self, tl: TopicList) {
        match tl.list.unwrap() {
            topic_list::List::Init(topic_list_init) => {
                for info in topic_list_init.topics {
                    for part in info.partition {
                        self.my_partitions
                            .insert((info.topic.clone(), part.id), true);
                    }
                }
            }
            topic_list::List::Add(topic_list_add) => {
                for info in topic_list_add.topics {
                    for part in info.partition {
                        self.my_partitions
                            .insert((info.topic.clone(), part.id), true);
                    }
                }
            }
        }
    }
}
