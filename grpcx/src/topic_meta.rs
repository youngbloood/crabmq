use crate::commonsvc::{LabelValue, TopicPartitionMeta};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct TopicPartitionDetail {
    pub topic: String,
    // 分区 id
    pub partition_id: u32,
    // 该 partition 分布的 broekr
    pub broker_leader_id: u32,
    pub broker_leader_addr: String,
    pub broker_follower_ids: Arc<Vec<u32>>,
    pub broker_follower_addrs: Arc<Vec<String>>,

    // 该 partition 的其他标签
    // 发布时的 key
    pub pub_keys: Arc<Vec<String>>,

    // 订阅时的 member_id
    pub sub_member_ids: Vec<String>,
}

impl From<TopicPartitionDetailSnapshot> for TopicPartitionDetail {
    fn from(v: TopicPartitionDetailSnapshot) -> Self {
        TopicPartitionDetail {
            topic: v.topic,
            partition_id: v.id,
            broker_leader_id: v.broker_leader_id,
            broker_leader_addr: v.broker_leader_addr.clone(),
            broker_follower_ids: Arc::new(v.broker_follower_ids.clone()),
            broker_follower_addrs: Arc::new(v.broker_follower_addrs.clone()),
            pub_keys: Arc::new(v.pub_keys.clone()),
            sub_member_ids: v.sub_member_ids.clone(),
        }
    }
}

impl From<TopicPartitionMeta> for TopicPartitionDetail {
    fn from(v: TopicPartitionMeta) -> Self {
        Self::from(TopicPartitionDetailSnapshot::from(v))
    }
}

impl From<&TopicPartitionMeta> for TopicPartitionDetail {
    fn from(v: &TopicPartitionMeta) -> Self {
        Self::from(TopicPartitionDetailSnapshot::from(v))
    }
}

impl TopicPartitionDetail {
    fn clone_without_members(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            partition_id: self.partition_id,
            broker_leader_id: self.broker_leader_id,
            broker_leader_addr: self.broker_leader_addr.clone(),
            broker_follower_ids: self.broker_follower_ids.clone(),
            broker_follower_addrs: self.broker_follower_addrs.clone(),
            pub_keys: self.pub_keys.clone(),
            sub_member_ids: vec![],
        }
    }

    pub fn snapshot(&self) -> TopicPartitionDetailSnapshot {
        TopicPartitionDetailSnapshot {
            topic: self.topic.clone(),
            id: self.partition_id,
            broker_leader_id: self.broker_leader_id,
            broker_leader_addr: self.broker_leader_addr.clone(),
            broker_follower_ids: self.broker_follower_ids.iter().copied().collect(),
            broker_follower_addrs: self
                .broker_follower_addrs
                .iter()
                .map(|v| v.to_string())
                .collect(),
            pub_keys: self.pub_keys.iter().map(|v| v.to_string()).collect(),
            sub_member_ids: self.sub_member_ids.clone(),
        }
    }

    pub fn convert_to_topic_partition_meta(&self, topic: &str) -> TopicPartitionMeta {
        convert_to_topic_partition_meta(
            topic,
            self.partition_id,
            self.broker_leader_id,
            &self.broker_leader_addr,
            &self.broker_follower_ids,
            &self.broker_follower_addrs,
            &self.pub_keys,
            &self.sub_member_ids,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionDetailSnapshot {
    pub topic: String,
    // 分区 id
    pub id: u32,
    // 该 partition 分布的 broekr
    pub broker_leader_id: u32,
    pub broker_leader_addr: String,
    pub broker_follower_ids: Vec<u32>,
    pub broker_follower_addrs: Vec<String>,

    // 该 partition 的其他标签
    // 发布时的 key
    pub pub_keys: Vec<String>,

    // 订阅时的 member_id
    pub sub_member_ids: Vec<String>,
}

impl From<TopicPartitionMeta> for TopicPartitionDetailSnapshot {
    fn from(v: TopicPartitionMeta) -> Self {
        TopicPartitionDetailSnapshot {
            topic: v.topic,
            id: v.partition_id,
            broker_leader_id: v.single_labels["broker_leader_id"].parse::<u32>().unwrap(),
            broker_leader_addr: v.single_labels["broker_leader_addr"].clone(),
            broker_follower_ids: v.multi_labels["broker_follower_ids"]
                .values
                .iter()
                .map(|v| v.parse::<u32>().unwrap())
                .collect(),
            broker_follower_addrs: v.multi_labels["broker_follower_addrs"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
            pub_keys: v.multi_labels["pub_keys"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
            sub_member_ids: v.multi_labels["sub_member_ids"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
        }
    }
}

impl From<&TopicPartitionMeta> for TopicPartitionDetailSnapshot {
    fn from(v: &TopicPartitionMeta) -> Self {
        TopicPartitionDetailSnapshot {
            topic: v.topic.clone(),
            id: v.partition_id,
            broker_leader_id: v.single_labels["broker_leader_id"].parse::<u32>().unwrap(),
            broker_leader_addr: v.single_labels["broker_leader_addr"].clone(),
            broker_follower_ids: v.multi_labels["broker_follower_ids"]
                .values
                .iter()
                .map(|v| v.parse::<u32>().unwrap())
                .collect(),
            broker_follower_addrs: v.multi_labels["broker_follower_addrs"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
            pub_keys: v.multi_labels["pub_keys"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
            sub_member_ids: v.multi_labels["sub_member_ids"]
                .values
                .iter()
                .map(|v| v.to_string())
                .collect(),
        }
    }
}

impl TopicPartitionDetailSnapshot {
    pub fn convert_to_topic_partition_meta(&self, topic: &str) -> TopicPartitionMeta {
        convert_to_topic_partition_meta(
            topic,
            self.id,
            self.broker_leader_id,
            &self.broker_leader_addr,
            &self.broker_follower_ids,
            &self.broker_follower_addrs,
            &self.pub_keys,
            &self.sub_member_ids,
        )
    }
}

pub struct TopicMeta {
    list: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
}

impl TopicMeta {
    pub fn snapshot(&self) -> Vec<TopicPartitionMeta> {
        let mut list = vec![];
        for tpms in self.list.iter() {
            for tpm in tpms.value().iter() {
                list.push(tpm.convert_to_topic_partition_meta(tpms.key()));
            }
        }
        list
    }
}

#[allow(clippy::too_many_arguments)]
fn convert_to_topic_partition_meta(
    topic: &str,
    partition_id: u32,
    broker_leader_id: u32,
    broker_leader_addr: &str,
    broker_follower_ids: &[u32],
    broker_follower_addrs: &[String],
    pub_keys: &[String],
    sub_member_ids: &[String],
) -> TopicPartitionMeta {
    TopicPartitionMeta {
        topic: topic.to_string(),
        partition_id,
        single_labels: HashMap::from([
            ("broker_leader_id".to_string(), broker_leader_id.to_string()),
            (
                "broker_leader_addr".to_string(),
                broker_leader_addr.to_string(),
            ),
        ]),
        multi_labels: HashMap::from([
            (
                "broker_follower_ids".to_string(),
                LabelValue {
                    values: broker_follower_ids.iter().map(|v| v.to_string()).collect(),
                },
            ),
            (
                "broker_follower_addr".to_string(),
                LabelValue {
                    values: broker_follower_addrs
                        .iter()
                        .map(|v| v.to_string())
                        .collect(),
                },
            ),
            (
                "pub_keys".to_string(),
                LabelValue {
                    values: pub_keys.iter().map(|v| v.to_string()).collect(),
                },
            ),
            (
                "sub_member_ids".to_string(),
                LabelValue {
                    values: sub_member_ids.iter().map(|v| v.to_string()).collect(),
                },
            ),
        ]),
    }
}
