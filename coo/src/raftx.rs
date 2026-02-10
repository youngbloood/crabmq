use crate::partition::SinglePartition;
use raftx::Callback;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProposeData {
    TopicPartition(TopicPartitionData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPartitionData {
    pub topic: SinglePartition,
    #[serde(skip)]
    pub callback: Option<Callback>,
}
