use crate::partition::SinglePartition;
use anyhow::Result;
use raft::{
    eraftpb::ConfChange,
    prelude::{ConfChangeV2, Message},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub enum MessageType {
    RaftPropose(ProposeData),
    RaftConfChange(ConfChange),
    RaftConfChangeV2(ConfChangeV2),
    RaftMessage(Message),
}

#[derive(Serialize, Deserialize)]
pub struct TopicPartitionData {
    pub topic: SinglePartition,
    #[serde(skip)]
    pub callback: Option<mpsc::Sender<Result<String>>>,
}

pub enum ProposeData {
    // 分区布署详情
    TopicPartition(TopicPartitionData),
    // 消费者组信息详情
    ConsumerGroupDetail(),
}
