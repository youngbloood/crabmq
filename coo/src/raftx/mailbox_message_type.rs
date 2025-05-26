use crate::partition::SinglePartition;
use anyhow::Result;
use raft::{
    eraftpb::ConfChange,
    prelude::{ConfChangeV2, Message},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub enum MessageType {
    RaftPropose(ProproseData),
    RaftConfChange(ConfChange),
    RaftConfChangeV2(ConfChangeV2),
    RaftMessage(Message),
}

#[derive(Serialize, Deserialize)]
pub struct ProproseData {
    pub topic: SinglePartition,
    #[serde(skip)]
    pub callback: Option<mpsc::Sender<Result<String>>>,
}
