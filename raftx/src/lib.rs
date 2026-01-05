// mod grpc_service;
mod mailbox_message_type;
mod peer;
pub mod raw_node;
mod storage;

use sled::Db;

pub use mailbox_message_type::*;
pub use raw_node::*;

use crate::partition::SinglePartition;
use anyhow::Result;

pub trait PartitionApply {
    fn apply(&self, part: SinglePartition) -> Result<()>;
    fn get_db(&self) -> Db;
}

pub struct Config {
    // raft 节点 id
    pub id: u64,
    // raft 节点监听 addr
    pub raft_addr: String,
    // raft 监听协议
    pub protocol: ProtocolType,
}

pub enum ProtocolType {
    TCP,
    QUIC,
    UDP,
}
