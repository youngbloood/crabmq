// mod grpc_service;
mod mailbox_message_type;
mod peer;
pub mod raw_node;
mod storage;

pub use mailbox_message_type::*;
pub use raw_node::*;
use sled::Db;

use crate::partition::SinglePartition;
use anyhow::Result;

pub trait PartitionApply {
    fn apply(&self, part: SinglePartition) -> Result<()>;
    fn get_db(&self) -> Db;
}
