mod broker_status;
mod command;
pub mod config;
mod conn;
mod consumer_group;
pub mod coo;
mod event_bus;
mod partition;
mod raftx;

// mod topic_meta;
use std::collections::HashMap;

pub use config::*;
use grpcx::{brokercoosvc, commonsvc::TopicPartitionMeta};
struct BrokerNode {
    state: brokercoosvc::BrokerState,
}

struct ClientNode {}

pub trait Filter {}

#[cfg(test)]
mod test {}
