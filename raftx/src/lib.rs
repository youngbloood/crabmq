mod mailbox;
mod node;
mod peer;
mod storage;

use std::collections::HashMap;
use tokio::sync::mpsc;
use transporter::TransportProtocol;

use anyhow::Result;
pub use node::Node;

pub trait StateApply: Send + Sync + 'static {
    fn apply(&self, v: u8, message: &[u8]) -> Result<()>;
}

#[derive(Clone)]
pub struct Config {
    // 节点 id
    pub id: u32,

    pub mailbox_buffer_len: usize,
    //  raft config
    pub raft: RaftConfig,
    // db config
    pub db: DBConfig,
}

#[derive(Clone)]
pub struct RaftConfig {
    pub addr: String,
    pub write_timeout_milli: u64,
    pub protocol: TransportProtocol,
    pub meta: HashMap<String, String>,

    pub election_tick: u64,
    pub heartbeat_tick: u64,
    pub applied: u64,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: u64,
}

#[derive(Clone)]
pub struct DBConfig {
    pub path: String,
    pub max_size: u64,
}

pub type Callback = mpsc::Sender<Result<String>>;
