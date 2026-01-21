mod mailbox;
mod node;
mod peer;
mod storage;

use std::collections::HashMap;

use transporter::TransportProtocol;

use crate::storage::DbConfig;

pub struct Config {
    // 节点 id
    pub id: u32,

    pub mailbox_buffer_len: usize,
    //  raft config
    pub raft: RaftConfig,
    // db config
    pub db: DBConfig,
}

pub struct RaftConfig {
    pub addr: String,
    pub write_timeout_milli: u64,
    pub protocol: TransportProtocol,
    pub meta: HashMap<String, String>,
    pub db_conf: DbConfig,

    pub election_tick: u64,
    pub heartbeat_tick: u64,
    pub applied: u64,
    pub max_size_per_msg: u64,
    pub max_inflight_msgs: u64,
}

pub struct DBConfig {
    pub path: String,
    pub max_size: u64,
}
