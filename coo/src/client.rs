use std::{collections::HashMap, sync::Arc};

use transporter::{Transporter, TransporterWriter};

#[derive(Clone)]
pub struct Client {
    // base info
    pub id: u64,
    pub addr: String,
    pub meta: HashMap<String, Vec<u8>>,
    pub version: String,

    // 该 client 运行状态
    state: State,

    // 该 broker 当前状态
    pub status: Status,

    w: Arc<TransporterWriter>,
}

#[derive(Clone)]
struct State {
    // 该 broker 网络速率
    pub netrate: u32,
    // 该 broker 的 cpu 占用率
    pub cpurate: u32,
    // 该 broker 的内存占用率
    pub memrate: u32,
    // 该 broker 的磁盘占用率
    pub diskrate: u32,

    // 该 broker 的订阅连接数
    pub sub_count: u32,
    // 该 broker 的发布连接数
    pub pub_count: u32,
}

#[derive(Clone)]
pub enum Status {
    Online,
    Offline,
    Timeout,
}

impl Client {
    pub fn new(
        id: u64,
        addr: String,
        meta: HashMap<String, Vec<u8>>,
        version: String,
        w: TransporterWriter,
    ) -> Self {
        Self {
            id,
            addr,
            meta,
            version,

            state: State {
                netrate: 0,
                cpurate: 0,
                memrate: 0,
                diskrate: 0,
                sub_count: 0,
                pub_count: 0,
            },
            status: Status::Offline,
            w: Arc::new(w),
        }
    }

    pub fn update_state(&mut self, state: State) {
        self.state = state;
    }
}
