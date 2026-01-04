#[derive(Clone, Copy)]
pub struct BrokerState {
    pub broker_id: u64,
    pub broker_addr: String,
    pub version: String,

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

    // 该 broker 当前状态
    pub status: BrokerStatus,
}

pub enum BrokerStatus {
    Online,
    Offline,
    Timeout,
}
