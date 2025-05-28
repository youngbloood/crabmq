use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    // coordinator 节点id
    pub id: u32,

    // coordinator 节点监听的 grpc 地址
    pub coo_addr: String,

    // coordinator 节点间的 raft 通信模块监听的 grpc 地址
    pub raft_addr: String,

    // Db 存储路径
    pub db_path: PathBuf,

    // 检查自身是否是 Leader 的时间间隔，单位: s
    pub check_self_is_leader_interval: u64,

    // 创建新 topic 超时时间，单位: s
    pub new_topic_timeout: u64,

    // 新增 partition 超时时间，单位: s
    pub add_partition_timeout: u64,

    // event_bus 消息订阅缓冲区大小
    pub event_bus_buffer_size: usize,

    // 客户端调用 pull 时缓冲区大小
    pub client_pull_buffer_size: usize,

    // broker 调用 pull 时缓冲区大小
    pub broker_pull_buffer_size: usize,
}

impl Config {
    pub fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    pub fn with_coo_addr(mut self, coo_addr: String) -> Self {
        self.coo_addr = coo_addr;
        self
    }

    pub fn with_raft_addr(mut self, raft_addr: String) -> Self {
        self.raft_addr = raft_addr;
        self
    }

    pub fn with_db_path(mut self, db_path: PathBuf) -> Self {
        self.db_path = db_path;
        self
    }
}

pub fn default_config() -> Config {
    Config {
        id: 0,
        coo_addr: String::new(),
        raft_addr: String::new(),
        db_path: PathBuf::from("./data"),
        check_self_is_leader_interval: 2,
        new_topic_timeout: 3,
        add_partition_timeout: 3,
        event_bus_buffer_size: 12,
        client_pull_buffer_size: 12,
        broker_pull_buffer_size: 12,
    }
}
