use anyhow::{Result, anyhow};
use std::path::PathBuf;
use transporter::TransportProtocol;

#[derive(Clone)]
pub struct Config {
    // coordinator 节点id
    pub id: u32,

    // coo 配置
    pub coo: Base,
    // raftx 配置
    pub raftx_config: raftx::Config,
}

#[derive(Clone)]
pub struct Base {
    // coordinator 节点监听的地址
    pub addr: String,

    pub protocol: TransportProtocol,

    pub incoming_max_connections: usize,

    pub outgoing_max_connections: usize,

    // Db 存储路径
    pub db_path: PathBuf,

    // 检查自身是否是 Leader 的时间间隔，单位: s
    pub check_self_is_leader_interval: u64,

    // 创建新 topic 超时时间，单位: s
    pub new_topic_timeout: u64,

    // 创建 topic 时的分区因子，仅有两种类型，数字类型和类似“100n”类型。后者表示当前的broker每个分配100个。
    pub new_topic_partition_factor: String,
    // 创建 topic 时，默认的分区副本数量
    pub new_topic_partition_replication_count: u8,
    // 创建 topic 时，分区副本是否可读
    pub new_topic_partition_replication_readble: bool,

    // 新增 partition 超时时间，单位: s
    pub add_partition_timeout: u64,

    // event_bus 消息订阅缓冲区大小
    pub event_bus_buffer_size: usize,

    // 客户端调用 pull 时缓冲区大小
    pub client_pull_buffer_size: usize,
}

impl Config {
    pub fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    pub fn with_coo_addr(mut self, coo_addr: String) -> Self {
        self.coo.addr = coo_addr;
        self
    }

    pub fn with_raft_addr(mut self, raft_addr: String) -> Self {
        self.raftx_config.raft.addr = raft_addr;
        self
    }

    pub fn with_db_path(mut self, db_path: String) -> Self {
        self.raftx_config.db.path = db_path;
        self
    }

    pub fn validate(&self) -> Result<()> {
        let must_gt_zero = |attr, v| -> Result<()> {
            if v == 0 {
                return Err(anyhow!("'{attr}' must grate than zero"));
            }
            Ok(())
        };
        must_gt_zero("id", self.id)?;
        must_gt_zero(
            "check_self_is_leader_interval",
            self.coo.check_self_is_leader_interval as _,
        )?;
        must_gt_zero("new_topic_timeout", self.coo.new_topic_timeout as _)?;
        must_gt_zero("add_partition_timeout", self.coo.add_partition_timeout as _)?;
        must_gt_zero("event_bus_buffer_size", self.coo.event_bus_buffer_size as _)?;
        must_gt_zero(
            "client_pull_buffer_size",
            self.coo.client_pull_buffer_size as _,
        )?;
        // must_gt_zero(
        //     "broker_pull_buffer_size",
        //     self.coo.broker_pull_buffer_size as _,
        // )?;

        let must_not_empty = |attr, v: &str| -> Result<()> {
            if v.is_empty() {
                return Err(anyhow!("'{attr}' must not empty"));
            }
            Ok(())
        };
        must_not_empty("coo.addr", &self.coo.addr)?;
        must_not_empty("raftx_config.raft.addr", &self.raftx_config.raft.addr)?;

        // 检查是否以 n/N 结尾，并验证前面是数字
        if let Some(last) = self.coo.new_topic_partition_factor.chars().last() {
            if last == 'n' || last == 'N' {
                let num_part = &self.coo.new_topic_partition_factor
                    [..self.coo.new_topic_partition_factor.len() - 1];
                if !(!num_part.is_empty() && num_part.chars().all(|c| c.is_ascii_digit())) {
                    return Err(anyhow!("illigal new_topic_partition_factor"));
                }
            }
        } else {
            // 检查纯数字
            if !self
                .coo
                .new_topic_partition_factor
                .chars()
                .all(|c| c.is_ascii_digit())
            {
                return Err(anyhow!("illigal new_topic_partition_factor"));
            }
        }

        Ok(())
    }
}

pub fn default_config() -> Config {
    // Config {
    //     id: 0,
    //     coo_addr: String::new(),
    //     raft_addr: String::new(),
    //     db_path: PathBuf::from("./data"),
    //     check_self_is_leader_interval: 2,
    //     new_topic_timeout: 3,
    //     new_topic_partition_factor: "10n".to_string(),
    //     add_partition_timeout: 3,
    //     event_bus_buffer_size: 12,
    //     client_pull_buffer_size: 12,
    //     broker_pull_buffer_size: 12,
    //     new_topic_partition_replication_count: 3,
    //     new_topic_partition_replication_readble: true,
    // }
    todo!()
}
