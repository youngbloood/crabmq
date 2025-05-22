use anyhow::{Result, anyhow};
use broker::Broker;
use coo::coo::Coordinator;
use logic_node::{LogicNode, Slave};
use std::{env::join_paths, net::SocketAddr, path::Path, sync::Arc};
use storagev2::mem;
use structopt::StructOpt;
use tokio::sync::Mutex;

#[derive(StructOpt)]
struct Args {
    /// The LoginNode id
    #[structopt(long, short = "id")]
    id: u32,

    /// Coordinator 模块自身监听的 grpc 地址
    #[structopt(short = "c", long = "coo", default_value = "")]
    coo: String,

    /// Coordinator-raft 模块自身监听的 grpc 地址
    #[structopt(long = "coo-raft", default_value = "")]
    coo_raft: String,

    /// 指定 raft 数据存储路径
    #[structopt(long = "db-path", default_value = "./data")]
    db_path: String,

    /// Indicate the Coordinator Raft Leader gprc address
    #[structopt(long = "raft-leader", default_value = "")]
    raft_leader: String,

    /// Broker 模块自身监听的 grpc 地址
    #[structopt(short = "b", default_value = "")]
    broker: String,

    /// 目标 Coo 地址，提供给本 Broker 模块获取自身信息，为空时默认为 .coo 值
    #[structopt(long = "coo-leader", default_value = "")]
    coo_leader: String,

    /// Slave 模块自身监听的 grpc 地址
    #[structopt(short = "s", default_value = "")]
    slave: String,
}

#[derive(StructOpt)]
struct CooArgs {}

impl Args {
    fn validate(&mut self) -> Result<()> {
        // 规则1: slave不为空时，broker和coo必须为空
        if !self.slave.is_empty() && (!self.broker.is_empty() || !self.coo.is_empty()) {
            return Err(anyhow!(
                "When slave is specified, broker and coo must be empty"
            ));
        }

        // 规则2: slave为空时，broker和coo至少一个非空
        if self.slave.is_empty() && self.broker.is_empty() && self.coo.is_empty() {
            return Err(anyhow!(
                "Either broker or coo must be specified when slave is empty"
            ));
        }

        // 规则3: coo_raft必须跟coo一致（同时有值或同时为空）
        if self.coo.is_empty() != self.coo_raft.is_empty() {
            return Err(anyhow!(
                "coo and coo_raft must both have values or both be empty"
            ));
        }

        // 规则4: 有 broker 模块，但是其 coo_leader 为空，默认为 .coo 值
        if !self.broker.is_empty() && self.coo_leader.is_empty() {
            self.coo_leader = self.coo.clone();
            if self.coo_leader.is_empty() {
                return Err(anyhow!("must specify the coo-leader address"));
            }
        }

        // 规则5: IP:PORT格式验证
        let validate_addr = |name: &str, addr: &str| -> Result<()> {
            if !addr.is_empty() {
                addr.parse::<SocketAddr>().map_err(|_| {
                    anyhow!(
                        "Invalid {} address format: {}, expected ip:port",
                        name,
                        addr
                    )
                })?;
            }
            Ok(())
        };

        validate_addr("coo", &self.coo)?;
        validate_addr("coo_raft", &self.coo_raft)?;
        validate_addr("raft_leader", &self.raft_leader)?;
        validate_addr("broker", &self.broker)?;
        validate_addr("coo_leader", &self.coo_leader)?;
        validate_addr("slave", &self.slave)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::from_args();
    args.validate()?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut builder = logic_node::Builder::new();
    if !args.coo.is_empty() {
        let db_path = Path::new(&args.db_path).join(format!("coo{}", args.id));
        let coo = Coordinator::new(
            args.id,
            db_path.as_os_str(),
            args.coo,
            args.coo_raft,
            args.raft_leader.clone(),
        );
        builder = builder.coo(coo);
    }
    if !args.broker.is_empty() {
        let store = mem::MemStorage::new();
        let broker = Broker::new(args.id, args.broker, args.coo_leader.clone(), store);
        builder = builder.broker(broker);
    }
    if !args.slave.is_empty() {
        builder = builder.slave(Slave);
    }
    builder
        .build()
        .run(Arc::new(Mutex::new(args.coo_leader)))
        .await?;
    Ok(())
}
