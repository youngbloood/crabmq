use anyhow::{Result, anyhow};
use broker::Broker;
use coo::coo::Coordinator;
use logic_node::{LogicNode, Slave};
use std::{env::join_paths, net::SocketAddr, path::Path};
use storagev2::mem;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Args {
    /// The LoginNode id
    #[structopt(long, short = "id")]
    id: u32,

    /// Coordinator listen grpc address
    #[structopt(short = "c", long = "coo", default_value = "")]
    coo: String,

    /// Coordinator raft interact the message
    #[structopt(long = "coo-raft", default_value = "")]
    coo_raft: String,

    /// Indicate the database path of the coordinator raft module
    #[structopt(long = "db-path", default_value = "./data")]
    db_path: String,

    /// Broker listen grpc address
    #[structopt(short = "b", default_value = "")]
    broker: String,

    /// Slave listen grpc address
    #[structopt(short = "s", default_value = "")]
    slave: String,

    /// Indicate the Coordinator Raft Leader gprc address
    #[structopt(long = "coo-leader", default_value = "")]
    coo_leader: String,
}

impl Args {
    fn validate(&self) -> Result<()> {
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

        // 规则3: IP:PORT格式验证
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

        validate_addr("broker", &self.broker)?;
        validate_addr("coo", &self.coo)?;
        validate_addr("coo_raft", &self.coo_raft)?;
        validate_addr("slave", &self.slave)?;
        validate_addr("coo_leader", &self.coo_leader)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args();
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
            args.coo_leader.clone(),
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
    builder.build().run(args.coo_leader).await?;
    Ok(())
}
