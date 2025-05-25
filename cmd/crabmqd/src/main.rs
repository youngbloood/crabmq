use anyhow::{Result, anyhow};
use broker::Broker;
use coo::coo::Coordinator;
use logic_node::{LogicNode, Slave};
use std::{env::join_paths, net::SocketAddr, path::Path, sync::Arc};
use storagev2::mem;
use structopt::StructOpt;
use tokio::sync::Mutex;

#[derive(StructOpt, Debug)]
struct Args {
    /// The LoginNode id
    #[structopt(long, short = "id")]
    id: u32,

    #[structopt(flatten)]
    coo_args: CooArgs,

    #[structopt(flatten)]
    broker_args: BrokerArgs,

    #[structopt(flatten)]
    slave_args: SlaveArgs,
}

#[derive(StructOpt, Debug)]
struct CooArgs {
    /// Coordinator listen grpc address
    #[structopt(short = "c", long = "coo")]
    coo: String,

    /// Coordinator raft listen grpc address, if it empty, will be equal to {.coo}
    #[structopt(long = "coo-raft")]
    coo_raft: String,

    /// Indicate the raft data storage path
    #[structopt(long = "db-path", default_value = "./data")]
    db_path: String,
}

#[derive(StructOpt, Debug)]
struct BrokerArgs {
    /// Broker listen grpc address
    #[structopt(short = "b")]
    broker: String,

    /// Indicate the Coordinator Raft Leader gprc address
    #[structopt(long = "raft-leader")]
    raft_leader: String,

    /// Indicate the coo-address to broker report state
    #[structopt(long = "coo-leader")]
    coo_leader: String,
}

#[derive(StructOpt, Debug)]
struct SlaveArgs {
    /// Slave listen grpc address
    #[structopt(short = "s")]
    slave: String,
}

impl Args {
    fn validate(&mut self) -> Result<()> {
        // 规则1: slave不为空时，broker和coo必须为空
        if !self.slave_args.slave.is_empty()
            && (!self.broker_args.broker.is_empty() || !self.coo_args.coo.is_empty())
        {
            return Err(anyhow!(
                "When slave is specified, broker and coo must be empty"
            ));
        }

        // 规则2: slave为空时，broker和coo至少一个非空
        if self.slave_args.slave.is_empty()
            && self.broker_args.broker.is_empty()
            && self.coo_args.coo.is_empty()
        {
            return Err(anyhow!(
                "Either broker or coo must be specified when slave is empty"
            ));
        }

        // 规则3: .coo 和 .coo_raft 互补
        if self.coo_args.coo_raft.is_empty() && !self.coo_args.coo.is_empty() {
            self.coo_args.coo_raft = self.coo_args.coo.clone();
        } else if !self.coo_args.coo_raft.is_empty() && self.coo_args.coo.is_empty() {
            self.coo_args.coo = self.coo_args.coo_raft.clone();
        }
        if self.coo_args.coo.is_empty() != self.coo_args.coo_raft.is_empty() {
            return Err(anyhow!(
                "coo and coo_raft must both have values or both be empty"
            ));
        }

        // 规则4: 有 broker 模块, .coo_leader 和 .raft_leader 互补
        if self.broker_args.coo_leader.is_empty() && !self.broker_args.raft_leader.is_empty() {
            self.broker_args.coo_leader = self.broker_args.raft_leader.clone();
        } else if !self.broker_args.coo_leader.is_empty() && self.broker_args.raft_leader.is_empty()
        {
            self.broker_args.raft_leader = self.broker_args.coo_leader.clone();
        } else if self.broker_args.coo_leader.is_empty() && self.broker_args.raft_leader.is_empty()
        {
            // 若两者均为空，则取其 .coo_args.coo地址
            self.broker_args.raft_leader = self.coo_args.coo.clone();
            self.broker_args.coo_leader = self.coo_args.coo.clone();
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

        validate_addr(".coo_args.coo", &self.coo_args.coo)?;
        validate_addr(".coo_args.coo_raft", &self.coo_args.coo_raft)?;
        validate_addr(".broker_args.broker", &self.broker_args.broker)?;
        validate_addr(".broker_args.raft_leader", &self.broker_args.raft_leader)?;
        validate_addr(".broker_args.coo_leader", &self.broker_args.coo_leader)?;
        validate_addr(".slave_args.slave", &self.slave_args.slave)?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::from_args();
    args.validate()?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut builder = logic_node::Builder::new();
    if !args.coo_args.coo.is_empty() {
        let db_path = Path::new(&args.coo_args.db_path).join(format!("coo{}", args.id));
        let coo = Coordinator::new(
            args.id,
            db_path.as_os_str(),
            args.coo_args.coo.clone(),
            args.coo_args.coo_raft.clone(),
            args.broker_args.raft_leader.clone(),
        );
        builder = builder.coo(coo);
    }
    if !args.broker_args.broker.is_empty() {
        let store = mem::MemStorage::new();
        let broker = Broker::new(args.id, args.broker_args.broker.clone(), store);
        builder = builder.broker(broker);
    }
    if !args.slave_args.slave.is_empty() {
        builder = builder.slave(Slave);
    }

    println!("args ======= {:?}", args);
    builder
        .build()
        .run(Arc::new(Mutex::new(args.broker_args.coo_leader)))
        .await?;
    Ok(())
}
