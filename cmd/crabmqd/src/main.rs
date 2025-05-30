use anyhow::{Result, anyhow};
use broker::Broker;
use coo::coo::Coordinator;
use logic_node::Slave;
use std::{net::SocketAddr, path::Path, sync::Arc};
use storagev2::disk::DiskStorageReader;
use storagev2::disk::DiskStorageWriter;
use storagev2::disk::default_config;
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
    coo: Option<String>,

    /// Coordinator raft listen grpc address, if it empty, will be equal to {.coo}
    #[structopt(long = "coo-raft")]
    coo_raft: Option<String>,

    /// Indicate the raft data storage path
    #[structopt(long = "db-path", default_value = "./data")]
    db_path: String,
}

#[derive(StructOpt, Debug)]
struct BrokerArgs {
    /// Broker listen grpc address
    #[structopt(short = "b")]
    broker: Option<String>,

    /// Indicate the Coordinator Raft Leader gprc address
    #[structopt(long = "raft-leader")]
    raft_leader: Option<String>,

    /// Indicate the coo-address to broker report state
    #[structopt(long = "coo-leader")]
    coo_leader: Option<String>,
}

#[derive(StructOpt, Debug)]
struct SlaveArgs {
    /// Slave listen grpc address
    #[structopt(short = "s")]
    slave: Option<String>,
}

impl Args {
    fn validate(&mut self) -> Result<()> {
        // 规则1: slave不为空时，broker和coo必须为空
        if self.slave_args.slave.is_some()
            && (self.broker_args.broker.is_some() || self.coo_args.coo.is_some())
        {
            return Err(anyhow!(
                "When slave is specified, broker and coo must be empty"
            ));
        }

        // 规则2: slave为空时，broker和coo至少一个非空
        if self.slave_args.slave.is_none()
            && self.broker_args.broker.is_none()
            && self.coo_args.coo.is_none()
        {
            return Err(anyhow!(
                "Either broker or coo must be specified when slave is empty"
            ));
        }

        // 规则3: .coo 和 .coo_raft 互补
        if self.coo_args.coo_raft.is_none() && self.coo_args.coo.is_some() {
            self.coo_args.coo_raft = self.coo_args.coo.clone();
        } else if self.coo_args.coo_raft.is_some() && self.coo_args.coo.is_none() {
            self.coo_args.coo = self.coo_args.coo_raft.clone();
        }
        if self.coo_args.coo.is_none() != self.coo_args.coo_raft.is_none() {
            return Err(anyhow!(
                "coo and coo_raft must both have values or both be empty"
            ));
        }

        // 规则4: 有 broker 模块, .coo_leader 和 .raft_leader 互补
        if self.broker_args.coo_leader.is_none() && self.broker_args.raft_leader.is_some() {
            self.broker_args.coo_leader = self.broker_args.raft_leader.clone();
        } else if self.broker_args.coo_leader.is_some() && self.broker_args.raft_leader.is_none() {
            self.broker_args.raft_leader = self.broker_args.coo_leader.clone();
        } else if self.broker_args.coo_leader.is_none() && self.broker_args.raft_leader.is_none() {
            // 若两者均为空，则取其 .coo_args.coo地址
            self.broker_args.raft_leader = self.coo_args.coo.clone();
            self.broker_args.coo_leader = self.coo_args.coo.clone();
        }

        // 规则5: IP:PORT格式验证
        let validate_addr = |name: &str, addr: Option<&str>| -> Result<()> {
            if addr.is_some() && !addr.as_ref().unwrap().is_empty() {
                addr.unwrap().parse::<SocketAddr>().map_err(|_| {
                    anyhow!(
                        "Invalid {} address format: {}, expected ip:port",
                        name,
                        addr.unwrap()
                    )
                })?;
            }
            Ok(())
        };

        validate_addr(".coo_args.coo", self.coo_args.coo.as_deref())?;
        validate_addr(".coo_args.coo_raft", self.coo_args.coo_raft.as_deref())?;
        validate_addr(".broker_args.broker", self.broker_args.broker.as_deref())?;
        validate_addr(
            ".broker_args.raft_leader",
            self.broker_args.raft_leader.as_deref(),
        )?;
        validate_addr(
            ".broker_args.coo_leader",
            self.broker_args.coo_leader.as_deref(),
        )?;
        validate_addr(".slave_args.slave", self.slave_args.slave.as_deref())?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::from_args();
    args.validate()?;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let mut builder = logic_node::Builder::new();

    if args.coo_args.coo.is_some() {
        let conf = coo::default_config()
            .with_id(args.id)
            .with_coo_addr(args.coo_args.coo.unwrap())
            .with_raft_addr(args.coo_args.coo_raft.unwrap())
            .with_db_path(Path::new(&args.coo_args.db_path).join(format!("coo{}", args.id)));
        conf.validate()?;
        let coo = Coordinator::new(args.broker_args.raft_leader.unwrap(), conf);
        builder = builder.coo(coo);
    }

    if args.broker_args.broker.is_some() {
        let conf = default_config();
        let broker = Broker::new(
            broker::default_config()
                .with_id(args.id)
                .with_broker_addr(args.broker_args.broker.unwrap()),
            DiskStorageWriter::new(conf.clone()),
            DiskStorageReader::new(conf.storage_dir),
        );
        builder = builder.broker(broker);
    }

    if args.slave_args.slave.is_some() {
        builder = builder.slave(Slave);
    }

    builder
        .build()
        .run(Arc::new(Mutex::new(args.broker_args.coo_leader.unwrap())))
        .await?;
    Ok(())
}
