use log::error;
use tokio::select;
use tokio::sync::mpsc;

use crate::config::Config as CooConfig;
use crate::coo::Coordinator;
use crate::partition::{PartitionManager, PartitionPolicy};
use anyhow::Result;
use raftx::Node as RaftNode;
use std::sync::Arc;
use transporter::{TransportMessage, TransporterServiceManager};

pub struct CoordinatorService {
    coo: Arc<Coordinator>,
    broker_trans_service: TransporterServiceManager,
    client_trans_service: TransporterServiceManager,
    raft_node: Arc<RaftNode<PartitionManager>>,
}

impl CoordinatorService {
    pub fn new(conf: CooConfig) -> Result<Self> {
        let partition_manager = PartitionManager::new(
            PartitionPolicy::default(),
            conf.raftx_config.db.path.clone(),
        );

        let (raft_node, raft_node_sender) =
            RaftNode::new(conf.raftx_config.clone(), partition_manager.clone());
        let raft_node = Arc::new(raft_node);

        let broker_trans_service =
            TransporterServiceManager::new(transporter::TransporterServiceConfig {
                addr: conf.coo.for_broker_addr.clone(),
                protocol: conf.coo.protocol,
                incoming_max_connections: conf.coo.incoming_max_connections,
            });

        let client_trans_service =
            TransporterServiceManager::new(transporter::TransporterServiceConfig {
                addr: conf.coo.for_client_addr.clone(),
                protocol: conf.coo.protocol,
                incoming_max_connections: conf.coo.incoming_max_connections,
            });

        let coo = Arc::new(Coordinator::new(
            conf.clone(),
            raft_node_sender,
            partition_manager,
        )?);

        Ok(Self {
            coo,
            broker_trans_service,
            client_trans_service,
            raft_node,
        })
    }

    /// 启动 Coordinator 服务
    pub async fn run(&self) -> Result<()> {
        self.run_broker_trans_service().await?;
        self.run_client_trans_service().await?;
        self.run_raft_node().await?;
        Ok(())
    }

    async fn run_raft_node(&self) -> Result<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.raft_node
            .run(async move |m| {
                if let Err(e) = tx.send(m) {
                    error!("RaftNode upload message error: {}", e);
                }
            })
            .await?;

        self.loop_handle_command(rx).await;
        Ok(())
    }

    async fn run_broker_trans_service(&self) -> Result<()> {
        self.broker_trans_service.run().await?;
        let mut trans = self.broker_trans_service.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    msg = trans.recv(None) => {
                        if msg.is_none() {
                            continue;
                        }
                        let msg = msg.unwrap();
                        // 处理接收到的命令
                        todo!();
                    }
                }
            }
        });
        Ok(())
    }

    async fn run_client_trans_service(&self) -> Result<()> {
        self.client_trans_service.run().await?;
        let mut trans = self.client_trans_service.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    msg = trans.recv(None) => {
                        if msg.is_none() {
                            continue;
                        }
                        let msg = msg.unwrap();
                        // 处理接收到的命令
                        todo!();
                    }
                }
            }
        });
        Ok(())
    }

    async fn loop_handle_command(&self, mut rx: mpsc::UnboundedReceiver<TransportMessage>) {
        loop {
            select! {
                    Some(msg) = rx.recv() => {
                        let coo = self.coo.clone();
                        tokio::spawn(async move {
                            if let Err(e) =  coo.handle_raft_message(msg).await{
                                error!("handle raft message error: {}", e);
                            }
                        });
                    }
            }
        }
    }
}
