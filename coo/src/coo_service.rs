use log::error;
use tokio::select;
use tokio::sync::mpsc;

use crate::config::Config as CooConfig;
use crate::coo::Coordinator;
use crate::partition::{PartitionManager, PartitionPolicy};
use anyhow::Result;
use raftx::Node as RaftNode;
use std::sync::Arc;
use transporter::{TransportMessage, service::TransporterServiceManager};

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
            TransporterServiceManager::new(transporter::service::TransporterServiceConfig {
                addr: conf.coo.for_broker_addr.clone(),
                protocol: conf.coo.protocol,
                incoming_max_connections: conf.coo.incoming_max_connections,
                max_frame_body_size: todo!(),
                send_timeout: todo!(),
                idle_timeout: todo!(),
                send_message_buffer_size: todo!(),
            });

        let client_trans_service =
            TransporterServiceManager::new(transporter::service::TransporterServiceConfig {
                addr: conf.coo.for_client_addr.clone(),
                protocol: conf.coo.protocol,
                incoming_max_connections: conf.coo.incoming_max_connections,
                max_frame_body_size: todo!(),
                send_timeout: todo!(),
                idle_timeout: todo!(),
                send_message_buffer_size: todo!(),
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
    pub async fn run(&self) -> Result<mpsc::Sender<TransportMessage>> {
        let (tx, rx) = mpsc::channel(1024);

        // 启动接收服务，将消息写入 tx
        self.raft_node.run(tx.clone()).await?;
        self.broker_trans_service.run(tx.clone()).await?;
        self.client_trans_service.run(tx.clone()).await?;

        // 消费消息并处理
        self.loop_handle_command(rx).await;
        Ok(tx)
    }

    async fn loop_handle_command(&self, mut rx: mpsc::Receiver<TransportMessage>) {
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

    pub async fn is_leader(&self) -> bool {
        self.raft_node.is_leader().await
    }
}
