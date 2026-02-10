use tokio::sync::mpsc;

use crate::coo::Coordinator;
use crate::coo::config::CooConfig;
use crate::partition::{PartitionManager, PartitionPolicy};
use anyhow::Result;
use raftx::Node as RaftNode;
use std::sync::Arc;
use transporter::{TransportMessage, Transporter};

pub struct CoordinatorService {
    coo: Arc<Coordinator>,
    trans: Transporter,
    raft_node: Arc<RaftNode<PartitionManager>>,
}

impl CoordinatorService {
    pub fn new(conf: CooConfig) -> Result<Self> {
        let partition_manager = Arc::new(PartitionManager::new(
            PartitionPolicy::default(),
            conf.db_path.clone(),
        ));

        let (raft_node, raft_node_sender) =
            RaftNode::new(conf.raftx_config.clone(), partition_manager.clone());
        let raft_node = Arc::new(raft_node);

        let trans = Transporter::new(conf.transporter_config.clone());
        let coo = Arc::new(Coordinator::new(
            conf,
            raft_node_sender,
            raft_node.clone(),
            partition_manager,
        )?);

        Ok(Self {
            coo,
            trans,
            raft_node,
        })
    }

    /// 启动 Coordinator 服务
    pub async fn run(&self) -> Result<()> {
        let (tx, rx) = mpsc::unbounded_channel();

        self.raft_node
            .run(async move |m| {
                if let Err(e) = tx.send(m).await {
                    error!("RaftNode upload message error: {}", e);
                }
            })
            .await?;
        self.t.start().await?;
        self.loop_handle_command(rx);
        Ok(())
    }

    fn loop_handle_command(&self, rx: mpsc::UnboundedReceiver<RaftMessage>) {
        select! {
            Some(msg) = rx.recv() => {
                let coo = self.coo.clone();
                tokio::spawn(async move {
                    coo.handle_raft_message(msg).await;
                });
            }


        }
        let mut coo = self.coo.clone();
        tokio::spawn(async move {
            loop {
                let cmd = coo.get_command().await;
                if let Ok(cmd) = cmd {
                    coo.handle_command(cmd).await;
                }
            }
        });
    }
}
