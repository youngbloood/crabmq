use tokio::sync::mpsc;

use crate::coo::Coordinator;

pub struct CoordinatorService {
    // raft_node 节点
    raft_node: Arc<RaftNode<PartitionManager>>,

    t: Transporter,

    coo: Arc<Coordinator>,

    shutdown: CancelToken,
}

impl CoordinatorService {
    pub fn new(
        raft_leader_addr: String, /* 用于后续的 raft 节点 join 之前的集群中，为空时表示自己为当前集群的第一个节点 */
        conf: CooConfig,
    ) -> Result<Self> {
        conf.validate()?;

        let (raft_node, raft_node_sender) = RaftNode::new(conf.raftx_config);
        let raft_node = Arc::new(raft_node);
        Ok(Self {
            raft_node,
            t: Transporter::new(transporter::Config {
                addr: conf.coo.addr.clone(),
                protocol: conf.coo.protocol.clone(),
                incoming_max_connections: conf.coo.incoming_max_connections,
                outgoing_max_connections: conf.coo.outgoing_max_connections,
            })?,
            coo: Coordinator::new(raft_leader_addr, conf, raft_node_sender),
            shutdown: todo!(),
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
