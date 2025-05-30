use super::PartitionApply;
use super::mailbox_message_type::MessageType as AllMessageType;
use super::storage::SledStorage;
use super::{
    // grpc_service::{RaftServiceImpl, start_grpc_server},
    peer::PeerState,
};
use crate::partition::SinglePartition;
use crate::raftx::ProposeData;
use anyhow::{Result, anyhow};
use bincode::config;
use dashmap::DashMap;
use grpcx::cooraftsvc::{self, ConfChangeReq, RaftMessage, raft_service_client::RaftServiceClient};
use grpcx::smart_client::repair_addr_with_http;
use log::{debug, error, info, trace, warn};
use protobuf::Message as PbMessage;
use raft::{Config, StateRole, prelude::*, raw_node::RawNode};
use std::{num::NonZero, sync::Arc, time::Instant};
use tokio::{
    select,
    sync::{Mutex, mpsc, oneshot},
    time::{self, Duration, interval},
};
use tonic::{Request, transport::Channel};

#[derive(Clone)]
pub struct RaftNode<P: PartitionApply> {
    pub id: u32,
    // 该 raft 节点监听地址
    pub raft_grpc_addr: String,
    // 该 coo 节点监听地址
    pub coo_grpc_addr: String,
    // raft node
    pub raw_node: Arc<Mutex<RawNode<SledStorage>>>,

    // 本节点收到的 信息
    my_mailbox: Arc<Mutex<mpsc::Receiver<AllMessageType>>>,
    // 将消息分发到所有的 mailboxes 中
    mailboxes: Arc<DashMap<u64, mpsc::Sender<RaftMessage>>>,
    // 集群中其他节点的信息(包含自身)
    peer: Arc<DashMap<u64, Arc<PeerState>>>,

    db: P, // Key-value stor

    callbacks: Arc<DashMap<String, Callback>>,
}

pub type Callback = mpsc::Sender<Result<String>>;

// unsafe impl Send for RaftNode {}
// unsafe impl Sync for RaftNode {}

impl<P> RaftNode<P>
where
    P: PartitionApply,
{
    pub fn new(
        id: u32,
        raft_grpc_addr: String,
        coo_grpc_addr: String,
        p: P,
    ) -> (Self, mpsc::Sender<AllMessageType>) {
        let config = Config {
            id: id as u64,
            election_tick: 10,
            heartbeat_tick: 3,
            applied: 0,
            max_size_per_msg: 4096,
            max_inflight_msgs: 256,
            // pre_vote: true,
            ..Default::default()
        };
        // let conf_state = if join {
        //     // 加入集群的节点，初始化为空 ConfState
        //     ConfState {
        //         voters: vec![],
        //         learners: vec![],
        //         voters_outgoing: vec![],
        //         learners_next: vec![],
        //         auto_leave: false,
        //         ..Default::default()
        //     }
        // } else {
        //     // 独立节点，包含自身
        //     ConfState {
        //         voters: vec![id],
        //         learners: vec![],
        //         voters_outgoing: vec![],
        //         learners_next: vec![],
        //         auto_leave: false,
        //         ..Default::default()
        //     }
        // };
        // let conf_state = ConfState {
        //     voters: vec![id],
        //     learners: vec![],
        //     voters_outgoing: vec![],
        //     learners_next: vec![],
        //     auto_leave: false,
        //     ..Default::default()
        // };

        let storage = SledStorage::new(id as u64, p.get_db());
        let raw_node = Arc::new(Mutex::new(
            RawNode::with_default_logger(&config, storage).expect("Failed to create Raft node"),
        ));

        // Start Tonic gRPC server
        let (tx_grpc, rx_grpc) = mpsc::channel(128);

        // let raft_service = RaftServiceImpl::new(id, raw_node.clone(), tx_grpc.clone());
        // 转换为 String
        // tokio::spawn(start_grpc_server(grpc_addr.clone(), raft_service));

        let rn = RaftNode {
            id,
            raw_node,
            my_mailbox: Arc::new(Mutex::new(rx_grpc)),
            mailboxes: Arc::new(DashMap::new()),
            peer: Arc::new(DashMap::new()),
            db: p,
            raft_grpc_addr: raft_grpc_addr.clone(),
            coo_grpc_addr: coo_grpc_addr.clone(),
            callbacks: Arc::new(DashMap::new()),
        };
        rn.peer.insert(
            id.into(),
            Arc::new(PeerState::new(id.into(), raft_grpc_addr, coo_grpc_addr)),
        );

        (rn, tx_grpc)
    }

    #[inline]
    pub fn get_id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub async fn get_leader_id(&self) -> u32 {
        self.raw_node.lock().await.raft.leader_id as u32
    }

    #[inline]
    pub fn get_peer(&self) -> Vec<Arc<PeerState>> {
        let mut list = vec![];
        self.peer.iter().for_each(|v| {
            list.push(v.value().clone());
        });
        list
    }

    pub async fn get_term(&self) -> u64 {
        let raw_node = self.raw_node.lock().await;
        raw_node.raft.term
    }

    pub async fn get_not_leader_err_status(&self, message: String) -> tonic::Status {
        let coo_leader_addr = self
            .get_coo_leader_addr()
            .await
            .unwrap_or_else(|| "unknown".to_string());

        let mut status = tonic::Status::new(tonic::Code::PermissionDenied, message);

        // 添加 leader 地址到 metadata
        status
            .metadata_mut()
            .insert("x-raft-leader", coo_leader_addr.parse().unwrap());
        status
    }

    pub async fn run(&self) {
        let mut interval = time::interval(Duration::from_millis(100));
        let mut print_interval = Instant::now();
        let mut is_initial_conf_committed = false;
        loop {
            if self.is_leader().await && !is_initial_conf_committed {
                self.commit_self_conf_change().await;
                is_initial_conf_committed = true;
            }
            select! {
                msg = async {
                    let mut my_mailbox = self.my_mailbox.lock().await;
                    my_mailbox.recv().await
                } => {
                    if msg.is_none(){
                        continue;
                    }
                    let msg = msg.unwrap();
                    match msg {
                        AllMessageType::RaftMessage(msg) => {
                            debug!(
                                "Node[{}] 收到消息From[{}]->To[{}], 类型: {:?}",
                                self.id,
                                msg.from,
                                msg.to,
                                msg.get_msg_type()
                            );
                            let mut raw_node = self.raw_node.lock().await;
                            raw_node.step(msg).unwrap();
                        }

                        AllMessageType::RaftConfChange(cc) => {
                            if !self.is_leader().await {
                                continue;
                            }

                            let context_str = String::from_utf8(cc.context.to_vec()).unwrap();
                            let remote_grpc_addr: Vec<_> =context_str
                                .splitn(2, ",")
                                .collect();
                            info!(
                                "Node[{}] 收到：{}:{} 的ConfChange = {:?}",
                                self.id, remote_grpc_addr[0],remote_grpc_addr[1], &cc
                            );
                            self.add_endpoint(
                                cc.node_id,
                                remote_grpc_addr[0].to_string(),
                                remote_grpc_addr[1].to_string(),
                                false,
                                false)
                                .await
                                .unwrap();

                            let mut raw_node = self.raw_node.lock().await;
                            // if raw_node.raft.has_pending_conf() {
                            //     error!(
                            //         "Node[{}] ignoring ConfChange due to unapplied changes",
                            //         self.id
                            //     );
                            //     continue;
                            // }
                            raw_node
                                .propose_conf_change(cc.context.to_vec(), cc.clone())
                                .unwrap();

                            // self.handle_all_ready("ConfChange").await;
                        }

                        AllMessageType::RaftConfChangeV2(cc) => {
                            if !self.is_leader().await {
                                continue;
                            }

                            let context_str = String::from_utf8(cc.context.to_vec()).unwrap();
                            let remote_grpc_addr: Vec<_> = context_str
                                .splitn(2, ",")
                                .collect();
                            for cc in &cc.changes {
                                info!(
                                    "Node[{}] 收到：[raft{}:coo{}] 的ConfChangeV2",
                                    self.id, remote_grpc_addr[0], remote_grpc_addr[1]
                                );
                                self.add_endpoint(
                                    cc.node_id,
                                    remote_grpc_addr[0].to_string(),
                                    remote_grpc_addr[1].to_string(),
                                    false,
                                    false)
                                    .await
                                    .unwrap();
                            }

                            let mut raw_node = self.raw_node.lock().await;
                            if raw_node.raft.has_pending_conf() {
                                error!(
                                    "Node[{}] ignoring ConfChangeV2 due to unapplied changes",
                                    self.id
                                );
                                continue;
                            }
                            raw_node
                                .propose_conf_change(cc.context.to_vec(), cc)
                                .unwrap();
                        }

                        AllMessageType::RaftPropose(pp) => {
                            match pp{
                                ProposeData::TopicPartition(tp) => {
                                    let _ = self.propose(&tp.topic, tp.callback).await;
                                },
                                ProposeData::ConsumerGroupDetail() => todo!(),
                            }

                        }
                    }
                }

                _ = interval.tick() => {
                    {
                        let mut raw_node = self.raw_node.lock().await;
                        raw_node.tick();
                        if print_interval.elapsed() > Duration::from_secs(5) {
                            info!(
                                "Node[{}] term = {}, leader_id = {}, role = {:?}, raft.pr().conf() = {:?}, peer = {:?}",
                                self.id,
                                raw_node.raft.term,
                                raw_node.raft.leader_id,
                                raw_node.raft.state,
                                raw_node.raft.prs().conf(),
                                self.peer,
                            );
                            print_interval = Instant::now();
                        }
                    }
                    // self.handle_all_ready("tick").await;
                }
            }
            self.handle_all_ready().await;
        }
    }

    // ref: https://docs.rs/raft/0.7.0/raft/#processing-the-ready-state
    async fn handle_all_ready(&self) {
        let result = 'ready_block: {
            let mut raw_node = self.raw_node.lock().await;
            if !raw_node.has_ready() {
                break 'ready_block (None, raw_node.raft.raft_log.store.clone());
            }
            (Some(raw_node.ready()), raw_node.raft.raft_log.store.clone())
        };
        if result.0.is_none() {
            return;
        }
        let store = result.1;
        let mut ready = result.0.unwrap();

        // 1. handle messages
        self.handle_messages(ready.take_messages()).await;
        // 2. handle snapshot
        self.handle_snapshot(ready.snapshot(), &store).await;
        // 3. handle committed entries
        self.handle_entries(ready.take_committed_entries(), &store)
            .await;
        // 4. handle entries
        if let Err(e) = store.append(&ready.take_entries()) {
            error!("persist raft log fail: {:?}, need to retry or panic", e);
            return;
        }
        // 5. handle HardState
        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let _ = store.set_hard_state(hs);
        }
        // 6. handle persisted messages
        self.handle_messages(ready.take_persisted_messages()).await;

        let mut light_rd = {
            let mut raw_node = self.raw_node.lock().await;
            // let store = raw_node.mut_store().rl().append(ents);
            // 7. handle advance
            raw_node.advance(ready)
        };
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            let _ = store.set_hard_state_commit(commit);
        }

        self.handle_messages(light_rd.take_messages()).await;
        self.handle_entries(light_rd.take_committed_entries(), &store)
            .await;

        let mut raw_node = self.raw_node.lock().await;
        raw_node.advance_apply();
        debug!("Node[{}], advance_apply", self.id);
        if raw_node.raft.state == StateRole::Leader {
            debug!("Node {} is leader", self.id);
        }
    }

    async fn handle_messages(&self, messages: Vec<Message>) {
        for msg in messages {
            debug!(
                "Node[{}] to Node[{}], type = {:?}",
                msg.from,
                msg.to,
                &msg.get_msg_type(),
            );
            if let Some(sender) = self.mailboxes.get(&msg.to) {
                if let Err(e) = sender
                    .send(RaftMessage {
                        message: msg.write_to_bytes().unwrap(),
                    })
                    .await
                {
                    error!(
                        "Node[{}] to Node[{}] msg Type[{:?}] send failed: {:?}",
                        msg.from,
                        msg.to,
                        &msg.get_msg_type(),
                        e
                    );
                }
            }
        }
    }

    async fn handle_snapshot(&self, snapshot: &Snapshot, store: &SledStorage) {
        if *snapshot != Snapshot::default() {
            let s = snapshot.clone();
            if let Err(e) = store.apply_snapshot(&s) {
                error!("apply snapshot fail: {:?}, need to retry or panic", e);
            }
        }
    }

    async fn handle_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
        debug!("Node[{}]: handle_entries", self.id);
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let data = &entry.data;

                    let part: SinglePartition =
                        bincode::serde::decode_from_slice(data, config::standard())
                            .unwrap()
                            .0;
                    let unique_id = part.unique_id.clone();
                    self.db.apply(part);
                    if let Some((unique_id, cb)) = self.callbacks.remove(&unique_id) {
                        let _ = cb.send(Ok(unique_id)).await;
                    }
                }

                EntryType::EntryConfChange => {
                    debug!("Node[{}] 收到 EntryConfChange", self.id);
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();

                    // 确保 follower 收到该类型消息时增加 endpoint
                    let context_str = String::from_utf8(cc.context.to_vec()).unwrap();
                    let remote_grpc_addr: Vec<_> = context_str.splitn(2, ",").collect();
                    self.add_endpoint(
                        cc.node_id,
                        remote_grpc_addr[0].to_string(),
                        remote_grpc_addr[1].to_string(),
                        false,
                        false,
                    )
                    .await
                    .unwrap();

                    let mut raw_node = self.raw_node.lock().await;
                    let cs = raw_node.apply_conf_change(&cc).unwrap();
                    info!("Node[{}] ConfChange applied: {:?}", self.id, cs);
                    let _ = store.set_conf_state(&cs);
                }

                EntryType::EntryConfChangeV2 => {
                    debug!("Node[{}] 收到 EntryConfChangeV2", self.id);
                    let mut ccv2 = ConfChangeV2::default();
                    ccv2.merge_from_bytes(&entry.data).unwrap();

                    // 确保 follower 收到该类型消息时增加 endpoint
                    for cc in &ccv2.changes {
                        let context_str = String::from_utf8(ccv2.context.to_vec()).unwrap();
                        let remote_grpc_addr: Vec<_> = context_str.splitn(2, ",").collect();
                        self.add_endpoint(
                            cc.node_id,
                            remote_grpc_addr[0].to_string(),
                            remote_grpc_addr[1].to_string(),
                            false,
                            false,
                        )
                        .await
                        .unwrap();
                    }

                    let mut raw_node = self.raw_node.lock().await;
                    let cs = raw_node.apply_conf_change(&ccv2).unwrap();
                    info!("Node[{}] ConfChangeV2 applied: {:?}", self.id, cs);
                    let _ = store.set_conf_state(&cs);
                }
            }
        }
    }

    async fn commit_self_conf_change(&self) {
        let context = format!("{},{}", self.raft_grpc_addr, self.coo_grpc_addr);
        let cc = ConfChange {
            change_type: ConfChangeType::AddNode,
            node_id: self.id as u64,
            context: context.as_bytes().to_vec().into(),
            id: self.id as u64,
            ..Default::default()
        };

        let mut raw_node = self.raw_node.lock().await;
        raw_node.propose_conf_change(vec![], cc).unwrap();
        info!("Leader[{}] 提交初始配置变更", self.id);
    }

    pub async fn join(&self, remote_addr: String) -> Result<()> {
        self.add_endpoint(0, remote_addr, "".to_string(), true, true)
            .await
    }

    async fn add_endpoint(
        &self,
        node_id: u64,
        remote_raft_addr: String,
        remote_coo_addr: String,
        commit_self_conf_change: bool,
        sync: bool,
    ) -> Result<()> {
        let src_id = self.id;
        let raft_grpc_addr = self.raft_grpc_addr.clone();
        let coo_grpc_addr = self.coo_grpc_addr.clone();
        let mailboxes = Arc::clone(&self.mailboxes);
        let peer = Arc::clone(&self.peer);

        let (tx_sync, rx_sync) = oneshot::channel();
        tokio::spawn(async move {
            let mut inter = interval(Duration::from_secs(10));
            loop {
                // 连接到 Leader 的 gRPC 服务
                match cooraftsvc::raft_service_client::RaftServiceClient::connect(
                    repair_addr_with_http(remote_raft_addr.clone()),
                )
                .await
                {
                    Ok(mut client) => {
                        if let Ok(resp) = client.get_meta(Request::new(cooraftsvc::Empty {})).await
                        {
                            let resp = resp.into_inner();
                            let dst_id = if node_id != 0 {
                                node_id
                            } else {
                                resp.id as u64
                            };
                            if dst_id == src_id as u64 {
                                if sync {
                                    let _ = tx_sync.send(None);
                                }
                                break;
                            }

                            if mailboxes.contains_key(&dst_id) {
                                let _ = tx_sync.send(None);
                                break;
                            }

                            if commit_self_conf_change {
                                let context = format!("{},{}", raft_grpc_addr, coo_grpc_addr);
                                let cc = ConfChange {
                                    change_type: ConfChangeType::AddNode,
                                    node_id: src_id as u64,
                                    context: context.into_bytes().into(),
                                    id: 0,
                                    ..Default::default()
                                };

                                let _ = client
                                    .propose_conf_change(Request::new(ConfChangeReq {
                                        version: 1,
                                        message: cc.write_to_bytes().unwrap(),
                                    }))
                                    .await;
                            }

                            info!(
                                "链接目标端成功: Node[{}]->Node[{}:{}], sync={}",
                                src_id, dst_id, remote_raft_addr, sync
                            );

                            let (tx_msg, rx_msg) = mpsc::channel(1);
                            let peer_state =
                                Arc::new(PeerState::new(dst_id, resp.raft_addr, resp.coo_addr));
                            let mut mb = Mailbox::new(
                                src_id,
                                dst_id as u32,
                                NonZero::new(5_u64).unwrap(),
                                client,
                                rx_msg,
                                peer_state.clone(),
                            );
                            mailboxes.insert(dst_id, tx_msg);
                            peer.insert(dst_id, peer_state);
                            let handle = tokio::spawn(async move { mb.start_serve().await });
                            if sync {
                                let _ = tx_sync.send(Some(handle));
                            }
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Node[{}] 增加链接至 {} 失败: {:?}",
                            src_id, remote_raft_addr, e
                        );
                    }
                };
                // 等待下次链接
                inter.tick().await;
            }
        });
        if sync {
            let _ = rx_sync.await;
        }
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        let raw_node = self.raw_node.lock().await;
        raw_node.raft.state == StateRole::Leader
    }

    async fn propose(&self, part: &SinglePartition, cb: Option<Callback>) -> Result<()> {
        let send = |e| async {
            if let Some(cb) = &cb {
                let _ = cb.send(e).await;
            }
        };
        let mut raw_node = self.raw_node.lock().await;
        if raw_node.raft.state != StateRole::Leader {
            send(Err(anyhow!("Not Leader"))).await;
            return Err(anyhow!("Not leader"));
        }
        let unique_id = part.unique_id.clone();
        let data = bincode::serde::encode_to_vec(part, bincode::config::standard())?;
        if let Err(e) = raw_node.propose(vec![], data) {
            send(Err(anyhow!(e.to_string()))).await;
            return Err(anyhow!(e));
        }
        if let Some(cb) = cb {
            self.callbacks.insert(unique_id, cb);
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.db
            .get_db()
            .get(key)
            .unwrap()
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
    }

    pub async fn get_raft_leader_addr(&self) -> Option<String> {
        let raw_node = self.raw_node.lock().await;
        self.peer
            .get(&raw_node.raft.leader_id)
            .map(|v| v.get_raft_addr().to_string())
    }

    pub async fn get_coo_leader_addr(&self) -> Option<String> {
        let raw_node = self.raw_node.lock().await;
        self.peer
            .get(&raw_node.raft.leader_id)
            .map(|v| v.get_coo_addr().to_string())
    }
}

struct Mailbox {
    src_id: u32,
    dst_id: u32,
    send_timeout: NonZero<u64>,
    client: RaftServiceClient<Channel>,
    rx_msg: mpsc::Receiver<RaftMessage>,
    status: Arc<PeerState>, // rx_stop: mpsc::Receiver<()>,
}

impl Mailbox {
    fn new(
        src_id: u32,
        dst_id: u32,
        send_timeout: NonZero<u64>,
        client: RaftServiceClient<Channel>,
        rx_msg: mpsc::Receiver<RaftMessage>,
        status: Arc<PeerState>, // rx_stop: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            src_id,
            dst_id,
            send_timeout,
            client,
            rx_msg,
            status,
        }
    }

    async fn start_serve(&mut self) {
        let mut failed_times = 0;
        loop {
            if self.rx_msg.is_closed() {
                warn!("Mailbox[{}->{}] has been closed", self.src_id, self.dst_id);
                break;
            }
            select! {
                msg = self.rx_msg.recv() => {
                    if msg.is_none() {
                        continue;
                    }
                    let msg = msg.unwrap();
                    let mut req = Request::new(msg);
                    trace!("Node[{}] -> Node[{}]: timeout: {}s",self.src_id,self.dst_id,self.send_timeout);
                    req.set_timeout(Duration::from_secs(self.send_timeout.get()));
                    match self.client.send_raft_message(req).await {
                        Ok(_) => {
                            self.status.rotate_upgrade().await;
                            debug!("Node[{}]->Node[{}] Mailbox sent message successfully", self.src_id, self.dst_id);
                        },
                        Err(e) => {
                            self.status.rotate_downgrade().await;
                            if failed_times % 50 == 0 {
                                error!("Node[{}]->Node[{}] Mailbox send error: {:?}", self.src_id, self.dst_id, e);
                            }
                            failed_times += 1;
                        },
                    }
                }
            }
        }
    }
}
