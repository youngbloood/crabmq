use super::mailbox_message_type::MessageType as AllMessageType;
use super::storage::SledStorage;
use super::{
    // grpc_service::{RaftServiceImpl, start_grpc_server},
    peer::PeerState,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use grpcx::cooraftsvc::raft_service_server::RaftServiceServer;
use grpcx::cooraftsvc::{self, ConfChangeReq, RaftMessage, raft_service_client::RaftServiceClient};
use log::{debug, error, info, trace, warn};
use protobuf::Message as PbMessage;
use raft::{Config, StateRole, prelude::*, raw_node::RawNode};
use sled::Db;
use std::{num::NonZero, sync::Arc, time::Instant};
use tokio::{
    select,
    sync::{Mutex, mpsc, oneshot},
    time::{self, Duration, interval},
};
use tonic::Response;
use tonic::transport::Server;
use tonic::{Request, transport::Channel};

#[derive(Clone)]
pub struct RaftNode {
    id: u32,
    // raft nodo
    raw_node: Arc<Mutex<RawNode<SledStorage>>>,

    _my_mailbox_sender: mpsc::Sender<AllMessageType>,
    // 本节点收到的 信息
    my_mailbox: Arc<Mutex<mpsc::Receiver<AllMessageType>>>,
    // 将消息分发到所有的 mailboxes 中
    mailboxes: Arc<DashMap<u64, mpsc::Sender<RaftMessage>>>,
    // 集群中其他节点的信息(包含自身)
    peer: Arc<DashMap<u64, Arc<PeerState>>>,

    db: Db, // Key-value stor

    // 该 raft 节点监听地址
    raft_grpc_addr: String,
    // 该 coo 节点监听地址
    coo_grpc_addr: String,
}

unsafe impl Send for RaftNode {}
unsafe impl Sync for RaftNode {}

impl RaftNode {
    pub fn new<T: AsRef<std::path::Path>>(
        id: u32,
        db_path: T,
        raft_grpc_addr: String,
        coo_grpc_addr: String,
    ) -> (Self, mpsc::Sender<AllMessageType>) {
        let db = sled::open(db_path).expect("Failed to open sled DB");
        let config = Config {
            id: id as u64,
            election_tick: 10,
            heartbeat_tick: 3,
            applied: 0,
            max_size_per_msg: 4096,
            max_inflight_msgs: 256,
            pre_vote: true,
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

        let storage = SledStorage::new(id as u64, db.clone());
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
            _my_mailbox_sender: tx_grpc.clone(),
            my_mailbox: Arc::new(Mutex::new(rx_grpc)),
            mailboxes: Arc::new(DashMap::new()),
            peer: Arc::new(DashMap::new()),
            db,
            raft_grpc_addr: raft_grpc_addr.clone(),
            coo_grpc_addr: coo_grpc_addr.clone(),
        };
        rn.peer.insert(
            id.into(),
            Arc::new(PeerState::new(id.into(), raft_grpc_addr, coo_grpc_addr)),
        );

        (rn, tx_grpc)
    }

    async fn start_grpc_service(&self) {
        let addr = self.raft_grpc_addr.parse().unwrap();
        let svc = RaftServiceServer::new(self.clone());
        info!("Coordinator-Raft listen: {}", addr);
        match Server::builder().add_service(svc).serve(addr).await {
            Ok(_) => {
                info!("Coordinator-Raft server started at {}", addr);
            }
            Err(e) => panic!("Coordinator-Raft listen : {}, err: {:?}", addr, e),
        }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub async fn get_leader_id(&self) -> u32 {
        self.raw_node.lock().await.raft.leader_id as u32
    }

    pub fn get_peer(&self) -> Vec<Arc<PeerState>> {
        let mut list = vec![];
        self.peer.iter().for_each(|v| {
            list.push(v.value().clone());
        });
        list
    }

    pub async fn run(&self) {
        // 开启 grpc 服务
        let node = self.clone();
        tokio::spawn(async move {
            node.start_grpc_service().await;
        });

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
                            todo!("raft propose")
                        }
                    }
                }

                _ = interval.tick() => {
                    {
                        let mut raw_node = self.raw_node.lock().await;
                        raw_node.tick();
                        if print_interval.elapsed() > Duration::from_secs(5) {
                            info!(
                                "Node[{}] role = {:?}, raft.pr().conf() = {:?}, peer = {:?}",
                                self.id,
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
                    let kv: serde_json::Value = serde_json::from_slice(data).unwrap();
                    let key = kv["key"].as_str().unwrap();
                    let value = kv["value"].as_array().unwrap();
                    let mut values = Vec::with_capacity(value.len());
                    for v in value {
                        values.push(v.as_u64().unwrap() as u32);
                    }
                    // TODO: 插入values
                    todo!("self.db.insert(key, values).unwrap();")
                    // self.db.insert(key, values).unwrap();
                    // info!("Node[{}] applied: {} = {}", self.id, key, value);
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
                match cooraftsvc::raft_service_client::RaftServiceClient::connect(format!(
                    "http://{}",
                    remote_raft_addr
                ))
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

    pub async fn propose(&self, key: &str, broker_ids: &Vec<u32>) -> Result<()> {
        let mut raw_node = self.raw_node.lock().await;
        if raw_node.raft.state != StateRole::Leader {
            return Err(anyhow!("Not leader"));
        }
        let data = serde_json::json!({ "key": key, "value": broker_ids });
        let data = serde_json::to_vec(&data).unwrap();
        // TODO: propose data
        raw_node.propose(vec![], data)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.db
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

#[tonic::async_trait]
impl cooraftsvc::raft_service_server::RaftService for RaftNode {
    async fn send_raft_message(
        &self,
        request: Request<cooraftsvc::RaftMessage>,
    ) -> Result<Response<cooraftsvc::RaftResponse>, tonic::Status> {
        match Message::parse_from_bytes(&request.into_inner().message) {
            Ok(msg) => {
                let _ = self
                    ._my_mailbox_sender
                    .send(AllMessageType::RaftMessage(msg))
                    .await;
                Ok(Response::new(cooraftsvc::RaftResponse {
                    success: true,
                    error: String::new(),
                }))
            }
            Err(e) => {
                return Ok(Response::new(cooraftsvc::RaftResponse {
                    success: false,
                    error: e.to_string(),
                }));
            }
        }
    }

    async fn get_meta(
        &self,
        _request: tonic::Request<cooraftsvc::Empty>,
    ) -> Result<Response<cooraftsvc::MetaResp>, tonic::Status> {
        Ok(Response::new(cooraftsvc::MetaResp {
            id: self.id,
            raft_addr: String::new(),
            coo_addr: "".to_string(),
        }))
    }

    async fn propose_data(
        &self,
        _request: tonic::Request<cooraftsvc::ProposeDataReq>,
    ) -> std::result::Result<tonic::Response<cooraftsvc::ProposeDataResp>, tonic::Status> {
        let _ = self
            ._my_mailbox_sender
            .send(AllMessageType::RaftPropose((vec![], vec![])))
            .await;
        Ok(Response::new(cooraftsvc::ProposeDataResp {}))
    }

    async fn propose_conf_change(
        &self,
        request: tonic::Request<cooraftsvc::ConfChangeReq>,
    ) -> std::result::Result<tonic::Response<cooraftsvc::ConfChangeResp>, tonic::Status> {
        let req = request.into_inner();
        let reason = match req.version {
            1 => match ConfChange::parse_from_bytes(&req.message) {
                Ok(cc) => {
                    let _ = self
                        ._my_mailbox_sender
                        .send(AllMessageType::RaftConfChange(cc))
                        .await;
                    ""
                }
                Err(e) => &e.to_string(),
            },

            2 => match ConfChangeV2::parse_from_bytes(&req.message) {
                Ok(cc) => {
                    let _ = self
                        ._my_mailbox_sender
                        .send(AllMessageType::RaftConfChangeV2(cc))
                        .await;
                    ""
                }
                Err(e) => &e.to_string(),
            },
            _ => "unsupportted version",
        };

        if reason.is_empty() {
            return Ok(Response::new(cooraftsvc::ConfChangeResp {
                success: true,
                error: String::new(),
            }));
        }

        Ok(Response::new(cooraftsvc::ConfChangeResp {
            success: false,
            error: reason.to_string(),
        }))
    }

    /// 获取 snapshot
    async fn get_snapshot(
        &self,
        _request: tonic::Request<cooraftsvc::SnapshotReq>,
    ) -> std::result::Result<tonic::Response<cooraftsvc::SnapshotResp>, tonic::Status> {
        let raw_node = self.raw_node.lock().await;
        if let Some(snap) = raw_node.raft.snap() {
            match snap.write_to_bytes() {
                Ok(data) => return Ok(Response::new(cooraftsvc::SnapshotResp { data })),
                Err(_e) => return Ok(Response::new(cooraftsvc::SnapshotResp { data: vec![] })),
            };
        }
        Ok(Response::new(cooraftsvc::SnapshotResp { data: vec![] }))
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
