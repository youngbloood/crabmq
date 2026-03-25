use crate::{
    Callback, Config as RaftxConfig, StateApply, mailbox::Mailbox, peer::PeerState,
    storage::SledStorage,
};
use anyhow::Result;
use bincode::{Decode, Encode};
use bytes::Bytes;
use dashmap::DashMap;
use log::{debug, error, info, warn};
use prost::Message as ProstMessage; // Alias prost::Message
use protobuf::Message as ProtobufMessage; // Alias protobuf::Message
use protocol::{
    CooRaftOriginMessage, EnDecoder, aggregation::CooRaftProposeMessage,
    pbv1::CooRaftProposeMessage as CooRaftProposeMessageV1,
};
use raft::{
    Config, RawNode, StateRole,
    prelude::{ConfChange, ConfChangeType, ConfChangeV2, Entry, EntryType, Message, Snapshot},
};
use std::{collections::HashMap, num::NonZero, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{self, Sender},
    },
    time::{self, Instant},
};
use transporter::{
    TransportMessage,
    service::{TransporterServiceConfig, TransporterServiceManager},
};

pub struct Node<S: StateApply> {
    pub id: u32,

    conf: Arc<RaftxConfig>,
    // raft node
    pub raw_node: Arc<Mutex<RawNode<SledStorage>>>,

    my_mailbox_sender: Arc<mpsc::Sender<TransportMessage>>,
    // 本节点收到的 信息
    my_mailbox: Arc<Mutex<mpsc::Receiver<TransportMessage>>>,
    // 将消息分发到所有的 mailboxes 中
    mailboxes: Arc<DashMap<u32, mpsc::Sender<TransportMessage>>>,
    // 集群中其他节点的信息(包含自身)
    peer: Arc<DashMap<u32, Arc<PeerState>>>,

    // callbacks
    pub callbacks: Arc<DashMap<String, Callback>>,

    // finite state machine
    fsm: Arc<S>,

    trans_service: TransporterServiceManager,
}

impl<S: StateApply> Clone for Node<S> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            conf: self.conf.clone(),
            raw_node: self.raw_node.clone(),
            my_mailbox_sender: self.my_mailbox_sender.clone(),
            my_mailbox: self.my_mailbox.clone(),
            mailboxes: self.mailboxes.clone(),
            peer: self.peer.clone(),
            callbacks: self.callbacks.clone(),
            fsm: self.fsm.clone(),
            trans_service: self.trans_service.clone(),
        }
    }
}

// impl self functions
impl<S: StateApply> Node<S> {
    pub fn new(conf: RaftxConfig, fsm: S) -> (Self, mpsc::Sender<TransportMessage>) {
        let config = Config {
            id: conf.id as _,
            election_tick: 10,
            heartbeat_tick: 3,
            applied: 0,
            max_size_per_msg: 4096,
            max_inflight_msgs: 256,
            // pre_vote: true,
            ..Default::default()
        };

        let storage = SledStorage::new(
            conf.id as u64,
            sled::open(&conf.db.path).expect("Failed to open sled database"),
        );

        let raw_node = Arc::new(Mutex::new(
            RawNode::with_default_logger(&config, storage).expect("Failed to create Raft node"),
        ));

        // Start Tonic gRPC server
        let (tx, rx) = mpsc::channel(128);

        // let raft_service = RaftServiceImpl::new(id, raw_node.clone(), tx_grpc.clone());
        // 转换为 String
        // tokio::spawn(start_grpc_server(grpc_addr.clone(), raft_service));

        let node = Self {
            id: conf.id,
            raw_node,
            my_mailbox_sender: Arc::new(tx.clone()),
            my_mailbox: Arc::new(Mutex::new(rx)),
            mailboxes: Arc::new(DashMap::new()),
            peer: Arc::new(DashMap::new()),
            trans_service: TransporterServiceManager::new(TransporterServiceConfig::default()),
            conf: Arc::new(conf),
            callbacks: Arc::new(DashMap::new()),
            fsm: Arc::new(fsm),
        };
        node.peer.insert(
            node.conf.id.into(),
            Arc::new(PeerState::new(
                node.conf.id.into(),
                0,
                node.conf.raft.addr.clone(),
                node.conf.raft.meta.clone(),
            )),
        );

        (node, tx)
    }

    pub async fn is_leader(&self) -> bool {
        let raw_node = self.raw_node.lock().await;
        raw_node.raft.state == raft::StateRole::Leader
    }

    pub fn register_callback(&self, unique_id: String, cb: Callback) {
        self.callbacks.insert(unique_id, cb);
    }

    async fn handle_meta_req(&self, conn_id: u64, req: &protocol::CooRaftGetMetaRequest) {
        if req.id == self.conf.id {
            let resp = protocol::ErrorResponse {
                code: 1,
                message: "".to_string(),
                meta: HashMap::new(),
            };
            let t =
                TransportMessage::new_v1(1, conn_id, (Box::new(resp) as Box<dyn EnDecoder>).into());
            let _ = self.trans_service.send(&t, None).await;
            self.trans_service.close(conn_id).await;
            return;
        }

        self.add_peer(req.id, conn_id, &req.addr, req.meta.clone())
            .await;

        // 构建响应并发送至对端
        let resp = protocol::CooRaftGetMetaResponse {
            id: self.id,
            raft_addr: self.conf.raft.addr.clone(),
            meta: self.conf.raft.meta.clone(),
        };
        let t = TransportMessage::new_v1(1, conn_id, (Box::new(resp) as Box<dyn EnDecoder>).into());
        let _ = self.trans_service.send(&t, None).await;
    }

    async fn handle_conf_change(&self, req: &protocol::CooRaftConfChangeRequest) {
        if req.version == protocol::ConfChangeVersion::V1 as i32 {
            let mut cc = ConfChange::new();
            let _ = cc.merge_from_bytes(&req.message);
            let mut raw_node = self.raw_node.lock().await;
            raw_node.propose_conf_change(vec![], cc).unwrap();
        } else if req.version == protocol::ConfChangeVersion::V2 as i32 {
            let mut ccv2 = ConfChangeV2::default();
            ccv2.merge_from_bytes(&req.message).unwrap();
            let mut raw_node = self.raw_node.lock().await;
            raw_node.propose_conf_change(vec![], ccv2).unwrap();
        }
    }

    async fn handle_raft_message(&self, req: &protocol::CooRaftOriginMessage) {
        let mut msg = Message::new();
        let _ = msg.merge_from_bytes(&req.message);
        let _ = self.raw_node.lock().await.step(msg);
    }

    async fn handle_propose_message(&self, req: &CooRaftProposeMessage) {
        match req {
            CooRaftProposeMessage::ProposeV1(coo_raft_propose_message) => {
                if coo_raft_propose_message.index
                    == protocol::CooRaftProposeType::ProposeTypePartition as i32
                {
                    let _ = self
                        .raw_node
                        .lock()
                        .await
                        .propose(vec![1], coo_raft_propose_message.encode_to_vec());
                } else if coo_raft_propose_message.index
                    == protocol::CooRaftProposeType::ProposeTypeConsumerGroupOffset as i32
                {
                    let _ = self
                        .raw_node
                        .lock()
                        .await
                        .propose(vec![1], coo_raft_propose_message.encode_to_vec());
                }
            }
        }
    }

    async fn proccess_received_message(&self, msg: TransportMessage) {
        match msg.index {
            protocol::v1::COO_RAFT_GET_META_REQUEST_INDEX => {
                let req = msg
                    .message
                    .as_any()
                    .downcast_ref::<protocol::CooRaftGetMetaRequest>()
                    .unwrap();
                self.handle_meta_req(msg.conn_id, req).await;
            }

            protocol::v1::COO_RAFT_ORIGIN_MESSAGE_INDEX => {
                let req = msg
                    .message
                    .as_any()
                    .downcast_ref::<protocol::CooRaftOriginMessage>()
                    .unwrap();
                self.handle_raft_message(req).await;
            }

            protocol::v1::COO_RAFT_CONF_CHANGE_REQUEST_INDEX => {
                let req = msg
                    .message
                    .as_any()
                    .downcast_ref::<protocol::CooRaftConfChangeRequest>()
                    .unwrap();
                self.handle_conf_change(req).await;
            }

            protocol::v1::COO_RAFT_PROPOSE_MESSAGE_INDEX => {
                let req = msg
                    .message
                    .as_any()
                    .downcast_ref::<CooRaftProposeMessage>()
                    .expect("");
                self.handle_propose_message(req).await;
            }

            _ => {
                warn!(
                    "RAFTX[{}]: 收到未知消息类型: {}, content: {:?}",
                    self.id, msg.index, msg.message
                );
            }
        }
    }

    async fn add_peer(
        &self,
        remote_id: u32,
        conn_id: u64,
        remote_addr: &str,
        meta: HashMap<String, String>,
    ) {
        let (tx, rx) = mpsc::channel(self.conf.mailbox_buffer_len);
        let w = self.trans_service.split_writer(conn_id).await;
        if w.is_none() {
            error!(
                "RAFTX[{}]: not found writer for remote_addr[{}]",
                self.id, remote_addr
            );
        }

        let w = w.unwrap();
        let peer = self
            .peer
            .entry(remote_id)
            .or_insert(Arc::new(PeerState::new(
                remote_id,
                conn_id,
                remote_addr.to_string(),
                meta,
            )));

        let mb = Mailbox::new(
            self.id,
            remote_id,
            NonZero::new(self.conf.raft.write_timeout_milli).unwrap(),
            w,
            rx,
            peer.value().clone(),
        );
        self.mailboxes.insert(remote_id, tx);
        tokio::spawn(mb.start_serve());

        info!(
            "RAFTX[{}]: added peer[{}] with addr[{}]",
            self.id, remote_id, remote_addr
        );
    }
}

// impl raft functions
impl<S: StateApply> Node<S> {
    pub async fn run(&self, tx: Sender<TransportMessage>) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(100));
        let mut print_interval = Instant::now();
        let mut is_initial_conf_committed = false;

        self.trans_service.run(tx).await?;

        if self.is_leader().await && !is_initial_conf_committed {
            let _ = self.commit_self_conf_change().await;
            is_initial_conf_committed = true;
        }

        let node: Node<S> = self.clone();

        let inner_index = [
            protocol::v1::COO_RAFT_CONF_CHANGE_REQUEST_INDEX,
            protocol::v1::COO_RAFT_ORIGIN_MESSAGE_INDEX,
            protocol::v1::COO_RAFT_PROPOSE_MESSAGE_INDEX,
        ];

        loop {
            let my_mailbox = self.my_mailbox.clone();
            select! {
                msg = async {
                    let mut l = my_mailbox.lock().await;
                    l.recv().await
                } => {
                    if msg.is_none(){
                        continue;
                    }
                    let msg = msg.unwrap();
                    node.proccess_received_message(msg).await;
                }

                _ = interval.tick() => {
                    {
                        let mut raw_node = self.raw_node.lock().await;
                        raw_node.tick();
                        if print_interval.elapsed() > Duration::from_secs(5) {
                            info!(
                                "RAFTX[{}]: term = {}, leader_id = {}, role = {:?}, raft.pr().conf() = {:?}, peer = {:?}",
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
            self.proccess_all_ready().await;
        }
    }

    async fn commit_self_conf_change(&self) -> Result<()> {
        let context = ConfChangeContext {
            id: self.conf.id,
            addr: self.conf.raft.addr.clone(),
            meta: self.conf.raft.meta.clone(),
        };

        let ctx = bincode::encode_to_vec(context, bincode::config::standard())?;

        let context = format!("{},{}", self.conf.raft.addr, self.conf.raft.addr);
        let cc = ConfChange {
            change_type: ConfChangeType::AddNode,
            node_id: self.id as u64,
            context: Bytes::from(ctx),
            id: self.id as u64,
            ..Default::default()
        };

        let mut raw_node = self.raw_node.lock().await;
        raw_node.propose_conf_change(vec![], cc).unwrap();
        info!("RAFTX[{}]: 提交初始配置变更", self.id);

        Ok(())
    }

    // ref: https://docs.rs/raft/0.7.0/raft/#processing-the-ready-state
    async fn proccess_all_ready(&self) {
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
        self.distribute_messages_to_peers(ready.take_messages())
            .await;
        // 2. handle snapshot
        self.handle_snapshot(ready.snapshot(), &store).await;
        // 3. handle committed entries
        self.handle_entries(ready.take_committed_entries(), &store)
            .await;
        // 4. handle entries
        if let Err(e) = store.append(&ready.take_entries()) {
            error!(
                "RAFTX[{}]: persist raft log fail: {:?}, need to retry or panic",
                self.id, e
            );
            return;
        }
        // 5. handle HardState
        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let _ = store.set_hard_state(hs);
        }
        // 6. handle persisted messages
        self.distribute_messages_to_peers(ready.take_persisted_messages())
            .await;

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

        self.distribute_messages_to_peers(light_rd.take_messages())
            .await;
        self.handle_entries(light_rd.take_committed_entries(), &store)
            .await;

        let mut raw_node = self.raw_node.lock().await;
        raw_node.advance_apply();
        debug!("RAFTX[{}]: advance_apply", self.id);
        if raw_node.raft.state == StateRole::Leader {
            debug!("RAFTX[{}]: is leader", self.id);
        }
    }

    async fn distribute_messages_to_peers(&self, messages: Vec<Message>) {
        for msg in messages {
            debug!(
                "RAFTX[{}->{}]: type = {:?}",
                msg.from,
                msg.to,
                &msg.get_msg_type(),
            );
            let typ = msg.get_msg_type();
            let from = msg.from;
            let to = msg.to as u32;
            if let Some(sender) = self.mailboxes.get(&to) {
                let msg = CooRaftOriginMessage {
                    message: msg.write_to_bytes().unwrap(),
                };

                let t = TransportMessage::new_v1(
                    protocol::v1::COO_RAFT_ORIGIN_MESSAGE_INDEX,
                    0,
                    (Box::new(msg) as Box<dyn EnDecoder>).into(),
                );

                if let Err(e) = sender.send(t).await {
                    error!(
                        "RAFTX[{}->{}]: msg Type[{:?}] send failed: {:?}",
                        from, to, typ, e
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
        debug!("RAFTX[{}]: handle_entries", self.id);
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let v = entry.context[0];
                    let data = &entry.data;

                    match v {
                        protocol::VERSION1 => match CooRaftProposeMessageV1::decode(&data[..]) {
                            Ok(req) => {
                                if let Err(e) = self.fsm.apply(protocol::VERSION1, &req.message) {
                                    error!("RAFTX[{}]: fsm apply failed: {:?}", self.id, e);
                                }
                                if let Some((_, cb)) = self.callbacks.remove(&req.unique_id) {
                                    let _ = cb.send(Ok(req.unique_id)).await;
                                }
                            }
                            Err(e) => {
                                error!(
                                    "RAFTX[{}]: decode CooRaftProposeMessage failed: {:?}",
                                    self.id, e
                                );
                            }
                        },

                        _ => {
                            error!("RAFTX[{}]: unknown entry version: {}", self.id, v);
                        }
                    }
                }

                EntryType::EntryConfChange => {
                    debug!("RAFTX[{}]: 收到 EntryConfChange", self.id);
                    let mut cc = ConfChange::default();
                    let _ = cc.merge_from_bytes(&entry.data).unwrap();

                    let mut raw_node = self.raw_node.lock().await;
                    let cs = raw_node.apply_conf_change(&cc).unwrap();
                    info!("RAFTX[{}]: ConfChange applied: {:?}", self.id, cs);
                    let _ = store.set_conf_state(&cs);

                    // 确保 follower 收到该类型消息时增加 endpoint
                    let (ccc, _): (ConfChangeContext, usize) = bincode::decode_from_slice(
                        &cc.context.to_vec(),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    self.add_peer(cc.get_id() as _, 0, &ccc.addr, ccc.meta)
                        .await;
                }

                EntryType::EntryConfChangeV2 => {
                    debug!("RAFTX[{}]: 收到 EntryConfChangeV2", self.id);
                    let mut ccv2 = ConfChangeV2::default();
                    ccv2.merge_from_bytes(&entry.data).unwrap();

                    let mut raw_node = self.raw_node.lock().await;
                    let cs = raw_node.apply_conf_change(&ccv2).unwrap();
                    info!("RAFTX[{}]: ConfChangeV2 applied: {:?}", self.id, cs);
                    let _ = store.set_conf_state(&cs);

                    // 确保 follower 收到该类型消息时增加 endpoint
                    let (ccc, _): (ConfChangeContext, usize) = bincode::decode_from_slice(
                        &ccv2.context.to_vec(),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    self.add_peer(ccc.id, 0, &ccc.addr, ccc.meta).await;
                }
            }
        }
    }
}

#[derive(Decode, Encode, Default, Debug)]
struct ConfChangeContext {
    id: u32,
    addr: String,
    meta: HashMap<String, String>,
}
