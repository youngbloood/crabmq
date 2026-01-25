use crate::{Config as SelfConfig, mailbox::Mailbox, peer::PeerState, storage::SledStorage};
use anyhow::Result;
use bincode::{Decode, Encode};
use bytes::Bytes;
use dashmap::DashMap;
use log::{debug, error, info, warn};
use protobuf::Message as _;
use protocolv2::{CooRaftConfChangeRequest, CooRaftOriginMessage, EnDecoder};
use raft::{
    Config, RawNode, StateRole,
    prelude::{
        ConfChange, ConfChangeType, ConfChangeV2, Entry, EntryType, HardState, Message, Snapshot,
    },
};
use sled::Db;
use std::{collections::HashMap, num::NonZero, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, mpsc},
    time::{self, Instant},
};
use transporter::{TransportMessage, Transporter};

#[derive(Clone)]
pub struct Node {
    pub id: u32,

    conf: Arc<SelfConfig>,
    // raft node
    pub raw_node: Arc<Mutex<RawNode<SledStorage>>>,

    my_mailbox_sender: Arc<mpsc::Sender<TransportMessage>>,
    // 本节点收到的 信息
    my_mailbox: Arc<Mutex<mpsc::Receiver<TransportMessage>>>,
    // 将消息分发到所有的 mailboxes 中
    mailboxes: Arc<DashMap<u32, mpsc::Sender<TransportMessage>>>,
    // 集群中其他节点的信息(包含自身)
    peer: Arc<DashMap<u32, Arc<PeerState>>>,

    // db: P, // Key-value store
    callbacks: Arc<DashMap<String, Callback>>,

    trans: Transporter,
}

pub type Callback = mpsc::Sender<Result<String>>;

// impl self functions
impl Node {
    pub fn new(conf: SelfConfig) -> (Self, mpsc::Sender<TransportMessage>) {
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
            trans: Transporter::new(transporter::Config::default()),
            conf: Arc::new(conf),
            callbacks: Arc::new(DashMap::new()),
        };
        node.peer.insert(
            node.conf.id.into(),
            Arc::new(PeerState::new(
                node.conf.id.into(),
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

    async fn handle_meta_req(&self, remote_addr: &str, req: &protocolv2::CooRaftGetMetaRequest) {
        if req.id == self.conf.id {
            let resp = protocolv2::ErrorResponse {
                code: protocolv2::ErrorCode::RaftIDConflict,
                message: "".to_string(),
                meta: None,
            };
            let t = TransportMessage {
                index: 1,
                remote_addr: remote_addr.clone().to_string(),
                message: Box::new(resp) as Box<dyn EnDecoder>,
            };
            self.trans.send(&t).await;
            self.trans.close(remote_addr).await;
            return;
        }

        let (tx, rx) = mpsc::channel(self.conf.mailbox_buffer_len);
        let w = self.trans.split_writer(remote_addr).await;
        if w.is_none() {
            error!("not found writer for remote_addr[{}]", remote_addr);
            return;
        }
        let w = w.unwrap();
        let peer = self.peer.get(&req.id).unwrap();
        let mb = Mailbox::new(
            self.id,
            req.id,
            NonZero::new(self.conf.raft.write_timeout_milli).unwrap(),
            w,
            rx,
            peer.value().clone(),
        );
        self.mailboxes.insert(req.id, tx);
        tokio::spawn(mb.start_serve());

        // 构建响应并发送至对端
        let resp = protocolv2::CooRaftGetMetaResponse {
            id: self.id,
            raft_addr: self.conf.raft.addr.clone(),
            meta: self.conf.raft.meta.clone(),
        };
        let t = TransportMessage {
            index: 1,
            remote_addr: remote_addr.clone().to_string(),
            message: Box::new(resp) as Box<dyn EnDecoder>,
        };
        self.trans.send(&t).await;
    }

    async fn handle_conf_change(&self, req: &protocolv2::CooRaftConfChangeRequest) {
        match req.version {
            protocolv2::ConfChangeVersion::V1 => {
                let mut cc = ConfChange::new();
                cc.merge_from_bytes(&req.message);
                let mut raw_node = self.raw_node.lock().await;
                raw_node.propose_conf_change(vec![], cc).unwrap();
            }

            protocolv2::ConfChangeVersion::V2 => {
                let mut ccv2 = ConfChangeV2::default();
                ccv2.merge_from_bytes(&req.message).unwrap();
                let mut raw_node = self.raw_node.lock().await;
                raw_node.propose_conf_change(vec![], ccv2).unwrap();
            }
        }
    }

    async fn handle_raft_message(&self, req: &protocolv2::CooRaftOriginMessage) {
        let mut msg = Message::new();
        msg.merge_from_bytes(&req.message);
        self.raw_node.lock().await.step(msg);
    }

    async fn handle_propose_message(&self, req: &protocolv2::CooRaftProposeMessage) {
        match req.index {
            protocolv2::CooRaftProposeType::Partition => {
                self.raw_node
                    .lock()
                    .await
                    .propose(vec![], req.message.to_vec());
            }
            protocolv2::CooRaftProposeType::ConsumerGroupOffset => {
                self.raw_node
                    .lock()
                    .await
                    .propose(vec![], req.message.to_vec());
            }
        }
    }
}

// impl raft functions
impl Node {
    pub async fn run(&self) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(100));
        let mut print_interval = Instant::now();
        let mut is_initial_conf_committed = false;

        self.trans.start().await?;

        if self.is_leader().await && !is_initial_conf_committed {
            self.commit_self_conf_change().await;
            is_initial_conf_committed = true;
        }

        let node: Node = self.clone();
        let mut trans = self.trans.clone();

        loop {
            let my_mailbox = self.my_mailbox.clone();
            select! {
                msg = trans.recv(0) => {
                    let msg = msg.unwrap();
                    self.my_mailbox_sender.send(msg).await;
                }

                msg = async {
                    let mut l = my_mailbox.lock().await;
                    l.recv().await
                } => {
                    if msg.is_none(){
                        continue;
                    }
                    let msg = msg.unwrap();


                    match msg.index {
                        protocolv2::COO_RAFT_GET_META_REQUEST_INDEX => {
                            let req = msg.message.as_any().downcast_ref::<protocolv2::CooRaftGetMetaRequest>().unwrap();
                            node.handle_meta_req(&msg.remote_addr, req).await;
                        }

                        protocolv2::COO_RAFT_ORIGIN_MESSAGE_INDEX => {
                            let req = msg.message.as_any().downcast_ref::<protocolv2::CooRaftOriginMessage>().unwrap();
                            node.handle_raft_message(req).await;
                        }

                        protocolv2::COO_RAFT_CONF_CHANGE_REQUEST_INDEX => {
                            let req = msg.message.as_any().downcast_ref::<protocolv2::CooRaftConfChangeRequest>().unwrap();
                            node.handle_conf_change(req).await;
                        }

                        protocolv2::COO_RAFT_PROPOSE_MESSAGE_INDEX => {
                            let req = msg.message.as_any().downcast_ref::<protocolv2::CooRaftProposeMessage>().expect("");
                            node.handle_propose_message(req).await;
                        }

                        _ => {
                            warn!(
                                "Node[{}] 收到未知消息类型: {}, content: {:?}",
                                self.id, msg.index, msg.message
                            );
                        }
                    }

                //     match msg {
                //         AllMessageType::RaftMessage(msg) => {
                //             debug!(
                //                 "Node[{}] 收到消息From[{}]->To[{}], 类型: {:?}",
                //                 self.id,
                //                 msg.from,
                //                 msg.to,
                //                 msg.get_msg_type()
                //             );
                //             let mut raw_node = self.raw_node.lock().await;
                //             raw_node.step(msg).unwrap();
                //         }

                //         AllMessageType::RaftConfChange(cc) => {
                //             if !self.is_leader().await {
                //                 continue;
                //             }

                //             let context_str = String::from_utf8(cc.context.to_vec()).unwrap();
                //             let remote_grpc_addr: Vec<_> =context_str
                //                 .splitn(2, ",")
                //                 .collect();
                //             info!(
                //                 "Node[{}] 收到：{}:{} 的ConfChange = {:?}",
                //                 self.id, remote_grpc_addr[0],remote_grpc_addr[1], &cc
                //             );
                //             self.add_endpoint(
                //                 cc.node_id,
                //                 remote_grpc_addr[0].to_string(),
                //                 remote_grpc_addr[1].to_string(),
                //                 false,
                //                 false)
                //                 .await
                //                 .unwrap();

                //             let mut raw_node = self.raw_node.lock().await;
                //             // if raw_node.raft.has_pending_conf() {
                //             //     error!(
                //             //         "Node[{}] ignoring ConfChange due to unapplied changes",
                //             //         self.id
                //             //     );
                //             //     continue;
                //             // }
                //             raw_node
                //                 .propose_conf_change(cc.context.to_vec(), cc.clone())
                //                 .unwrap();

                //             // self.handle_all_ready("ConfChange").await;
                //         }

                //         AllMessageType::RaftConfChangeV2(cc) => {
                //             if !self.is_leader().await {
                //                 continue;
                //             }

                //             let context_str = String::from_utf8(cc.context.to_vec()).unwrap();
                //             let remote_grpc_addr: Vec<_> = context_str
                //                 .splitn(2, ",")
                //                 .collect();
                //             for cc in &cc.changes {
                //                 info!(
                //                     "Node[{}] 收到：[raft{}:coo{}] 的ConfChangeV2",
                //                     self.id, remote_grpc_addr[0], remote_grpc_addr[1]
                //                 );
                //                 self.add_endpoint(
                //                     cc.node_id,
                //                     remote_grpc_addr[0].to_string(),
                //                     remote_grpc_addr[1].to_string(),
                //                     false,
                //                     false)
                //                     .await
                //                     .unwrap();
                //             }

                //             let mut raw_node = self.raw_node.lock().await;
                //             if raw_node.raft.has_pending_conf() {
                //                 error!(
                //                     "Node[{}] ignoring ConfChangeV2 due to unapplied changes",
                //                     self.id
                //                 );
                //                 continue;
                //             }
                //             raw_node
                //                 .propose_conf_change(cc.context.to_vec(), cc)
                //                 .unwrap();
                //         }

                //         AllMessageType::RaftPropose(pp) => {
                //             match pp{
                //                 ProposeData::TopicPartition(tp) => {
                //                     let _ = self.propose(&tp.topic, tp.callback).await;
                //                 },
                //                 ProposeData::ConsumerGroupDetail() => todo!(),
                //             }

                //         }
                //     }
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

    async fn commit_self_conf_change(&self) -> Result<()> {
        let context = ConfChangeContext {
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
        info!("Leader[{}] 提交初始配置变更", self.id);

        Ok(())
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
        if ready.must_sync() {
            store.flush()?;
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
                "Node[{}->{}], type = {:?}",
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

                let t = TransportMessage {
                    index: protocolv2::COO_RAFT_ORIGIN_MESSAGE_INDEX,
                    remote_addr: "".to_string(),
                    message: Box::new(msg) as Box<dyn EnDecoder>,
                };

                if let Err(e) = sender.send(t).await {
                    error!(
                        "Node[{}->{}] msg Type[{:?}] send failed: {:?}",
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
        debug!("Node[{}]: handle_entries", self.id);
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let data = &entry.data;

                    // let part: SinglePartition =
                    //     bincode::serde::decode_from_slice(data, config::standard())
                    //         .unwrap()
                    //         .0;
                    // let unique_id = part.unique_id.clone();
                    // self.db.apply(part);
                    // // quorum 确认机制
                    // if let Some((unique_id, cb)) = self.callbacks.remove(&unique_id) {
                    //     let _ = cb.send(Ok(unique_id)).await;
                    // }
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
}

#[derive(Decode, Encode, Default, Debug)]
struct ConfChangeContext {
    addr: String,
    meta: HashMap<String, String>,
}
