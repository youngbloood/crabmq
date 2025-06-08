use super::Config as CooConfig;
use super::partition;
use super::raftx;
use crate::ClientNode;
use crate::consumer_group::ConsumerGroupManager;
use crate::event_bus::EventBus;
use crate::partition::PartitionPolicy;
use crate::partition::SinglePartition;
use crate::raftx::Callback;
use crate::raftx::ProposeData;
use crate::raftx::TopicPartitionData;
use anyhow::Result;
use anyhow::anyhow;
use dashmap::DashMap;
use grpcx::brokercoosvc::BrokerState;
use grpcx::brokercoosvc::broker_coo_service_server::BrokerCooServiceServer;
use grpcx::clientcoosvc::client_coo_service_server::ClientCooServiceServer;
use grpcx::commonsvc;
use grpcx::commonsvc::CooListResp;
use grpcx::cooraftsvc;
use grpcx::cooraftsvc::raft_service_server::RaftServiceServer;
use grpcx::smart_client::repair_addr_with_http;
use grpcx::topic_meta::TopicPartitionDetail;
use grpcx::{brokercoosvc, clientcoosvc};
use log::error;
use log::info;
use log::warn;
use partition::PartitionManager;
use protobuf::Message as _;
use raft::prelude::*;
use raftx::{MessageType as AllMessageType, RaftNode};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};

#[derive(Clone)]
pub struct Coordinator {
    // coordinator 节点指向的 raft leader 节点地址 （raft-leader 监听的地址）
    raft_leader_addr: String,

    pub conf: CooConfig,

    // raft_node 节点
    raft_node: Arc<RaftNode<PartitionManager>>,
    // 向 raft_node 节点发送消息的通道
    raft_node_sender: mpsc::Sender<AllMessageType>,

    // coo: leader 收集到的 broker 上报信息
    // broker_id -> BrokerState
    brokers: Arc<DashMap<u32, BrokerState>>,

    // 链接 coo 的客户端
    clients: Arc<DashMap<String, ClientNode>>,

    // (Leadder)集群的 Topic-Partition 变更事件总线
    broker_event_bus: EventBus<PartitionEvent>,
    client_event_bus: EventBus<PartitionEvent>,

    //（Leader）集群节点变更事件总线
    peer_change_bus: EventBus<Result<CooListResp, Status>>,

    // 分区管理
    partition_manager: PartitionManager,

    // 消费者组管理器
    consumer_group_manager: ConsumerGroupManager,
}

impl Deref for Coordinator {
    type Target = Arc<RaftNode<PartitionManager>>;

    fn deref(&self) -> &Self::Target {
        &self.raft_node
    }
}

impl Coordinator {
    pub fn new(
        raft_leader_addr: String, /* 用于后续的 raft 节点 join 之前的集群中，为空时表示自己为当前集群的第一个节点 */
        conf: CooConfig,
    ) -> Self {
        let partition_manager =
            PartitionManager::new(PartitionPolicy::default(), conf.db_path.clone());
        let (raft_node, raft_node_sender) = RaftNode::new(
            conf.id,
            conf.raft_addr.clone(),
            conf.coo_addr.clone(),
            partition_manager.clone(),
        );
        let raft_node = Arc::new(raft_node);

        Self {
            raft_leader_addr,
            raft_node: raft_node.clone(),
            raft_node_sender,
            brokers: Arc::new(DashMap::new()),
            clients: Arc::new(DashMap::new()),
            broker_event_bus: EventBus::new(conf.event_bus_buffer_size),
            client_event_bus: EventBus::new(conf.event_bus_buffer_size),
            peer_change_bus: EventBus::new(conf.event_bus_buffer_size),
            consumer_group_manager: ConsumerGroupManager::new(partition_manager.all_topics.clone()),
            partition_manager,
            conf,
            // broker_consumer_bus: todo!(),
            // client_consumer_bus: todo!(),
        }
    }

    pub async fn is_leader(&self) -> bool {
        self.raft_node.is_leader().await
    }

    pub async fn get_raft_leader_addr(&self) -> Option<String> {
        self.raft_node.get_raft_leader_addr().await
    }

    pub async fn get_coo_leader_addr(&self) -> Option<String> {
        self.raft_node.get_coo_leader_addr().await
    }

    async fn check_leader(&self) -> Result<(), tonic::Status> {
        if !self.raft_node.is_leader().await {
            return Err(self
                .raft_node
                .get_not_leader_err_status("Not Leader".to_string())
                .await);
        }
        Ok(())
    }

    /// broker_pull
    ///
    /// 用于本地的 `broker` 向 coo 报道自身信息
    pub fn apply_broker_report(&self, state: brokercoosvc::BrokerState) {
        let id = state.id;
        self.brokers
            .entry(id)
            .and_modify(|entry| {
                // TODO: 修改值
                *entry = state.clone();
            })
            .or_insert(state.clone());
    }

    /// 启动 Coordinator 服务
    pub async fn run(&self) -> Result<()> {
        if self.conf.coo_addr.is_empty() || self.coo_grpc_addr.is_empty() {
            return Err(anyhow!("must both have coo_addr and coo_grpc_addr"));
        }
        // 定期检查本节点从: 主 -> 非主，并发送 NotLeader 消息，用户通知 broker 和 client 变更链接逻辑
        self.start_main_loop();

        let raft_svc = RaftServiceServer::new(self.clone());
        let brokercoo_svc = BrokerCooServiceServer::new(self.clone());
        let clientcoo_svc = ClientCooServiceServer::new(self.clone());

        let mut svc_builer = Server::builder()
            .add_service(brokercoo_svc)
            .add_service(clientcoo_svc);

        let coo_addr = self.conf.coo_addr.parse().unwrap();
        if !self.conf.coo_addr.eq(&self.coo_grpc_addr) {
            let raft_grpc_addr = self.coo_grpc_addr.parse().unwrap();
            tokio::spawn(async move {
                tokio::spawn(async move {
                    info!("Coordinator-Raft listen: {}", raft_grpc_addr);
                    match Server::builder()
                        .add_service(raft_svc)
                        .serve(raft_grpc_addr)
                        .await
                    {
                        Ok(_) => {
                            info!("Coordinator-Raft server started at {}", raft_grpc_addr);
                        }
                        Err(e) => {
                            panic!("Coordinator-Raft listen : {}, err: {:?}", raft_grpc_addr, e)
                        }
                    }
                });
            });
        } else {
            svc_builer = svc_builer.add_service(raft_svc);
            info!("Coordinator-Raft listen: {}", coo_addr);
        }

        let raft_node = self.raft_node.clone();
        let raft_handle = tokio::spawn(async move {
            raft_node.run().await;
        });

        let grpc_handle = tokio::spawn(async move {
            info!("Coordinator listen: {}", coo_addr);
            match svc_builer.serve(coo_addr).await {
                Ok(_) => {
                    info!("Coordinator server started at {}", coo_addr);
                }
                Err(e) => panic!("Coordinator listen : {}, err: {:?}", coo_addr, e),
            }
        });

        let join_handle = if !self.raft_leader_addr.is_empty() {
            let raft_node = self.raft_node.clone();
            let raft_leader_addr = self.raft_leader_addr.clone();
            tokio::spawn(async move {
                let _ = raft_node.join(raft_leader_addr).await;
            })
        } else {
            tokio::spawn(async {})
        };

        tokio::try_join!(raft_handle, grpc_handle, join_handle)?;
        Ok(())
    }

    // coo 模块所有循环
    fn start_main_loop(&self) {
        let raft_node = self.raft_node.clone();
        let broker_event_bus = self.broker_event_bus.clone();
        let client_event_bus = self.client_event_bus.clone();
        let check_self_is_leader_interval = self.conf.check_self_is_leader_interval;
        // 定期检查本节点从: 主 -> 非主，并发送 NotLeader 消息，用户通知 broker 和 client 变更链接逻辑
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(check_self_is_leader_interval));
            let mut become_leader = false;
            loop {
                ticker.tick().await;
                if raft_node.is_leader().await {
                    become_leader = true;
                } else if become_leader {
                    broker_event_bus
                        .broadcast(PartitionEvent::NotLeader {
                            new_leader_id: raft_node.get_id(),
                            new_coo_leader_addr: raft_node
                                .get_coo_leader_addr()
                                .await
                                .unwrap_or_default(),
                            new_raft_leader_addr: raft_node
                                .get_raft_leader_addr()
                                .await
                                .unwrap_or_default(),
                        })
                        .await;

                    client_event_bus
                        .broadcast(PartitionEvent::NotLeader {
                            new_leader_id: raft_node.get_id(),
                            new_coo_leader_addr: raft_node
                                .get_coo_leader_addr()
                                .await
                                .unwrap_or_default(),
                            new_raft_leader_addr: raft_node
                                .get_raft_leader_addr()
                                .await
                                .unwrap_or_default(),
                        })
                        .await;
                }
            }
        });

        let raft_node = self.raft_node.clone();
        let broker_event_bus = self.broker_event_bus.clone();
        let client_event_bus = self.client_event_bus.clone();

        // 定期发送空PartitionEvent
        // tokio::spawn(async move {
        //     let mut ticker = interval(Duration::from_secs(5));
        //     loop {
        //         ticker.tick().await;
        //         if raft_node.is_leader().await {
        //             broker_event_bus
        //                 .broadcast(PartitionEvent::AddPartitions {
        //                     added: SinglePartition::default(),
        //                 })
        //                 .await;

        //             client_event_bus
        //                 .broadcast(PartitionEvent::AddPartitions {
        //                     added: SinglePartition::default(),
        //                 })
        //                 .await;
        //         }
        //     }
        // });
    }

    /// broker_pull
    ///
    /// 用于本地的 `broker` 获取 coo 中的 TopicList 最新数据
    pub async fn broker_pull(
        &self,
        broker_id: u32,
    ) -> Result<mpsc::UnboundedReceiver<Result<commonsvc::TopicPartitionResp, Status>>> {
        let id = format!("local_broker_{}", broker_id);
        let (_, mut recver) = self.broker_event_bus.subscribe(id.clone());
        let bus = self.broker_event_bus.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        let init = self.qeury_topics(&[], &[broker_id], &[], &[]).await;
        // 返回初始值

        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            // 发送初始值
            let _ = tx.send(Ok(init));

            // 发送后续增量值
            while let Some(part) = recver.recv().await {
                match part {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        let _ = tx.send(Err(raft_node
                            .get_not_leader_err_status(format!(
                                "Leader changed to [{}:{}:{}]",
                                new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                            ))
                            .await));
                        break;
                    }

                    PartitionEvent::NewTopic { partitions } => {
                        if let Some(partitions) =
                            filter_single_partition(partitions, &[], &[broker_id], &[], &[])
                        {
                            let res = convert_to_pull_resp(partitions, raft_node.get_term().await);
                            let _ = tx.send(Ok(res));
                        }
                    }

                    PartitionEvent::AddPartitions { added } => {
                        if let Some(added) =
                            filter_single_partition(added, &[], &[broker_id], &[], &[])
                        {
                            let _ = tx
                                .send(Ok(convert_to_pull_resp(added, raft_node.get_term().await)));
                        }
                    }
                }
            }
            bus.unsubscribe(&id);
        });

        Ok(rx)
    }
}

impl Coordinator {
    /// qeury_topics: 获取所有的 topic
    ///
    /// 根据条件过滤：
    ///
    /// 1. 根据 `topic` 过滤
    /// 2. 根据 `broker_id` 过滤
    /// 3. 根据 `partition` 过滤
    async fn qeury_topics(
        &self,
        topics: &[String],
        broker_ids: &[u32],
        partition_ids: &[u32],
        keys: &[String],
    ) -> commonsvc::TopicPartitionResp {
        let topic_assignment =
            self.partition_manager
                .query_partitions(topics, broker_ids, partition_ids, keys);

        let mut tpr = commonsvc::TopicPartitionResp {
            term: self.get_term().await,
            list: vec![],
        };
        for (topic, ta) in topic_assignment {
            ta.iter()
                .for_each(|v| tpr.list.push(v.convert_to_topic_partition_meta(&topic)));
        }

        tpr
    }

    async fn propose(
        &self,
        partitions: &SinglePartition,
        callback: Option<Callback>,
    ) -> Result<()> {
        self.raft_node_sender
            .send(AllMessageType::RaftPropose(ProposeData::TopicPartition(
                TopicPartitionData {
                    topic: partitions.clone(),
                    callback,
                },
            )))
            .await?;
        Ok(())
    }

    async fn get_term(&self) -> u64 {
        self.raft_node.get_term().await
    }

    async fn list_peer(
        &self,
        remote_addr: SocketAddr,
        id: String,
        is_broker: bool,
    ) -> mpsc::Receiver<Result<CooListResp, Status>> {
        let peer = self.raft_node.get_peer();
        let mut list = vec![];
        let leader_id = self.raft_node.get_leader_id().await;
        peer.iter().for_each(|v| {
            let id = v.id as u32;
            list.push(commonsvc::CooInfo {
                act: commonsvc::CooInfoAction::Add.into(),
                id,
                coo_addr: repair_addr_with_http(v.coo_addr.clone()),
                raft_addr: repair_addr_with_http(v.raft_addr.clone()),
                role: if id == leader_id {
                    commonsvc::CooRole::Leader.into()
                } else {
                    commonsvc::CooRole::Follower.into()
                },
            });
        });

        let sub_id = if is_broker {
            format!("broker_{}_{}", id, remote_addr)
        } else {
            format!("client_{}_{}", id, remote_addr)
        };
        let (tx, rx) = self.peer_change_bus.subscribe(sub_id);

        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(CooListResp {
                    cluster_term: raft_node.get_term().await,
                    list,
                }))
                .await;
        });

        rx
    }
}

#[derive(Debug, Clone)]
pub enum PartitionEvent {
    NotLeader {
        new_leader_id: u32,
        new_coo_leader_addr: String,
        new_raft_leader_addr: String,
    },
    NewTopic {
        partitions: SinglePartition,
    },
    AddPartitions {
        added: SinglePartition,
    },
}

#[tonic::async_trait]
impl brokercoosvc::broker_coo_service_server::BrokerCooService for Coordinator {
    type ReportStream = tonic::codegen::BoxStream<brokercoosvc::BrokerStateResp>;
    type PullStream = tonic::codegen::BoxStream<commonsvc::TopicPartitionResp>;
    type ListStream = tonic::codegen::BoxStream<commonsvc::CooListResp>;
    type SyncConsumerAssignmentsStream =
        tonic::codegen::BoxStream<brokercoosvc::SyncConsumerAssignmentsResp>;

    async fn auth(
        &self,
        request: tonic::Request<brokercoosvc::AuthReq>,
    ) -> std::result::Result<tonic::Response<brokercoosvc::AuthResp>, tonic::Status> {
        let req = request.into_inner();

        // 示例验证逻辑
        if req.username.is_empty() || req.password.is_empty() {
            return Ok(tonic::Response::new(brokercoosvc::AuthResp {
                error: "用户名或密码不能为空".into(),
                token: "".into(),
            }));
        }

        // 生成token（示例）
        let token = format!(
            "{}-{}-{}",
            req.username,
            req.nonce,
            chrono::Utc::now().timestamp()
        );

        Ok(tonic::Response::new(brokercoosvc::AuthResp {
            error: "".into(), // 空表示成功
            token,
        }))
    }

    async fn list(
        &self,
        request: tonic::Request<commonsvc::CooListReq>,
    ) -> std::result::Result<tonic::Response<Self::ListStream>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();
        let broker_id = request.into_inner().id;
        let rx = self.list_peer(remote_addr, broker_id, true).await;

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // /// Server streaming response type for the report method.
    async fn report(
        &self,
        request: tonic::Request<tonic::Streaming<brokercoosvc::BrokerState>>,
    ) -> std::result::Result<tonic::Response<Self::ReportStream>, tonic::Status> {
        self.check_leader().await?;

        let mut strm = request.into_inner();
        let coo_id = self.id;
        let brokers = self.brokers.clone();

        let output_stream = async_stream::try_stream! {
            while let Ok(Some(state)) = strm.message().await {
                let id = state.id;
                info!("Coo[{}] handle Broker[{}] BrokerState",coo_id,id);

                brokers.entry(id)
                .and_modify(|entry| {
                    // TODO: 修改值
                    *entry = state.clone();
                })
                .or_insert(state.clone());

                  // 返回响应
                yield brokercoosvc::BrokerStateResp {
                    id,
                    success: true,
                };
            }
        };

        Ok(tonic::Response::new(Box::pin(output_stream)))
    }

    /// Server streaming response type for the pull method.
    async fn pull(
        &self,
        request: tonic::Request<brokercoosvc::PullReq>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        self.check_leader().await?;

        let remote_addr = request.remote_addr().unwrap();
        let req = request.into_inner();
        let broker_id = req.broker_id;
        let sub_id = format!("broker_{}_{}", broker_id, remote_addr);
        // 获取事件流
        let (_, mut event_rx) = self.broker_event_bus.subscribe(sub_id.clone());
        let bus = self.broker_event_bus.clone();
        let init_data = self
            .qeury_topics(&req.topics, &[broker_id], &req.partition_ids, &[])
            .await;
        let (resp_tx, resp_rx) = mpsc::channel(self.conf.broker_pull_buffer_size);
        let raft_node = self.raft_node.clone();

        tokio::spawn(async move {
            // 发送初始数据
            let _ = resp_tx.send(Ok(init_data)).await;

            // 监听事件
            while let Some(event) = event_rx.recv().await {
                match event {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        // 发送错误并终止流
                        let _ = resp_tx
                            .send(Err(raft_node
                                .get_not_leader_err_status(format!(
                                    "Leader changed to [{}:{}:{}]",
                                    new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                                ))
                                .await))
                            .await;
                        break;
                    }
                    PartitionEvent::NewTopic { partitions } => {
                        if let Some(partitions) = filter_single_partition(
                            partitions,
                            &req.topics,
                            &[broker_id],
                            &req.partition_ids,
                            &[],
                        ) {
                            let _ = resp_tx
                                .send(Ok(convert_to_pull_resp(
                                    partitions,
                                    raft_node.get_term().await,
                                )))
                                .await;
                        }
                    }
                    // 处理其他事件类型...
                    PartitionEvent::AddPartitions { added } => {
                        if let Some(added) = filter_single_partition(
                            added,
                            &req.topics,
                            &[broker_id],
                            &req.partition_ids,
                            &[],
                        ) {
                            let _ = resp_tx
                                .send(Ok(convert_to_pull_resp(added, raft_node.get_term().await)))
                                .await;
                        }
                    }
                }
            }
            warn!("unsubscribe broker_event_bus: {}", sub_id);
            // 连接结束时取消订阅
            bus.unsubscribe(&sub_id);
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }

    async fn sync_consumer_assignments(
        &self,
        request: tonic::Request<brokercoosvc::SyncConsumerAssignmentsReq>,
    ) -> std::result::Result<tonic::Response<Self::SyncConsumerAssignmentsStream>, tonic::Status>
    {
        let req = request.into_inner();
        let broker_id = req.broker_id;
        let mut rx = self.consumer_group_manager.subscribe_broker_consumer(req);

        let (tx_middleware, rx_middleware) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                if rx.is_closed() {
                    break;
                }
                select! {
                    res = rx.recv() => {
                        if res.is_none() {
                            continue;
                        }
                        let res = res.unwrap();
                        if res.broker_id == broker_id {
                            if let Err(e) = tx_middleware.send(Ok(res)).await {
                                error!("sync_consumer_assignments resp stream err: {e:?}");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            rx_middleware,
        ))))
    }
}

#[tonic::async_trait]
impl clientcoosvc::client_coo_service_server::ClientCooService for Coordinator {
    type PullStream = tonic::codegen::BoxStream<commonsvc::TopicPartitionResp>;
    type ListStream = tonic::codegen::BoxStream<commonsvc::CooListResp>;
    type SyncConsumerAssignmentsStream =
        tonic::codegen::BoxStream<clientcoosvc::SyncConsumerAssignmentsResp>;

    async fn auth(
        &self,
        request: tonic::Request<clientcoosvc::AuthReq>,
    ) -> std::result::Result<tonic::Response<clientcoosvc::AuthResp>, tonic::Status> {
        Ok(tonic::Response::new(clientcoosvc::AuthResp {
            error: todo!(),
            token: todo!(),
        }))
    }

    async fn list(
        &self,
        request: tonic::Request<commonsvc::CooListReq>,
    ) -> std::result::Result<tonic::Response<Self::ListStream>, tonic::Status> {
        let remote_addr = request.remote_addr().unwrap();
        let client_id = request.into_inner().id;
        let rx = self.list_peer(remote_addr, client_id, false).await;

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    /// new_topic: 新增 `topic` 及其分区分配
    ///
    /// 1. 分区模块对新 `topic` 及其分区进行分配
    ///
    /// 2. 将topic元信息提交至 `raft` 集群
    ///
    /// 3. 发送topic元信息广播至 `broker_event_bus` 和 `client_event_bus`
    async fn new_topic(
        &self,
        request: tonic::Request<clientcoosvc::NewTopicReq>,
    ) -> std::result::Result<tonic::Response<clientcoosvc::NewTopicResp>, tonic::Status> {
        self.check_leader().await?;

        let req = request.into_inner();

        let (part, exist) = self
            .partition_manager
            .build_allocator(
                self.conf.new_topic_partition_factor.clone(),
                Some(self.brokers.clone()),
            )
            .assign_new_topic(&req.topic, req.partitio_num)
            .await
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        if exist {
            return Ok(Response::new(clientcoosvc::NewTopicResp {
                success: true,
                error: "".to_string(),
                detail: Some(commonsvc::TopicPartitionResp {
                    term: self.get_term().await,
                    list: part.into(),
                }),
            }));
        }

        let (tx_notify, mut rx_notify) = mpsc::channel(1);
        let _ = self.propose(&part, Some(tx_notify)).await;
        match timeout(
            Duration::from_secs(self.conf.new_topic_timeout),
            rx_notify.recv(),
        )
        .await
        {
            Ok(Some(res)) => match res {
                Ok(unique_id) => {
                    if unique_id == part.unique_id {
                        self.partition_manager.apply_topic_partition_detail(
                            part.partitions
                                .iter()
                                .map(TopicPartitionDetail::from)
                                .collect(),
                        );
                        // 发送通知
                        self.broker_event_bus
                            .broadcast(PartitionEvent::NewTopic {
                                partitions: part.clone(),
                            })
                            .await;
                        self.client_event_bus
                            .broadcast(PartitionEvent::NewTopic {
                                partitions: part.clone(),
                            })
                            .await;

                        let detail = commonsvc::TopicPartitionResp {
                            term: self.get_term().await,
                            list: part.into(),
                        };

                        Ok(Response::new(clientcoosvc::NewTopicResp {
                            success: true,
                            error: "".to_string(),
                            detail: Some(detail),
                        }))
                    } else {
                        Err(tonic::Status::internal("unique_id not eq part.unique_id"))
                    }
                }
                Err(e) => Err(tonic::Status::internal(e.to_string())),
            },
            Ok(None) => Err(tonic::Status::internal("none")),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    /// add_partitions: 某个 `topic` 下新增的分区
    ///
    /// 1. 分区模块将“新分区”分配至 `broker` 节点
    ///
    /// 2. 将新增元信息提交至 `raft` 集群
    ///
    /// 3. 发送新增元信息广播至 `broker_event_bus` 和 `client_event_bus`
    async fn add_partitions(
        &self,
        request: tonic::Request<clientcoosvc::AddPartitionsReq>,
    ) -> std::result::Result<tonic::Response<clientcoosvc::AddPartitionsResp>, tonic::Status> {
        self.check_leader().await?;

        let req = request.into_inner();
        if !self.partition_manager.has_topic(&req.topic) {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("Not has topic: {}", &req.topic),
            ));
        }
        if req.partitio_num == 0 {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "partition must be greater than zero",
            ));
        }
        let added = self
            .partition_manager
            .build_allocator(
                self.conf.new_topic_partition_factor.clone(),
                Some(self.brokers.clone()),
            )
            .add_partitions(&req.topic, req.partitio_num)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        let (tx_notify, mut rx_notify) = mpsc::channel(1);
        let _ = self.propose(&added, Some(tx_notify)).await;
        match timeout(
            Duration::from_secs(self.conf.add_partition_timeout),
            rx_notify.recv(),
        )
        .await
        {
            Ok(Some(res)) => match res {
                Ok(unique_id) => {
                    if unique_id == added.unique_id {
                        // 发送通知
                        self.broker_event_bus
                            .broadcast(PartitionEvent::NewTopic {
                                partitions: added.clone(),
                            })
                            .await;

                        self.client_event_bus
                            .broadcast(PartitionEvent::NewTopic { partitions: added })
                            .await;

                        Ok(Response::new(clientcoosvc::AddPartitionsResp {
                            success: true,
                            error: "".to_string(),
                            detail: todo!(),
                        }))
                    } else {
                        Err(tonic::Status::internal("unique_id not eq added.unique_id"))
                    }
                }
                Err(e) => Err(tonic::Status::internal(e.to_string())),
            },
            Ok(None) => Err(tonic::Status::internal("none")),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    /// Server streaming response type for the pull method.
    async fn pull(
        &self,
        request: tonic::Request<clientcoosvc::PullReq>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        self.check_leader().await?;

        let remote_addr = request.remote_addr().unwrap().to_string();
        let req = request.into_inner();
        let sub_id = format!("client_{}", remote_addr);
        let (_, mut event_rx) = self.client_event_bus.subscribe(sub_id.clone());
        let init_data = self
            .qeury_topics(&req.topics, &req.broker_ids, &req.partition_ids, &req.keys)
            .await;
        let bus: EventBus<PartitionEvent> = self.client_event_bus.clone();
        let (resp_tx, resp_rx) = mpsc::channel(self.conf.client_pull_buffer_size);
        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            let _ = resp_tx.send(Ok(init_data)).await;

            while let Some(event) = event_rx.recv().await {
                match event {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        // 发送错误并终止流
                        let _ = resp_tx
                            .send(Err(raft_node
                                .get_not_leader_err_status(format!(
                                    "Leader changed to [{}:{}:{}]",
                                    new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                                ))
                                .await))
                            .await;
                        break;
                    }
                    PartitionEvent::NewTopic { partitions } => {
                        if let Some(partitions) = filter_single_partition(
                            partitions,
                            &req.topics,
                            &req.broker_ids,
                            &req.partition_ids,
                            &req.keys,
                        ) {
                            let _ = resp_tx
                                .send(Ok(convert_to_pull_resp(
                                    partitions,
                                    raft_node.get_term().await,
                                )))
                                .await;
                        }
                    }
                    // 处理其他事件...
                    PartitionEvent::AddPartitions { added } => {
                        if let Some(added) = filter_single_partition(
                            added,
                            &req.topics,
                            &req.broker_ids,
                            &req.partition_ids,
                            &req.keys,
                        ) {
                            let _ = resp_tx
                                .send(Ok(convert_to_pull_resp(added, raft_node.get_term().await)))
                                .await;
                        }
                    }
                }
            }
            bus.unsubscribe(&sub_id);
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }

    async fn join_group(
        &self,
        request: tonic::Request<clientcoosvc::GroupJoinRequest>,
    ) -> std::result::Result<tonic::Response<clientcoosvc::GroupJoinResponse>, tonic::Status> {
        let res = self
            .consumer_group_manager
            .apply_join(&request.into_inner())
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?;

        if res.error == 0 {
            // TODO: 提交消费者详情值 raft
            // self.raft_node_sender.send(AllMessageType::RaftPropose(()))
        }

        Ok(tonic::Response::new(res))
    }

    async fn sync_consumer_assignments(
        &self,
        request: tonic::Request<clientcoosvc::SyncConsumerAssignmentsReq>,
    ) -> std::result::Result<tonic::Response<Self::SyncConsumerAssignmentsStream>, tonic::Status>
    {
        let req = request.into_inner();
        let member_id = req.member_id.clone();
        let mut rx = self.consumer_group_manager.subscribe_client_consumer(req);

        let (tx_middleware, rx_middleware) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                if rx.is_closed() {
                    break;
                }
                select! {
                    res = rx.recv() => {
                        if res.is_none() {
                            continue;
                        }
                        let res = res.unwrap();
                        if res.member_id == member_id {
                            if let Err(e) = tx_middleware.send(Ok(res)).await {
                                error!("sync_consumer_assignments resp stream err: {e:?}");
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            rx_middleware,
        ))))
    }
}

#[tonic::async_trait]
impl cooraftsvc::raft_service_server::RaftService for Coordinator {
    async fn send_raft_message(
        &self,
        request: Request<cooraftsvc::RaftMessage>,
    ) -> Result<Response<cooraftsvc::RaftResponse>, tonic::Status> {
        match Message::parse_from_bytes(&request.into_inner().message) {
            Ok(msg) => {
                let _ = self
                    .raft_node_sender
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
            raft_addr: repair_addr_with_http(self.raft_node.raft_grpc_addr.clone()),
            coo_addr: repair_addr_with_http(self.raft_node.coo_grpc_addr.clone()),
        }))
    }

    async fn propose_data(
        &self,
        _request: tonic::Request<cooraftsvc::ProposeDataReq>,
    ) -> std::result::Result<tonic::Response<cooraftsvc::ProposeDataResp>, tonic::Status> {
        self.check_leader().await?;

        let (tx_notify, mut rx_notify) = mpsc::channel(1);
        let _ = self
            .raft_node_sender
            .send(AllMessageType::RaftPropose(ProposeData::TopicPartition(
                TopicPartitionData {
                    topic: SinglePartition::default(),
                    callback: Some(tx_notify),
                },
            )))
            .await;

        let _ = rx_notify.recv().await;
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
                        .raft_node_sender
                        .send(AllMessageType::RaftConfChange(cc))
                        .await;
                    ""
                }
                Err(e) => &e.to_string(),
            },

            2 => match ConfChangeV2::parse_from_bytes(&req.message) {
                Ok(cc) => {
                    let _ = self
                        .raft_node_sender
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

// 转换函数示例
fn convert_to_pull_resp(ps: SinglePartition, term: u64) -> commonsvc::TopicPartitionResp {
    let mut tp = commonsvc::TopicPartitionResp::from(ps);
    tp.term = term;
    tp
}

fn filter_single_partition(
    sp: SinglePartition,
    topics: &[String],
    broker_ids: &[u32],
    partition_ids: &[u32],
    keys: &[String],
) -> Option<SinglePartition> {
    // 1. 检查 Topic 是否匹配
    if !topics.is_empty() && !topics.contains(&sp.topic) {
        return None;
    }

    // 2. 创建新的 TopicAssignment 用于存储过滤结果
    let mut filtered_assignment = vec![];

    // 3. 过滤分区，broker, keys
    for entry in sp.partitions.iter() {
        // 检查分区 ID 是否匹配
        let match_partition = partition_ids.is_empty() || partition_ids.contains(&entry.id);

        // 检查 Broker 是否匹配（主副本或从副本）
        let match_broker = broker_ids.is_empty() || broker_ids.contains(&entry.broker_leader_id);
        // TODO： 下面的根据 broker_follower_ids 先隐藏
        // || entry
        //     .broker_follower_ids
        //     .iter()
        //     .any(|f| broker_ids.contains(f));

        // 检查 Key 是否匹配
        let match_key = keys.is_empty() || entry.pub_keys.iter().any(|k| keys.contains(k));

        if match_partition && match_broker && match_key {
            filtered_assignment.push(entry.clone());
        }
    }

    // 5. 如果没有任何分区匹配，则返回 None
    if filtered_assignment.is_empty() {
        return None;
    }

    // 6. 返回过滤后的 SinglePartition
    Some(SinglePartition {
        unique_id: sp.unique_id,
        topic: sp.topic,
        partitions: filtered_assignment,
    })
}

impl From<SinglePartition> for commonsvc::TopicPartitionResp {
    fn from(sp: SinglePartition) -> Self {
        let mut tp = commonsvc::TopicPartitionResp::default();
        tp.list = sp
            .partitions
            .iter()
            .map(|v| v.convert_to_topic_partition_meta(&sp.topic))
            .collect();

        tp
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::coo::Coordinator;
    use crate::default_config;
    #[tokio::test]
    async fn qeury_topics() {
        let id = 1;
        let mut conf = default_config();
        let db_path = conf.db_path.clone();
        conf = conf
            .with_id(id)
            .with_db_path(Path::new("..").join(&db_path).join(format!("coo{}", id)));
        let coo = Coordinator::new("".to_string(), conf);

        let ts = coo
            .qeury_topics(&["mytopic1".to_string()], &[], &[], &[])
            .await;
        println!("ts = {:?}", ts);
    }
}
