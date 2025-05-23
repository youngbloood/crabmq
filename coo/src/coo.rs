use super::partition;
use super::raftx;
use crate::event_bus::EventBus;
use crate::partition::PartitionInfo;
use crate::partition::PartitionPolicy;
use crate::raftx::repair_addr_with_http;
use crate::{BrokerNode, ClientNode};
use anyhow::Result;
use dashmap::DashMap;
use grpcx::brokercoosvc::broker_coo_service_server::BrokerCooServiceServer;
use grpcx::clientcoosvc::client_coo_service_server::ClientCooServiceServer;
use grpcx::commonsvc;
use grpcx::commonsvc::CooInfo;
use grpcx::commonsvc::CooListResp;
use grpcx::commonsvc::TopicListAdd;
use grpcx::commonsvc::TopicListInit;
use grpcx::commonsvc::topic_list;
use grpcx::{brokercoosvc, clientcoosvc};
use log::error;
use log::info;
use partition::PartitionManager;
use raftx::{MessageType as AllMessageType, RaftNode};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Server;

#[derive(Clone)]
pub struct Coordinator {
    pub id: u32,
    // coordinator 节点监听的 grpc 地址
    pub coo_addr: String,
    // coordinator 节点间的 raft 通信模块监听的 grpc 地址
    pub raft_addr: String,
    // coordinator 节点指向的 raft leader 节点地址 （raft-leader 监听的地址）
    raft_leader_addr: String,

    // raft_node 节点
    raft_node: Arc<RaftNode>,
    // 向 raft_node 节点发送消息的通道
    raft_node_sender: mpsc::Sender<AllMessageType>,

    // coo: leader 收集到的 broker 上报信息
    brokers: Arc<DashMap<u32, BrokerNode>>,

    // 链接 coo 的客户端
    clients: Arc<DashMap<String, ClientNode>>,

    // (Leadder)集群的 Topic-Partition 变更事件总线
    broker_event_bus: EventBus<PartitionEvent>,
    client_event_bus: EventBus<PartitionEvent>,

    //（Leader）集群节点变更事件总线
    peer_change_bus: EventBus<Result<CooListResp, Status>>,

    // 分区管理
    partition_mgr: PartitionManager,
}

impl Coordinator {
    pub fn new<T: AsRef<std::path::Path>>(
        id: u32,
        db_path: T,
        coo_addr: String,
        raft_addr: String,
        raft_leader_addr: String,
    ) -> Self {
        let (raft_node, raft_node_sender) =
            RaftNode::new(id, db_path, raft_addr.clone(), coo_addr.clone());
        let raft_node = Arc::new(raft_node);

        println!("coo_addr = {}, raft_addr ={}", coo_addr, raft_addr);
        Self {
            id,
            coo_addr,
            raft_addr,
            raft_leader_addr,
            raft_node: raft_node.clone(),
            raft_node_sender,
            brokers: Arc::new(DashMap::new()),
            clients: Arc::new(DashMap::new()),
            broker_event_bus: EventBus::new(),
            client_event_bus: EventBus::new(),
            peer_change_bus: EventBus::new(),
            partition_mgr: PartitionManager::new(PartitionPolicy::default()),
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

    /// broker_pull
    ///
    /// 用于本地的 `broker` 向 coo 报道自身信息
    pub fn broker_report(&self, state: brokercoosvc::BrokerState) {
        let id = state.id;
        self.brokers
            .entry(id)
            .and_modify(|entry| {
                // 修改值
                entry.state.memrate = state.memrate;
            })
            .or_insert_with(|| BrokerNode { state });
    }

    /// 启动 Coordinator 服务
    pub async fn run(&self) -> Result<()> {
        // 定期检查本节点从: 主 -> 非主，并发送 NotLeader 消息，用户通知 broker 和 client 变更链接逻辑
        self.start_checker_loop();

        // 启动 raft-node: 加入目标节点，并启动 raft 通信的 grpc 服务
        let raft_node = self.raft_node.clone();
        let raft_leader_addr = self.raft_leader_addr.clone();
        tokio::spawn(async move {
            let raft_node_clone = raft_node.clone();
            tokio::spawn(async move {
                raft_node_clone.run().await;
            });
            if !raft_leader_addr.is_empty() {
                let _ = raft_node.join(raft_leader_addr).await;
            }
        });

        let brokercoo_svc = BrokerCooServiceServer::new(self.clone());
        let clientcoo_svc = ClientCooServiceServer::new(self.clone());
        let addr = self.coo_addr.parse().unwrap();

        info!("Coordinator listen: {}", addr);
        match Server::builder()
            .add_service(brokercoo_svc)
            .add_service(clientcoo_svc)
            .serve(addr)
            .await
        {
            Ok(_) => {
                info!("Coordinator server started at {}", addr);
            }
            Err(e) => panic!("Coordinator listen : {}, err: {:?}", addr, e),
        }
        Ok(())
    }

    // 定期检查本节点从: 主 -> 非主，并发送 NotLeader 消息，用户通知 broker 和 client 变更链接逻辑
    fn start_checker_loop(&self) {
        let raft_node = self.raft_node.clone();
        let broker_event_bus = self.broker_event_bus.clone();
        let client_event_bus = self.client_event_bus.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(2));
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
    }

    /// broker_pull
    ///
    /// 用于本地的 `broker` 获取 coo 中的 TopicList 最新数据
    pub fn broker_pull(
        &self,
        broker_id: u32,
    ) -> Result<mpsc::UnboundedReceiver<Result<commonsvc::TopicList, Status>>> {
        let id = format!("local_broker_{}", broker_id);
        let (_, mut recver) = self.broker_event_bus.subscribe(id.clone())?;
        let bus = self.broker_event_bus.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        let init = self.get_all_topics("", &[broker_id], &[]);
        for v in init {
            let _ = tx.send(Ok(v));
        }

        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            while let Some(part) = recver.recv().await {
                match part {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        let coo_leader_addr = raft_node
                            .get_coo_leader_addr()
                            .await
                            .unwrap_or_else(|| "unknown".to_string());

                        let mut status = tonic::Status::new(
                            tonic::Code::PermissionDenied,
                            format!(
                                "Leader changed to [{}:{}:{}]",
                                new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                            ),
                        );

                        // 添加 leader 地址到 metadata
                        status
                            .metadata_mut()
                            .insert("x-raft-leader", coo_leader_addr.parse().unwrap());
                        let _ = tx.send(Err(status));
                        break;
                    }
                    PartitionEvent::NewTopic { topic, partitions } => {
                        let _ = tx.send(Ok(convert_to_pull_resp(
                            topic,
                            partitions,
                            raft_node.get_term().await,
                        )));
                    }
                    PartitionEvent::AddPartitions { topic, added } => {
                        let _ = tx.send(Ok(convert_to_added_resp(
                            topic,
                            added,
                            raft_node.get_term().await,
                        )));
                    }
                }
            }
            bus.unsubscribe(&id);
        });

        Ok(rx)
    }
}

impl Coordinator {
    fn get_broker_assignments(&self, broker_id: u32) -> Vec<commonsvc::TopicList> {
        // 实现获取当前分区信息的逻辑
        vec![]
    }

    /// get_all_topics: 获取所有的 topic
    ///
    /// 根据条件过滤：
    ///
    /// 1. 根据 `topic` 过滤
    /// 2. 根据 `broker_id` 过滤
    /// 3. 根据 `partition` 过滤
    fn get_all_topics(
        &self,
        topic: &str,
        broker_ids: &[u32],
        partitions: &[u32],
    ) -> Vec<commonsvc::TopicList> {
        vec![]
    }

    async fn propose(&self, topic: &str, partitions: Arc<Vec<PartitionInfo>>) -> Result<()> {
        // propose: key = {topic}:{part_id}, value = [broker_ids] : 用于根据 topic 查询
        // propose: key = {broker_id}:{topic}, value = [part_ids] : 用于根据 broker_id 查询
        for i in 0..partitions.len() {
            let part = &partitions[i];
            self.raft_node
                .propose(&format!("{}:{}", topic, part.id), &part.brokers)
                .await?;

            for broker_id in &part.brokers {
                self.raft_node
                    .propose(&format!("{}:{}", broker_id, topic,), &vec![part.id])
                    .await?;
            }
        }
        Ok(())
    }

    async fn get_term(&self) -> u64 {
        self.raft_node.get_term().await
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
        topic: String,
        partitions: Arc<Vec<PartitionInfo>>,
    },
    AddPartitions {
        topic: String,
        added: Arc<Vec<PartitionInfo>>,
    },
}

#[tonic::async_trait]
impl brokercoosvc::broker_coo_service_server::BrokerCooService for Coordinator {
    type ReportStream = tonic::codegen::BoxStream<brokercoosvc::BrokerStateResp>;
    type PullStream = tonic::codegen::BoxStream<commonsvc::TopicList>;
    type ListStream = tonic::codegen::BoxStream<commonsvc::CooListResp>;

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

        let sub_id = format!("broker_{}_{}", broker_id, remote_addr);
        let (tx, rx) = self.peer_change_bus.subscribe(sub_id).unwrap();

        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(CooListResp {
                    cluster_term: raft_node.get_term().await,
                    list,
                }))
                .await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // /// Server streaming response type for the report method.
    async fn report(
        &self,
        request: tonic::Request<tonic::Streaming<brokercoosvc::BrokerState>>,
    ) -> std::result::Result<tonic::Response<Self::ReportStream>, tonic::Status> {
        let mut strm = request.into_inner();

        let coo_id = self.id;
        let brokers = self.brokers.clone();

        let output_stream = async_stream::try_stream! {
            while let Ok(Some(state)) = strm.message().await {
                let id = state.id;
                info!("Coo[{}] handle Broker[{}] BrokerState",coo_id,id);

                brokers.entry(id).and_modify(|entry| {
                    // 修改值
                    entry.state.memrate = state.memrate;
                }).or_insert_with(|| {
                    BrokerNode { state }
                });

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
        if !self.raft_node.is_leader().await {
            let coo_leader_addr = self
                .raft_node
                .get_coo_leader_addr()
                .await
                .unwrap_or_else(|| "unknown".to_string());

            let mut status =
                tonic::Status::new(tonic::Code::PermissionDenied, "Not Leader".to_string());

            // 添加 leader 地址到 metadata
            status
                .metadata_mut()
                .insert("x-raft-leader", coo_leader_addr.parse().unwrap());
            return Err(status);
        }

        let remote_addr = request.remote_addr().unwrap();
        let req = request.into_inner();
        let broker_id = req.id;
        let sub_id = format!("broker_{}_{}", broker_id, remote_addr);
        // 获取事件流
        let mut event_rx = match self.broker_event_bus.subscribe(sub_id.clone()) {
            Ok((_, event_rx)) => event_rx,
            Err(e) => {
                error!("BrokerCooService.pull err: {:?}", e);
                return Err(tonic::Status::already_exists(broker_id.to_string()));
            }
        };
        let bus = self.broker_event_bus.clone();
        let init_data = self.get_broker_assignments(broker_id);
        let (resp_tx, resp_rx) = mpsc::channel(128);
        let raft_node = self.raft_node.clone();

        tokio::spawn(async move {
            // 发送初始数据
            for resp in init_data {
                let _ = resp_tx.send(Ok(resp)).await;
            }

            // 监听事件
            while let Some(event) = event_rx.recv().await {
                match event {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        let mut status = tonic::Status::new(
                            tonic::Code::PermissionDenied,
                            format!(
                                "Leader changed to [{}:{}:{}]",
                                new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                            ),
                        );

                        // 添加新的 leader 地址到 metadata
                        status
                            .metadata_mut()
                            .insert("x-raft-leader", new_coo_leader_addr.parse().unwrap());

                        // 发送错误并终止流
                        let _ = resp_tx.send(Err(status)).await;
                        break;
                    }
                    PartitionEvent::NewTopic { topic, partitions } => {
                        let _ = resp_tx
                            .send(Ok(convert_to_pull_resp(
                                topic,
                                partitions,
                                raft_node.get_term().await,
                            )))
                            .await;
                    }
                    // 处理其他事件类型...
                    PartitionEvent::AddPartitions { topic, added } => {
                        let _ = resp_tx
                            .send(Ok(convert_to_added_resp(
                                topic,
                                added,
                                raft_node.get_term().await,
                            )))
                            .await;
                    }
                }
            }
            // 连接结束时取消订阅
            bus.unsubscribe(&sub_id);
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }
}

#[tonic::async_trait]
impl clientcoosvc::client_coo_service_server::ClientCooService for Coordinator {
    type PullStream = tonic::codegen::BoxStream<commonsvc::TopicList>;
    type ListStream = tonic::codegen::BoxStream<commonsvc::CooListResp>;

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
        todo!()
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
        request: tonic::Request<clientcoosvc::AddTopicReq>,
    ) -> std::result::Result<tonic::Response<clientcoosvc::AddTopicResp>, tonic::Status> {
        let req = request.into_inner();

        let partitions =
            self.partition_mgr
                .assign_new_topic(&req.topic, req.partition, self.brokers.clone());

        self.propose(&req.topic, Arc::clone(&partitions)).await;

        // 发送通知
        self.broker_event_bus
            .broadcast(PartitionEvent::NewTopic {
                topic: req.topic.clone(),
                partitions: Arc::clone(&partitions),
            })
            .await;

        self.client_event_bus
            .broadcast(PartitionEvent::NewTopic {
                topic: req.topic,
                partitions,
            })
            .await;

        Ok(Response::new(clientcoosvc::AddTopicResp {
            success: false,
            error: String::new(),
        }))
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
        let req = request.into_inner();
        if !self.partition_mgr.has_topic(&req.topic) {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                format!("Not has topic: {}", &req.token),
            ));
        }
        if req.partition == 0 {
            return Err(tonic::Status::new(
                tonic::Code::InvalidArgument,
                "partition must be greater than zero",
            ));
        }
        let added =
            self.partition_mgr
                .add_partitions(&req.topic, req.partition, self.brokers.clone());

        self.propose(&req.topic, Arc::clone(&added)).await;

        // 发送通知
        self.broker_event_bus
            .broadcast(PartitionEvent::NewTopic {
                topic: req.topic.clone(),
                partitions: Arc::clone(&added),
            })
            .await;

        self.client_event_bus
            .broadcast(PartitionEvent::NewTopic {
                topic: req.topic,
                partitions: added,
            })
            .await;

        Ok(Response::new(clientcoosvc::AddPartitionsResp {
            success: false,
            error: String::new(),
        }))
    }

    /// Server streaming response type for the pull method.
    async fn pull(
        &self,
        request: tonic::Request<clientcoosvc::PullReq>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        if !self.raft_node.is_leader().await {
            let coo_leader_addr = self
                .raft_node
                .get_coo_leader_addr()
                .await
                .unwrap_or_else(|| "unknown".to_string());

            let mut status =
                tonic::Status::new(tonic::Code::PermissionDenied, "Not Leader".to_string());

            // 添加 leader 地址到 metadata
            status
                .metadata_mut()
                .insert("x-raft-leader", coo_leader_addr.parse().unwrap());

            return Err(status);
        }
        let remote_addr = request.remote_addr().unwrap().to_string();
        let req = request.into_inner();
        let sub_id = format!("client_{}", remote_addr);
        let mut event_rx = match self.client_event_bus.subscribe(sub_id.clone()) {
            Ok((_, rx)) => rx,
            Err(_) => return Err(tonic::Status::already_exists(remote_addr)),
        };

        let init_data = self.get_all_topics(&req.topic, &req.broker_ids, &req.partitions);
        let bus: EventBus<PartitionEvent> = self.client_event_bus.clone();
        let (resp_tx, resp_rx) = mpsc::channel(128);
        let raft_node = self.raft_node.clone();
        tokio::spawn(async move {
            for resp in init_data {
                resp_tx.send(Ok(resp)).await;
            }

            while let Some(event) = event_rx.recv().await {
                match event {
                    PartitionEvent::NotLeader {
                        new_leader_id,
                        new_coo_leader_addr,
                        new_raft_leader_addr,
                    } => {
                        let mut status = tonic::Status::new(
                            tonic::Code::PermissionDenied,
                            format!(
                                "Leader changed to [{}:{}:{}]",
                                new_leader_id, new_coo_leader_addr, new_raft_leader_addr
                            ),
                        );

                        // 添加新的 leader 地址到 metadata
                        status
                            .metadata_mut()
                            .insert("x-raft-leader", new_coo_leader_addr.parse().unwrap());

                        // 发送错误并终止流
                        resp_tx.send(Err(status)).await;
                        break;
                    }
                    PartitionEvent::NewTopic { topic, partitions } => {
                        resp_tx
                            .send(Ok(convert_to_pull_resp(
                                topic,
                                partitions,
                                raft_node.get_term().await,
                            )))
                            .await;
                    }
                    // 处理其他事件...
                    PartitionEvent::AddPartitions { topic, added } => {
                        resp_tx
                            .send(Ok(convert_to_added_resp(
                                topic,
                                added,
                                raft_node.get_term().await,
                            )))
                            .await;
                    }
                }
            }
            bus.unsubscribe(&sub_id);
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }
}

// 转换函数示例
fn convert_to_pull_resp(
    topic: String,
    ps: Arc<Vec<PartitionInfo>>,
    term: u64,
) -> commonsvc::TopicList {
    commonsvc::TopicList {
        term,
        list: Some(topic_list::List::Init(TopicListInit { topics: Vec::new() })),
    }
}

// 转换函数示例
fn convert_to_added_resp(
    topic: String,
    ps: Arc<Vec<PartitionInfo>>,
    term: u64,
) -> commonsvc::TopicList {
    commonsvc::TopicList {
        term,
        list: Some(topic_list::List::Add(TopicListAdd { topics: Vec::new() })),
    }
}
