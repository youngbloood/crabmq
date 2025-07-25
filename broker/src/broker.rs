use crate::{
    config::Config, consumer_group::ConsumerGroupManager, message_bus::MessageBus,
    partition::PartitionManager,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use grpcx::{
    brokercoosvc::{BrokerState, SyncConsumerAssignmentsResp},
    clientbrokersvc::{
        MessageBatch, PublishReq, PublishResp, Status as BrokerStatus, SubscribeReq,
        client_broker_service_server::{ClientBrokerService, ClientBrokerServiceServer},
        subscribe_req,
    },
    commonsvc::TopicPartitionResp,
    topic_meta::TopicPartitionDetail,
};
use log::{debug, error, info};
use std::{sync::Arc, time::Duration};
use storagev2::{MessagePayload, StorageReader, StorageWriter};
use sysinfo::System;
use tokio::{
    sync::{RwLock, mpsc},
    time::{self, interval},
};
use tonic::{Request, Response, Status, async_trait, transport::Server};

#[derive(Clone)]
pub struct Broker<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    conf: Config,

    storage_writer: Arc<SW>,
    partitions: PartitionManager,
    consumers: ConsumerGroupManager<SR>,
    message_bus: MessageBus,

    metrics: Arc<RwLock<BrokerState>>,
    state_bus: Arc<DashMap<String, mpsc::Sender<BrokerState>>>,
}

impl<SW, SR> Broker<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    pub fn new(conf: Config, sw: SW, sr: SR) -> Self {
        Self {
            storage_writer: Arc::new(sw),
            partitions: PartitionManager::new(conf.id),
            consumers: ConsumerGroupManager::new(conf.subscriber_timeout, sr),
            message_bus: MessageBus::new(
                conf.message_bus_producer_buffer_size,
                conf.message_bus_consumer_buffer_size,
            ),
            state_bus: Arc::new(DashMap::new()),
            metrics: Arc::new(RwLock::new(BrokerState {
                id: conf.id,
                addr: conf.broker_addr.clone(),
                ..Default::default()
            })),
            conf,
        }
    }
}

#[async_trait]
impl<SW, SR> ClientBrokerService for Broker<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    type PublishStream = tonic::codegen::BoxStream<PublishResp>;
    type SubscribeStream = tonic::codegen::BoxStream<MessageBatch>;

    async fn publish(
        &self,
        request: Request<tonic::Streaming<PublishReq>>,
    ) -> Result<Response<Self::PublishStream>, Status> {
        let remote_addr = request.remote_addr().unwrap();
        let mut strm = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(self.conf.publish_buffer_size);

        let broker_id = self.conf.id;
        let broker = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = strm.message().await {
                broker.process_publish(req, &tx).await;
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<SubscribeReq>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let remote_addr = request.remote_addr().unwrap().to_string();
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(self.conf.subscribe_buffer_size);
        let id = self.get_id();
        let broker = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                match broker
                    .process_subscribe(&remote_addr, req, tx.clone())
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Broker[{}] handle subscribe err: {e:?}", id);
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

impl<SW, SR> Broker<SW, SR>
where
    SW: StorageWriter,
    SR: StorageReader,
{
    #[inline]
    pub fn get_id(&self) -> u32 {
        self.conf.id
    }

    pub fn get_state_reciever(&self, module: String) -> mpsc::Receiver<BrokerState> {
        let (tx, rx) = mpsc::channel(self.conf.state_bus_buffer_size);
        self.state_bus.insert(module, tx);
        rx
    }

    /// 释放 module 模块的发送器，该模块的 rx 会收到错误
    pub fn unleash_state_reciever(&self, module: &str) {
        if let Some((m, _tx)) = self.state_bus.remove(module) {
            debug!("unleash state reciever: {}", m);
        }
    }

    pub async fn get_state(&self) -> BrokerState {
        self.metrics.read().await.clone()
    }

    pub fn apply_topic_infos(&self, tpr: TopicPartitionResp) {
        let list = tpr.list.iter().map(TopicPartitionDetail::from).collect();
        self.partitions.apply_topic_infos(list)
    }

    pub async fn apply_consumergroup(&self, s: SyncConsumerAssignmentsResp) -> Result<()> {
        if s.broker_id != self.conf.id {
            return Err(anyhow!("this message not belong the broker"));
        }
        println!("broker收到  ==== {:?}", s);
        self.consumers.apply_consumergroup(s).await;
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let broker = self.clone();
        let report_broker_state_interval = self.conf.report_broker_state_interval;
        // 定时发送 BrokerState 至 state_bus
        let broker_state_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(report_broker_state_interval));
            let timeout = Duration::from_millis(10);
            loop {
                ticker.tick().await;
                let state = broker.get_state().await;
                for sender in broker.state_bus.iter() {
                    if let Err(e) = sender.send_timeout(state.clone(), timeout).await {
                        error!("Send BrokerState to {} err: {:?}", sender.key(), e);
                    }
                }
            }
        });

        let broker = self.clone();
        // 启动 grpc service
        let broker_socket = self
            .conf
            .broker_addr
            .parse()
            .expect("need correct socket addr");
        let server_handle = tokio::spawn(async move {
            let svc = ClientBrokerServiceServer::new(broker);
            let server = Server::builder().add_service(svc);
            info!("Broker listen: {}", broker_socket);
            match server.serve(broker_socket).await {
                Ok(_) => {
                    info!("Broker service listen at: {}", broker_socket);
                }
                Err(e) => {
                    error!(
                        "Broker service listen at: {} failed: {:?}",
                        broker_socket, e
                    );
                }
            }
        });

        // 定期获取 broker 所在节点的状态
        let metrics = self.metrics.clone();
        let metrics_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            let mut sys = System::new_all();
            loop {
                interval.tick().await;

                let mut metrics = metrics.write().await;
                metrics.version = chrono::Local::now().timestamp() as _;
                // 更新系统指标
                sys.refresh_all();

                // 获取CPU使用率（平均所有核心）
                metrics.cpurate = sys.global_cpu_usage() as u32;

                // 获取内存使用率
                let mem_usage = (sys.used_memory() as f64 / sys.total_memory() as f64) * 100.0;
                metrics.memrate = mem_usage as u32;

                // 获取磁盘使用率（假设第一个磁盘是数据盘）
                // let disk_usage = sys
                //     .disks()
                //     .iter()
                //     .find(|d| {
                //         d.type_() == sysinfo::DiskType::SSD || d.type_() == sysinfo::DiskType::HDD
                //     })
                //     .map(|disk| {
                //         let used = disk.total_space() - disk.available_space();
                //         (used as f64 / disk.total_space() as f64) * 100.0
                //     })
                //     .unwrap_or(0.0);
                // metrics.diskrate.store(disk_usage as u32, Ordering::Relaxed);

                // 获取网络速率（所有接口的总和）
                // let net_rate = sys
                //     .networks()
                //     .values()
                //     .map(|net| net.received() + net.transmitted())
                //     .sum::<u64>()
                //     / 1024; // 转换为KB/s
                // metrics.netrate.store(net_rate as u32, Ordering::Relaxed);
            }
        });

        tokio::try_join!(broker_state_handle, server_handle, metrics_handle)?;
        Ok(())
    }

    async fn process_publish(
        &self,
        mut req: PublishReq,
        tx: &mpsc::Sender<Result<PublishResp, Status>>,
    ) {
        // 验证分区归属
        if !self.partitions.is_my_partition(&req.topic, req.partition) {
            let _ = tx.send(Err(BrokerError::InvalidPartition.into())).await;
            return;
        }
        // println!(
        //     "broker[{}] 收到分区[{}]的消息",
        //     self.get_id(),
        //     req.partition
        // );

        if req.ack() == grpcx::clientbrokersvc::PublishAckType::MasterMem {
            let _ = tx
                .send(Ok(PublishResp {
                    batch_id: String::new(),
                    overall_status: 0,
                    message_acks: Vec::new(),
                }))
                .await;
        }

        let mut msgs = Vec::with_capacity(req.messages.len());
        for v in req.messages.iter_mut() {
            msgs.push(MessagePayload {
                msg_id: std::mem::take(&mut v.message_id),
                timestamp: 0,
                metadata: std::mem::take(&mut v.metadata),
                payload: std::mem::take(&mut v.payload),
            });
        }

        let req = Arc::new(req);
        // 存储消息
        if let Err(e) = self
            .storage_writer
            .store(&req.topic, req.partition, msgs, None)
            .await
            .map_err(|e| {
                error!(
                    "process_publish req[{}-{}] error: {:?}",
                    req.topic, req.partition, e,
                );
                BrokerError::StorageFailure
            })
        {}

        if req.ack() == grpcx::clientbrokersvc::PublishAckType::MasterStorage {
            let _ = tx
                .send(Ok(PublishResp {
                    batch_id: String::new(),
                    overall_status: 0,
                    message_acks: Vec::new(),
                }))
                .await;
        }

        // 写入消息总线
        self.message_bus
            .broadcast_producer_message(&req.topic, req.partition, req.clone())
            .await;
    }

    async fn process_subscribe(
        &self,
        client_addr: &str,
        req: SubscribeReq,
        tx: mpsc::Sender<Result<MessageBatch, Status>>,
    ) -> Result<(), Status> {
        match req.request {
            Some(subscribe_req::Request::Sub(sub)) => {
                // 创建会话
                self.consumers
                    .new_sesssion(sub.member_id, client_addr.to_string())
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
            }

            Some(subscribe_req::Request::Fetch(fetch)) => {
                // 更新流量控制
                let sess = self.consumers.get_session(client_addr);
                if sess.is_none() {
                    // 统一由外部处理
                    return Err(tonic::Status::new(
                        tonic::Code::InvalidArgument,
                        "Must be sub firstly",
                    ));
                }
                let sess = sess.unwrap();
                // 处理ACK逻辑
                sess.handle_fetch(fetch, tx).await;
            }

            Some(subscribe_req::Request::Commit(commit)) => {
                // 更新流量控制
                let sess = self.consumers.get_session(client_addr);
                if sess.is_none() {
                    // 统一由外部处理
                    return Err(tonic::Status::new(
                        tonic::Code::InvalidArgument,
                        "Must be sub firstly",
                    ));
                }
                let sess = sess.unwrap();
                // 处理ACK逻辑
                sess.handle_commit(commit, tx).await;
            }

            Some(subscribe_req::Request::Heartbeat(hb)) => {}

            None => return Err(tonic::Status::new(tonic::Code::InvalidArgument, "Unkown ")),
        }
        Ok(())
    }

    // async fn push_messages(
    //     &self,
    //     mut session: SubSession<T>,
    //     tx: mpsc::Sender<Result<Message, Status>>,
    // ) {
    //     loop {
    //         match session.next().await {
    //             Ok(msg) => {
    //                 tx.send(Ok(msg.clone())).await;
    //                 self.message_bus
    //                     .broadcast_consumer_message("topic", 1, Bytes::new())
    //                     .await
    //             }
    //             Err(status) => {
    //                 if status.code() == tonic::Code::DeadlineExceeded {
    //                     tx.send(Err(status)).await;
    //                     break;
    //                 }
    //             }
    //         }
    //     }
    // }
}

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("Storage operation failed")]
    StorageFailure,
    #[error("Partition not owned by this node")]
    InvalidPartition,
    #[error("Already subscribed to this partition")]
    AlreadySubscribed,
    #[error("Invalid request format")]
    InvalidRequest,
}

impl From<BrokerError> for Status {
    fn from(e: BrokerError) -> Self {
        match e {
            BrokerError::StorageFailure => Status::internal(e.to_string()),
            BrokerError::InvalidPartition => Status::failed_precondition(e.to_string()),
            BrokerError::AlreadySubscribed => Status::already_exists(e.to_string()),
            _ => Status::invalid_argument(e.to_string()),
        }
    }
}
