use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use grpcx::{
    brokercoosvc::BrokerState,
    clientbrokersvc::{
        Message, PublishReq, PublishResp, Status as BrokerStatus, SubscribeReq,
        client_broker_service_server::{ClientBrokerService, ClientBrokerServiceServer},
        subscribe_req,
    },
    commonsvc::TopicList,
};
use log::{debug, error, info};
use std::{sync::Arc, time::Duration};
use storagev2::Storage;
use tokio::{sync::mpsc, time::interval};
use tonic::{Request, Response, Status, async_trait, transport::Server};

use crate::{
    consumer_group::{ConsumerGroup, SubSession},
    message_bus::MessageBus,
    partition::PartitionManager,
};

#[derive(Clone)]
pub struct Broker<T: Storage> {
    id: u32,
    broker_addr: String,
    storage: T,
    partitions: PartitionManager,
    consumers: ConsumerGroup<T>,
    message_bus: MessageBus,

    state_bus: Arc<DashMap<String, mpsc::Sender<BrokerState>>>,
}

impl<T: Storage> Broker<T> {
    pub fn new(id: u32, broker_addr: String, coo_addr: String, storage: T) -> Self {
        Self {
            id,
            broker_addr,
            storage,
            partitions: PartitionManager::new(id),
            consumers: ConsumerGroup::new(),
            message_bus: MessageBus::new(),
            state_bus: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl<T: Storage> ClientBrokerService for Broker<T> {
    type PublishStream = tonic::codegen::BoxStream<PublishResp>;
    type SubscribeStream = tonic::codegen::BoxStream<Message>;

    async fn publish(
        &self,
        request: Request<tonic::Streaming<PublishReq>>,
    ) -> Result<Response<Self::PublishStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let broker = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                let result = broker.process_publish(req).await;
                let _ = tx.send(result.map_err(Into::into)).await;
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
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let broker = self.clone();
        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                match broker.process_subscribe(&remote_addr, req, &tx).await {
                    Ok(_) => {}
                    Err(e) => {
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

impl<T: Storage> Broker<T> {
    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn get_state_reciever(&self, module: String) -> mpsc::Receiver<BrokerState> {
        let (tx, rx) = mpsc::channel(1);
        self.state_bus.insert(module, tx);
        rx
    }

    /// 释放 module 模块的发送器，该模块的 rx 会收到错误
    pub fn unleash_state_reciever(&self, module: &str) {
        if let Some((m, _tx)) = self.state_bus.remove(module) {
            debug!("unleash state reciever: {}", m);
        }
    }

    pub fn get_state(&self) -> BrokerState {
        BrokerState::default()
    }

    pub fn apply_topic_infos(&self, tl: TopicList) {
        self.partitions.apply_topic_infos(tl)
    }

    pub async fn run(&self) -> Result<()> {
        let broker = self.clone();
        // 定时发送 BrokerState 至 state_bus
        let broker_state_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            let timeout = Duration::from_millis(100);
            loop {
                ticker.tick().await;
                let state = broker.get_state();
                for sender in broker.state_bus.iter() {
                    if let Err(e) = sender.send_timeout(state.clone(), timeout).await {
                        error!("Send BrokerState to {} err: {:?}", sender.key(), e);
                    }
                }
            }
        });

        let broker = self.clone();
        // 启动 grpc service
        let broker_socket = self.broker_addr.parse().expect("need correct socket addr");
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

        tokio::try_join!(broker_state_handle, server_handle);
        Ok(())
    }

    async fn process_publish(&self, req: PublishReq) -> Result<PublishResp, BrokerError> {
        // 验证分区归属
        if !self.partitions.is_my_partition(&req.topic, req.partition) {
            return Err(BrokerError::InvalidPartition);
        }

        let payload = Bytes::from(req.payload);
        // 存储消息
        let message_id = self
            .storage
            .store(&req.topic, req.partition, payload.clone())
            .await
            .map_err(|e| {
                error!(
                    "process_publish req[{}-{}] error: {:?}",
                    req.topic, req.partition, e,
                );
                BrokerError::StorageFailure
            })?;

        // 写入消息总线
        self.message_bus
            .broadcast_producer_message(&req.topic, req.partition, payload)
            .await;

        Ok(PublishResp {
            message_id: String::new(),
            status: BrokerStatus::Success.into(),
        })
    }

    async fn process_subscribe(
        &self,
        client_addr: &str,
        req: SubscribeReq,
        tx: &tokio::sync::mpsc::Sender<Result<Message, Status>>,
    ) -> Result<(), Status> {
        match req.request {
            Some(subscribe_req::Request::Sub(sub)) => {
                // 验证分区归属
                if !self.partitions.is_my_partition(&sub.topic, sub.partition) {
                    return Err(Status::new(
                        tonic::Code::InvalidArgument,
                        "Invalid partition",
                    ));
                }

                // 检查是否已有订阅
                if self.consumers.has_subscription(&sub.topic, sub.partition) {
                    return Err(Status::new(
                        tonic::Code::PermissionDenied,
                        "Partition has been subscribed",
                    ));
                }

                // 创建会话
                let session: SubSession<T> = SubSession::new(
                    sub.topic.clone(),
                    sub.partition,
                    client_addr.to_string(),
                    self.storage.clone(),
                );

                // 启动消息推送
                let broker = self.clone();
                let tx_clone = tx.clone();
                tokio::spawn(async move {
                    broker.push_messages(session, tx_clone).await;
                });
            }
            Some(subscribe_req::Request::Ack(ack)) => {
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
                sess.handle_ack(ack);
            }
            Some(subscribe_req::Request::Flow(flow)) => {
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
                sess.handle_flow(flow);
            }
            None => return Err(tonic::Status::new(tonic::Code::InvalidArgument, "Unkown ")),
        }
        Ok(())
    }

    async fn push_messages(
        &self,
        mut session: SubSession<T>,
        tx: mpsc::Sender<Result<Message, Status>>,
    ) {
        loop {
            match session.next().await {
                Ok(msg) => {
                    tx.send(Ok(msg.clone())).await;
                    self.message_bus
                        .broadcast_consumer_message("topic", 1, Bytes::new())
                        .await
                }
                Err(status) => {
                    if status.code() == tonic::Code::DeadlineExceeded {
                        tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        }
    }
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
