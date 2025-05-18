use crate::message_bus::MessageBus;
use bytes::Bytes;
use dashmap::DashMap;
use grpcx::brokercoosvc::BrokerState;
use grpcx::brokercoosvc::broker_coo_service_client::BrokerCooServiceClient;
use grpcx::clientbrokersvc;
use grpcx::commonsvc::TopicList;
use log::error;
use std::{pin::Pin, sync::Arc, time::Duration};
use storagev2::Storage;
use tokio::{sync::mpsc, time::interval};
use tonic::transport::Channel;
pub struct Broker<T>
where
    T: Storage,
{
    id: u32,
    addr: String,
    topics: Arc<DashMap<String, Vec<Partition>>>,

    msg_bus: MessageBus<Bytes>,

    coos: Arc<DashMap<u32, CooNode>>,
    storage: T,
}

unsafe impl<T> Send for Broker<T> where T: Storage {}
unsafe impl<T> Sync for Broker<T> where T: Storage {}
struct BrokerStateManager {}

impl BrokerStateManager {
    /// 获取 broker 所在节点的节点信息
    pub fn get_state(&self) -> BrokerState {
        BrokerState {
            id: 0,
            addr: String::new(),
            version: 0,
            wps: 0,
            wbps: 0,
            rps: 0,
            rbps: 0,
            netrate: 0,
            cpurat: 0,
            memrate: 0,
            diskrate: 0,
            slaves: Vec::new(),
            topic_infos: Vec::new(),
        }
    }
}
struct CooNode {
    id: u32,
    addr: String,
    status: u32,
    client: Option<BrokerCooServiceClient<Channel>>,
}

struct Partition {
    id: u32,
    path: String,
}

impl<T> Broker<T>
where
    T: Storage,
{
    pub fn new(id: u32, addr: String, storage: T) -> Self {
        Self {
            id,
            addr,
            topics: Arc::new(DashMap::default()),
            coos: Arc::new(DashMap::default()),
            storage,
            msg_bus: MessageBus::new(),
        }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    fn get_state_manager(&self) -> BrokerStateManager {
        BrokerStateManager {}
    }

    /// 应用 topics 信息至本 broker 节点
    pub fn apply_topic_info(&self, topics: TopicList) {
        todo!()
    }

    pub fn get_state_reciever(&self) -> mpsc::Receiver<BrokerState> {
        let (tx, rx) = mpsc::channel(1);
        let state_mngr = self.get_state_manager();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(150));
            loop {
                ticker.tick().await;
                if let Err(e) = tx.send(state_mngr.get_state()).await {
                    error!("get_state_reciever get err: {:?}", e);
                    break;
                }
            }
        });
        rx
    }
}

// impl<T> clientbrokersvc::client_broker_service_server::ClientBrokerService for Broker<T>
// where
//     T: Storage + 'static,
// {
//     /// Server streaming response type for the Publish method.
//     type PublishStream = Pin<
//         Box<
//             dyn tonic::codegen::tokio_stream::Stream<
//                     Item = Result<clientbrokersvc::PublishResp, tonic::Status>,
//                 > + Send,
//         >,
//     >;

//     async fn publish(
//         &self,
//         request: tonic::Request<tonic::Streaming<clientbrokersvc::PublishReq>>,
//     ) -> std::result::Result<tonic::Response<Self::PublishStream>, tonic::Status> {

//         // T::store("topic", 11, Bytes::new()).await;

//         // let strm = async_stream::try_stream! {

//         // }

//         // Ok(Pin::new(Box::pin(x)))
//     }

//     /// Server streaming response type for the Publish method.
//     type SubscribeStream = Pin<
//         Box<
//             dyn tonic::codegen::tokio_stream::Stream<
//                     Item = Result<clientbrokersvc::SubscribeResp, tonic::Status>,
//                 > + Send,
//         >,
//     >;

//     async fn subscribe(
//         &self,
//         request: tonic::Request<clientbrokersvc::SubscribeReq>,
//     ) -> std::result::Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
//         todo!()
//     }
// }
