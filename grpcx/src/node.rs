use crate::{
    brokercoosvc::{self, broker_coo_service_client::BrokerCooServiceClient},
    commonsvc,
};
use dashmap::DashMap;
use log::error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Response, transport::Channel};

#[derive(PartialEq)]
pub enum PeerStatus {
    Normal,
    HalfClose,
    Closed,
}
pub struct CooNode {
    id: u32,
    addr: String,
    status: Arc<Mutex<PeerStatus>>,
    client: Option<BrokerCooServiceClient<Channel>>,
}

pub struct BrokerNode {
    id: u32,
    addr: String,
}

pub struct SlaveNode {
    id: u32,
    addr: String,
}

/// 根据初始链接，自动切换链接至 coo-leader
struct BrokerCooLeaderAutoSwitch {
    leader_addr: Arc<Mutex<String>>,
    coo_addrs: Arc<DashMap<u64, String>>,
    client: Arc<Mutex<BrokerCooServiceClient<Channel>>>,
}

impl BrokerCooLeaderAutoSwitch {
    pub fn new(coo_addr: String, client: BrokerCooServiceClient<Channel>) -> Self {
        Self {
            leader_addr: Arc::new(Mutex::new(coo_addr)),
            coo_addrs: Arc::new(DashMap::new()),
            client: Arc::new(Mutex::new(client)),
        }
    }

    pub async fn auth(
        &self,
        req: brokercoosvc::AuthReq,
    ) -> Result<Response<brokercoosvc::AuthResp>, tonic::Status> {
        match self.client.lock().await.auth(req.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                self.handle(e).await;
                self.client.lock().await.auth(req).await
            }
        }
    }

    pub async fn list(
        &self,
        req: commonsvc::CooListReq,
    ) -> Result<Response<commonsvc::CooListResp>, tonic::Status> {
        match self.client.lock().await.list(req.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                self.handle(e).await;
                self.client.lock().await.list(req).await
            }
        }
    }

    pub async fn report(
        &self,
        req: impl tonic::IntoStreamingRequest<Message = brokercoosvc::BrokerState>,
    ) -> Result<Response<brokercoosvc::AuthResp>, tonic::Status> {
        match self.client.lock().await.report(req.clone()).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                self.handle(e).await;
                self.client.lock().await.report(req).await
            }
        }
    }

    async fn handle(&self, status: tonic::Status) -> bool {
        if status.code() == tonic::Code::PermissionDenied {
            // 更新 coo_addr
            let mut leader_addr = self.leader_addr.lock().await;
            *leader_addr = status.message().to_string();

            let client = BrokerCooServiceClient::connect(format!("http://{}", *leader_addr)).await;
            if let Ok(c) = client {
                let mut grpc_client = self.client.lock().await;
                *grpc_client = c;
            }
        }
        error!("skip handle unexpected tonic status: {:?}", status);
        true
    }
}

struct ClientCooLeader {
    coo_addr: String,
}
