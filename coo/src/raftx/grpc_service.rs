use super::{mailbox_message_type::MessageType as AllMessageType, storage::SledStorage};
use grpcx::cooraftsvc;
use log::info;
use protobuf::Message;
use raft::{
    RawNode,
    prelude::{ConfChange, ConfChangeV2},
};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tonic::{Request, Response, Status};

// pub mod cooraftsvc {
//     tonic::include_proto!("cooraftsvc");
// }

#[derive(Clone)]
pub struct RaftServiceImpl {
    id: u64,
    raw_node: Arc<Mutex<RawNode<SledStorage>>>,
    tx_grpc: mpsc::Sender<AllMessageType>,
}

impl RaftServiceImpl {
    pub fn new(
        id: u64,
        raw_node: Arc<Mutex<RawNode<SledStorage>>>,
        tx_grpc: mpsc::Sender<AllMessageType>,
    ) -> Self {
        Self {
            id,
            raw_node,
            tx_grpc,
        }
    }
}

#[tonic::async_trait]
impl cooraftsvc::raft_service_server::RaftService for RaftServiceImpl {
    async fn send_raft_message(
        &self,
        request: Request<cooraftsvc::RaftMessage>,
    ) -> Result<Response<cooraftsvc::RaftResponse>, Status> {
        match Message::parse_from_bytes(&request.into_inner().message) {
            Ok(msg) => {
                let _ = self.tx_grpc.send(AllMessageType::RaftMessage(msg)).await;
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

    async fn get_id(
        &self,
        _request: tonic::Request<cooraftsvc::Empty>,
    ) -> Result<Response<cooraftsvc::IdResp>, Status> {
        Ok(Response::new(cooraftsvc::IdResp { id: self.id as u32 }))
    }

    async fn propose_data(
        &self,
        _request: tonic::Request<cooraftsvc::ProposeDataReq>,
    ) -> std::result::Result<tonic::Response<cooraftsvc::ProposeDataResp>, tonic::Status> {
        let _ = self
            .tx_grpc
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
                    let _ = self.tx_grpc.send(AllMessageType::RaftConfChange(cc)).await;
                    ""
                }
                Err(e) => &e.to_string(),
            },

            2 => match ConfChangeV2::parse_from_bytes(&req.message) {
                Ok(cc) => {
                    let _ = self
                        .tx_grpc
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

pub async fn start_grpc_server(addr: String, service: RaftServiceImpl) {
    use tonic::transport::Server;
    let addr = addr.parse().unwrap();
    let svc = cooraftsvc::raft_service_server::RaftServiceServer::new(service);
    info!("Coordinator-raft listen: {}", addr);
    match Server::builder().add_service(svc).serve(addr).await {
        Ok(_) => {
            info!("Coordinator-raft server started at {}", addr);
        }
        Err(e) => panic!("Coordinator-raft listen : {}, err: {:?}", addr, e),
    }
}
