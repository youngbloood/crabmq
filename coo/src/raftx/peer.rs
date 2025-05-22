use std::sync::Arc;

use log::debug;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct PeerState {
    pub id: u64,
    pub raft_addr: String,
    pub coo_addr: String,
    status: Arc<Mutex<PeerStatus>>,
}

#[derive(PartialEq, Debug)]
enum PeerStatus {
    Normal,
    HalfClose,
    Closed,
}

impl PeerState {
    pub fn new(id: u64, raft_addr: String, coo_addr: String) -> Self {
        Self {
            id,
            raft_addr,
            coo_addr,
            status: Arc::new(Mutex::new(PeerStatus::Normal)),
        }
    }

    pub fn get_raft_addr(&self) -> &str {
        &self.raft_addr
    }

    pub fn get_coo_addr(&self) -> &str {
        &self.coo_addr
    }

    // 降级
    pub async fn rotate_downgrade(&self) {
        if self.is_normal().await {
            let mut status = self.status.lock().await;
            *status = PeerStatus::HalfClose;
            debug!("Node[{}] downgrade to PeerStatus::HalfClose", self.id);
            return;
        }
        if self.is_halfclose().await {
            let mut status = self.status.lock().await;
            *status = PeerStatus::Closed;
            debug!("Node[{}] downgrade to PeerStatus::Closed", self.id);
        }
    }

    // 升级
    pub async fn rotate_upgrade(&self) {
        if self.is_closed().await {
            let mut status = self.status.lock().await;
            *status = PeerStatus::HalfClose;
            debug!("Node[{}] upgrade to PeerStatus::HalfClose", self.id);
            return;
        }
        if self.is_halfclose().await {
            let mut status = self.status.lock().await;
            *status = PeerStatus::Normal;
            debug!("Node[{}] upgrade to PeerStatus::Normal", self.id);
        }
    }

    pub async fn is_normal(&self) -> bool {
        let status = self.status.lock().await;
        *status == PeerStatus::Normal
    }

    pub async fn is_halfclose(&self) -> bool {
        let status = self.status.lock().await;
        *status == PeerStatus::HalfClose
    }

    pub async fn is_closed(&self) -> bool {
        let status = self.status.lock().await;
        *status == PeerStatus::Closed
    }
}
