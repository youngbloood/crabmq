use crate::{
    TransportMessage, TransportProtocol,
    err::{ErrorCode, TransporterError},
};

#[cfg(feature = "service")]
use crate::TransporterWriter;
#[cfg(feature = "service")]
use crate::conn::ProtocolTransporterService;
#[cfg(feature = "service")]
use crate::tcp::TcpService;

#[cfg(feature = "client")]
use crate::conn::{ProtocolTransporterClient, get_conn_id};
#[cfg(feature = "client")]
use crate::tcp::TcpClient;

use anyhow::Result;
use dashmap::DashMap;
#[cfg(feature = "client")]
use std::net::ToSocketAddrs;
#[cfg(feature = "service")]
use std::num::NonZeroUsize;
use std::{
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::sync::Mutex;
#[cfg(feature = "client")]
use tokio::sync::mpsc::Sender;

#[cfg(feature = "service")]
#[derive(Clone)]
pub struct TransporterServiceConfig {
    pub addr: String,
    pub protocol: TransportProtocol,
    pub incoming_max_connections: NonZeroUsize,
    pub max_frame_body_size: NonZeroUsize,
}

#[cfg(feature = "service")]
impl TransporterServiceConfig {
    fn fix(&mut self) {
        if self.addr.is_empty() {
            self.addr = "localhost:4343".to_string();
        }
    }
}

#[cfg(feature = "service")]
impl Default for TransporterServiceConfig {
    fn default() -> Self {
        TransporterServiceConfig {
            addr: "localhost:4343".to_string(),
            protocol: TransportProtocol::TCP,
            incoming_max_connections: NonZeroUsize::new(100).unwrap(),
            max_frame_body_size: NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 默认每个消息最大 body 大小为10MB
        }
    }
}

#[cfg(feature = "service")]
#[derive(Clone)]
pub struct TransporterServiceManager {
    conf: TransporterServiceConfig,
    service: Arc<Box<dyn ProtocolTransporterService>>,
}

#[cfg(feature = "service")]
impl TransporterServiceManager {
    pub fn new(conf: TransporterServiceConfig, tx: Sender<TransportMessage>) -> Self {
        let service: Arc<Box<dyn ProtocolTransporterService>> = match conf.protocol {
            TransportProtocol::TCP => Arc::new(Box::new(TcpService::new(
                conf.addr.clone(),
                conf.incoming_max_connections.get(),
                conf.max_frame_body_size.get(),
                tx,
            ))),
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        };

        TransporterServiceManager { conf, service }
    }

    pub async fn run(&self) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.run().await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn send(&self, msg: &TransportMessage, t: Option<Duration>) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.send(&msg, t).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn broadcast(&self, msg: &TransportMessage) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.broadcast(msg).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn split_writer(&self, conn_id: u64) -> Option<TransporterWriter> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.split_writer(conn_id).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn close(&self, conn_id: u64) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.close(conn_id).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }
}

#[cfg(feature = "client")]
#[derive(Clone)]
pub struct TransporterClientConfig {
    pub protocol: TransportProtocol,
    pub outgoing_max_connections: NonZeroUsize,
    pub max_frame_body_size: NonZeroUsize,
}

#[cfg(feature = "client")]
impl Default for TransporterClientConfig {
    fn default() -> Self {
        TransporterClientConfig {
            protocol: TransportProtocol::TCP,
            outgoing_max_connections: NonZeroUsize::new(100).unwrap(),
            max_frame_body_size: NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 默认每个消息最大 body 大小为10MB
        }
    }
}

#[cfg(feature = "client")]
struct TransporterClientUnit {
    remote_addr: String,
    round: Arc<atomic::AtomicUsize>,
    client: Box<dyn ProtocolTransporterClient>,
}

#[cfg(feature = "client")]
#[derive(Clone)]
pub struct TransporterClientManager {
    conf: TransporterClientConfig,
    sema: Arc<tokio::sync::Semaphore>,
    tx: Sender<TransportMessage>,
    clients: Arc<DashMap<u64, TransporterClientUnit>>,
}

#[cfg(feature = "client")]
impl TransporterClientManager {
    pub fn new(conf: TransporterClientConfig, tx: Sender<TransportMessage>) -> Self {
        TransporterClientManager {
            sema: Arc::new(tokio::sync::Semaphore::new(
                conf.outgoing_max_connections.get(),
            )),
            clients: Arc::new(DashMap::new()),
            conf,
            tx,
        }
    }

    pub async fn connect<A: ToSocketAddrs>(
        &self,
        remote_addr: A,
        protocol: TransportProtocol,
        timeout: Duration,
    ) -> Result<()> {
        let remote_addr = remote_addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid remote address"))?;
        let sema = self.sema.clone();
        let permit = sema.acquire_owned().await.map_err(|_| -> anyhow::Error {
            TransporterError::new(
                ErrorCode::MaxOutgoingReached,
                "Maximum outgoing connections reached".to_string(),
            )
            .into()
        })?;

        match protocol {
            TransportProtocol::TCP => {
                let t = TcpClient::new(
                    remote_addr,
                    self.tx.clone(),
                    permit,
                    self.conf.max_frame_body_size.get(),
                );
                let conn_id = t.connect(remote_addr, timeout).await?;
                self.clients.insert(
                    conn_id,
                    TransporterClientUnit {
                        remote_addr: remote_addr.to_string(),
                        round: Arc::new(atomic::AtomicUsize::new(0)),
                        client: Box::new(t),
                    },
                );

                Ok(())
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub fn close(&self, conn_id: u64) {
        if let Some(client) = self.clients.remove(&conn_id) {
            let _ = client.1.client.close();
        }
    }
}
