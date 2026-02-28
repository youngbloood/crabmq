use crate::{
    TransportMessage, TransportProtocol, TransporterWriter,
    conn::{ProtocolTransporterClient, ProtocolTransporterService},
    err::{ErrorCode, TransporterError},
    tcp::{TcpClient, TcpService},
};
use anyhow::Result;
use dashmap::DashMap;
use std::{
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::sync::Mutex;

#[cfg(feature = "service")]
#[derive(Clone)]
pub struct TransporterServiceConfig {
    pub addr: String,
    pub protocol: TransportProtocol,
    pub incoming_max_connections: usize,
}

#[cfg(feature = "service")]
impl TransporterServiceConfig {
    fn fix(&mut self) {
        if self.addr.is_empty() {
            self.addr = "localhost:4343".to_string();
        }
        if self.incoming_max_connections == 0 {
            self.incoming_max_connections = 100;
        }
    }
}

#[cfg(feature = "service")]
impl Default for TransporterServiceConfig {
    fn default() -> Self {
        TransporterServiceConfig {
            addr: "localhost:4343".to_string(),
            protocol: TransportProtocol::TCP,
            incoming_max_connections: 100,
        }
    }
}

#[cfg(feature = "service")]
#[derive(Clone)]
pub struct TransporterServiceManager {
    conf: TransporterServiceConfig,
    service: Arc<Mutex<Box<dyn ProtocolTransporterService>>>,
}

#[cfg(feature = "service")]
impl TransporterServiceManager {
    pub fn new(conf: TransporterServiceConfig) -> Self {
        let service: Arc<Mutex<Box<dyn ProtocolTransporterService>>> = match conf.protocol {
            TransportProtocol::TCP => Arc::new(Mutex::new(Box::new(TcpService::new(
                conf.addr.clone(),
                conf.incoming_max_connections,
            ))
                as Box<dyn ProtocolTransporterService>)),
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        };

        TransporterServiceManager { conf, service }
    }

    pub async fn run(&self) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.run().await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn send(&self, msg: &TransportMessage, t: Option<Duration>) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.send(&msg, t).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn broadcast(&self, msg: &TransportMessage) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.broadcast(msg).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn recv(&self, t: Option<Duration>) -> Option<TransportMessage> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.recv(t).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn split_writer(&self, remote: &str) -> Option<TransporterWriter> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.split_writer(remote).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn close(&self, remote_addr: &str) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.lock().await.close(remote_addr).await,
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
    pub outgoing_max_connections: usize,
}

#[cfg(feature = "client")]
impl Default for TransporterClientConfig {
    fn default() -> Self {
        TransporterClientConfig {
            protocol: TransportProtocol::TCP,
            outgoing_max_connections: 100,
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
    round: Arc<atomic::AtomicUsize>,
    clients: Arc<DashMap<String, TransporterClientUnit>>,
}

#[cfg(feature = "client")]
impl TransporterClientManager {
    pub fn new(conf: TransporterClientConfig) -> Self {
        TransporterClientManager {
            sema: Arc::new(tokio::sync::Semaphore::new(conf.outgoing_max_connections)),
            clients: Arc::new(DashMap::new()),
            conf,
            round: Arc::new(atomic::AtomicUsize::new(0)),
        }
    }

    pub async fn connect(&self, remote_addr: &str, protocol: TransportProtocol) -> Result<()> {
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
                let t = TcpClient::new(remote_addr.to_string(), permit);
                t.connect(remote_addr).await?;
                self.clients.insert(
                    remote_addr.to_string(),
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

    pub fn close(&self, remote_addr: &str) {
        if let Some(client) = self.clients.remove(remote_addr) {
            let _ = client.1.client.close();
        }
    }
}
