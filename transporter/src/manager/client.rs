use crate::TransporterWriter;
use crate::conn::ProtocolTransporterService;
use crate::conn::{ProtocolTransporterClient, get_conn_id};
use crate::tcp::TcpClient;
use crate::tcp::TcpService;
use crate::{
    TransportMessage, TransportProtocol,
    err::{ErrorCode, TransporterError},
};
use anyhow::Result;
use dashmap::DashMap;
use log::warn;
use std::any::Any;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::{
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct TransporterClientConfig {
    // connection_timeout: 连接超时时间，默认 5 秒
    pub connection_timeout: Duration,

    // idle_timeout: 空闲超时时间，默认 60 秒
    pub idle_timeout: Duration,

    // outgoing_max_connections: 最大外部连接数，默认 100
    pub outgoing_max_connections: usize,

    // max_frame_body_size: 每个消息最大 body 大小(发送和接收)，默认 10MB
    pub max_frame_body_size: usize,

    // send_timeout: 发送超时时间，默认 1 秒
    pub send_timeout: Duration,

    // send_message_buffer_size: 发送缓冲区的消息数量，默认 10
    pub send_message_buffer_size: usize,
}

impl Default for TransporterClientConfig {
    fn default() -> Self {
        TransporterClientConfig {
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            outgoing_max_connections: 100,
            max_frame_body_size: 10 * 1024 * 1024, // 默认每个消息最大 body 大小为10MB
            send_timeout: Duration::from_secs(1),
            send_message_buffer_size: 10,
        }
    }
}

impl TransporterClientConfig {
    fn fix(mut self) -> Self {
        if self.connection_timeout.is_zero() {
            self.connection_timeout = Duration::from_secs(5);
        }
        if self.idle_timeout.is_zero() {
            self.idle_timeout = Duration::from_secs(60);
        }
        if self.outgoing_max_connections == 0 {
            self.outgoing_max_connections = 100;
        }
        if self.send_timeout.is_zero() {
            self.send_timeout = Duration::from_secs(1);
        }
        if self.max_frame_body_size == 0 {
            self.max_frame_body_size = 10 * 1024 * 1024;
        }
        if self.send_message_buffer_size == 0 {
            self.send_message_buffer_size = 10;
        }

        self
    }
}

#[derive(Clone)]
pub struct TransporterClientManager {
    conf: TransporterClientConfig,
    sema: Arc<tokio::sync::Semaphore>,
    tx: Sender<TransportMessage>,
    clients: Arc<DashMap<u64, TransporterWriter>>,
}

impl TransporterClientManager {
    pub fn new(mut conf: TransporterClientConfig, tx: Sender<TransportMessage>) -> Self {
        conf = conf.fix();
        TransporterClientManager {
            sema: Arc::new(tokio::sync::Semaphore::new(conf.outgoing_max_connections)),
            clients: Arc::new(DashMap::new()),
            conf,
            tx,
        }
    }

    pub async fn connect<A: ToSocketAddrs>(
        &self,
        remote_addr: A,
        protocol: TransportProtocol,
    ) -> Result<()> {
        let remote_addr = remote_addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow::anyhow!("[Transporter]: Invalid remote address"))?;
        let sema = self.sema.clone();
        let permit: tokio::sync::OwnedSemaphorePermit =
            sema.acquire_owned().await.map_err(|_| -> anyhow::Error {
                warn!(
                    "[Transporter]: Maximum outgoing connections reached, cannot connect to {}",
                    remote_addr
                );

                TransporterError::new(
                    ErrorCode::MaxOutgoingReached,
                    "[Transporter]: Maximum outgoing connections reached".to_string(),
                )
                .into()
            })?;

        let timeout = self.conf.connection_timeout;
        match protocol {
            TransportProtocol::TCP => {
                let t = TcpClient::new(remote_addr, self.conf.clone(), permit, self.tx.clone());
                let tw = t.connect(remote_addr, timeout).await?;
                self.clients.insert(tw.conn_id, tw);

                Ok(())
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn send(&self, msg: TransportMessage) -> Result<()> {
        let conn_id = msg.conn_id;
        match self.clients.get(&conn_id) {
            Some(entry) => {
                entry.send(&msg, Some(self.conf.send_timeout)).await?;
                Ok(())
            }

            None => Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into()),
        }
    }

    pub fn close(&self, conn_id: u64) {
        if let Some(client) = self.clients.remove(&conn_id) {
            let _ = client.1.close();
        }
    }

    pub fn get_remote_addr(&self, conn_id: u64) -> Option<SocketAddr> {
        self.clients.get(&conn_id).map(|entry| entry.remote_addr)
    }
}
