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
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::NonZeroUsize;
use std::{
    sync::{Arc, atomic},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct TransporterServiceConfig {
    // addr: 本地监听地址，默认 localhost:4343
    pub addr: SocketAddr,

    // protocol: 传输协议，默认 TCP
    pub protocol: TransportProtocol,

    // incoming_max_connections: 最大接收连接数，默认 100
    pub incoming_max_connections: usize,

    // max_frame_body_size: 每个消息最大 body 大小(发送和接收)，默认 10MB
    pub max_frame_body_size: usize,

    // send_timeout: 发送超时时间，默认 1 秒
    pub send_timeout: Duration,

    // idle_timeout: 空闲超时时间，默认 60 秒
    pub idle_timeout: Duration,

    // send_message_buffer_size: 发送缓冲区的消息数量，默认 10
    pub send_message_buffer_size: usize,
}

impl TransporterServiceConfig {
    fn fix(mut self) -> Self {
        if self.incoming_max_connections == 0 {
            self.incoming_max_connections = 100;
        }
        if self.max_frame_body_size == 0 {
            self.max_frame_body_size = 10 * 1024 * 1024;
        }
        if self.send_timeout.is_zero() {
            self.send_timeout = Duration::from_secs(1);
        }
        if self.idle_timeout.is_zero() {
            self.idle_timeout = Duration::from_secs(60);
        }
        if self.send_message_buffer_size == 0 {
            self.send_message_buffer_size = 10;
        }

        self
    }
}

impl Default for TransporterServiceConfig {
    fn default() -> Self {
        TransporterServiceConfig {
            addr: SocketAddr::V4("localhost:4343".parse().unwrap()),
            protocol: TransportProtocol::TCP,
            incoming_max_connections: 100,
            max_frame_body_size: 10 * 1024 * 1024,
            send_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(60),
            send_message_buffer_size: 10,
        }
    }
}

#[derive(Clone)]
pub struct TransporterServiceManager {
    conf: TransporterServiceConfig,
    service: Arc<Box<dyn ProtocolTransporterService>>,
}

impl TransporterServiceManager {
    pub fn new(mut conf: TransporterServiceConfig, tx: Sender<TransportMessage>) -> Self {
        conf = conf.fix();
        let service: Arc<Box<dyn ProtocolTransporterService>> = match conf.protocol {
            TransportProtocol::TCP => Arc::new(Box::new(TcpService::new(
                conf.addr.clone(),
                conf.clone(),
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

    fn get_remote_addr(&self, conn_id: u64) -> Option<SocketAddr> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.service.get_remote_addr(conn_id),
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }
}
