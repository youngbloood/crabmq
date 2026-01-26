mod err;
mod tcp;

use std::{sync::Arc, time::Duration};

use crate::{
    err::{ErrorCode, TransporterError},
    tcp::Tcp,
};
use anyhow::Result;
use protocolv2::*;
use tokio::sync::{Mutex, mpsc::UnboundedSender};

pub struct TransportMessage {
    pub index: u8,
    pub remote_addr: String,
    pub message: Box<dyn EnDecoder>,
}

impl TransportMessage {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![self.index];
        let mut message_bytes = self.message.encode()?;
        bytes.extend_from_slice(message_bytes.len().to_be_bytes().as_slice());
        bytes.extend_from_slice(&mut message_bytes);
        Ok(bytes)
    }
}

#[derive(Clone)]
pub struct Config {
    pub addr: String,
    pub protocol: TransportProtocol,
    pub incoming_max_connections: usize,
    pub outgoing_max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            addr: "localhost:4343".to_string(),
            protocol: TransportProtocol::TCP,
            incoming_max_connections: 100,
            outgoing_max_connections: 100,
        }
    }
}

#[derive(Clone, Copy)]
pub enum TransportProtocol {
    TCP,
    UDP,
    QUIC,
    KCP,
}

#[derive(Clone)]
pub struct Transporter {
    conf: Config,
    pt: Option<Arc<Mutex<dyn ProtocolTransporterManager>>>,
}

impl Transporter {
    pub fn new(conf: Config) -> Self {
        let pt: Option<Arc<Mutex<dyn ProtocolTransporterManager>>> = match conf.protocol {
            TransportProtocol::TCP => Some(Arc::new(Mutex::new(Tcp::new(
                conf.addr.clone(),
                conf.incoming_max_connections,
                conf.outgoing_max_connections,
            )))),
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        };
        Transporter { conf, pt }
    }

    pub async fn start(&self) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.pt.as_ref().unwrap().lock().await.start().await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn send(&self, cmd: &TransportMessage) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.pt.as_ref().unwrap().lock().await.send(cmd).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    /// Broadcast a command to all connected remote addresses using the protocol specified in the configuration.
    pub async fn broadcast(&self, cmd: TransportMessage) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => {
                self.pt
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .broadcast_all(&cmd)
                    .await
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn connect(&self, remote_addr: &str) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => {
                self.pt
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .connect(remote_addr)
                    .await
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn recv(&mut self, timeout: u64) -> Option<TransportMessage> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.pt.as_mut().unwrap().lock().await.recv(timeout).await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn split_writer(&self, remote_addr: &str) -> Option<TransporterWriter> {
        match self.conf.protocol {
            TransportProtocol::TCP => {
                self.pt
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .split_writer(remote_addr)
                    .await
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn close(&self, remote_addr: &str) {
        match self.conf.protocol {
            TransportProtocol::TCP => {
                self.pt
                    .as_ref()
                    .unwrap()
                    .lock()
                    .await
                    .close(remote_addr)
                    .await
            }
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }
}

#[async_trait::async_trait]
pub trait ProtocolTransporterManager {
    // 启动本地监听服务
    async fn start(&self) -> Result<()>;
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn recv(&mut self, timeout: u64) -> Option<TransportMessage>;
    // 将本地监听服务分离出一个新的 TransporterWriter，remote_addr 是要分离的连接地址
    async fn split_writer(&self, remote_addr: &str) -> Option<TransporterWriter>;
    // 广播
    async fn broadcast_all(&self, cmd: &TransportMessage) -> Result<()>;
    // 广播到所有连接至本地的 endpoints
    async fn broadcast_incoming(&self, cmd: &TransportMessage) -> Result<()>;
    // 广播到所有连接至远端的 endpoints
    async fn broadcast_outgoing(&self, cmd: &TransportMessage) -> Result<()>;
    // 连接到远端地址，建立连接并加入到管理器中
    async fn connect(&self, remote_addr: &str) -> Result<()>;
    // 发送消息到指定的远端地址
    async fn send(&self, cmd: &TransportMessage) -> Result<()>;
    // 关闭指定连接
    async fn close(&self, remote_addr: &str);
    // 停止服务
    async fn shutdown(&self);
}

#[async_trait::async_trait]
pub trait ProtocolTransporterWriter: Send + Sync + 'static {
    async fn send(&self, cmd: &TransportMessage) -> Result<()>;
    async fn send_timeout(&self, cmd: &TransportMessage, t: Duration) -> Result<()>;
    async fn closed(&self) -> bool;
    async fn close(&self);
}

pub enum TransporterWriter {
    Tcp(tcp::TcpWriter),
}

impl TransporterWriter {
    fn from_tcp(w: tcp::TcpWriter) -> Self {
        TransporterWriter::Tcp(w)
    }

    pub async fn send(&self, cmd: &TransportMessage) -> Result<()> {
        match self {
            Self::Tcp(w) => w.send(cmd).await,
        }
    }

    pub async fn send_timeout(&mut self, cmd: &TransportMessage, t: Duration) -> Result<()> {
        match self {
            Self::Tcp(w) => w.send_timeout(cmd, t).await,
        }
    }

    pub async fn closed(&self) -> bool {
        match self {
            Self::Tcp(w) => w.closed().await,
        }
    }

    pub async fn close(&self) {
        match self {
            Self::Tcp(w) => w.close().await,
        }
    }
}

fn handle_message(
    tx: UnboundedSender<TransportMessage>,
    index: u8,
    body: &[u8],
    remote_addr: String,
) -> Result<()> {
    let message = decode_to_message(index, body, remote_addr).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::DecodeError, e.to_string()).into()
    })?;

    tx.send(message).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::SendError, e.to_string()).into()
    })?;

    Ok(())
}

fn decode_to_message(index: u8, body: &[u8], remote_addr: String) -> Result<TransportMessage> {
    let message = protocolv2::decode_message(index, body).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::UnknownMessageTypeError, e.to_string()).into()
    })?;
    Ok(TransportMessage {
        index,
        remote_addr,
        message,
    })
}
