mod tcp;

use std::sync::Arc;

use crate::tcp::Tcp;
use anyhow::Result;
use protocolv2::*;
use tokio::sync::Mutex;

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
            TransportProtocol::TCP => Some(Arc::new(Mutex::new(Tcp::new(conf.addr.clone())))),
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        };
        Transporter { conf, pt }
    }

    pub async fn start(&mut self) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.pt.as_mut().unwrap().lock().await.start().await,
            TransportProtocol::UDP => todo!(),
            TransportProtocol::QUIC => todo!(),
            TransportProtocol::KCP => todo!(),
        }
    }

    pub async fn send(&mut self, cmd: &TransportMessage) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => self.pt.as_mut().unwrap().lock().await.send(cmd).await,
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

    pub async fn connect(&mut self, remote_addr: String) -> Result<()> {
        match self.conf.protocol {
            TransportProtocol::TCP => {
                self.pt
                    .as_mut()
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
    async fn connect(&self, remote_addr: String) -> Result<()>;
    // 发送消息到指定的远端地址
    async fn send(&self, cmd: &TransportMessage) -> Result<()>;
    // 关闭指定连接
    async fn close(&self, remote_addr: &str);
}

#[async_trait::async_trait]
pub trait ProtocolTransporterWriter {
    async fn send(&mut self, cmd: &TransportMessage) -> Result<()>;
    async fn closed(&self) -> bool;
    async fn close(&self);
}

pub struct TransporterWriter {
    p: TransportProtocol,
    w: Box<dyn ProtocolTransporterWriter>,
}

impl TransporterWriter {
    async fn send(&mut self, cmd: &TransportMessage) -> Result<()> {
        self.w.send(cmd).await?;
        Ok(())
    }

    async fn closed(&self) -> bool {
        self.w.closed().await
    }
}
