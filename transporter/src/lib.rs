mod conn;
mod err;
mod manager;
mod tcp;

pub use manager::*;

use crate::{
    conn::{ProtocolTransporterCloser as _, ProtocolTransporterWriter},
    err::{ErrorCode, TransporterError},
};
use anyhow::Result;
use protocol::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub struct TransportMessage {
    pub version: u8,
    pub index: u16,
    pub remote_addr: String,
    pub message: Arc<Box<dyn EnDecoder>>,
}

impl TransportMessage {
    pub fn new_v1(index: u16, remote_addr: String, message: Box<dyn EnDecoder>) -> Self {
        TransportMessage {
            version: protocol::VERSION1,
            index,
            remote_addr,
            message: Arc::new(message),
        }
    }

    /**
     * Head
     * [version] 1 byte
     * [index] 2 bytes
     * [body_length] 4 bytes
     * [body] body_length bytes
     */
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![self.version];
        bytes.extend_from_slice(self.index.to_be_bytes().as_slice());
        let mut message_bytes = self.message.encode()?;
        bytes.extend_from_slice((message_bytes.len() as u32).to_be_bytes().as_slice());
        bytes.extend_from_slice(&mut message_bytes);
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TransportProtocol {
    TCP,
    UDP,
    QUIC,
    KCP,
}

impl From<&str> for TransportProtocol {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "tcp" => TransportProtocol::TCP,
            "udp" => TransportProtocol::UDP,
            "quic" => TransportProtocol::QUIC,
            "kcp" => TransportProtocol::KCP,
            _ => {
                panic!("Unknown transport protocol: {}", s);
            }
        }
    }
}

impl TransportProtocol {
    pub fn as_str(&self) -> &str {
        match self {
            TransportProtocol::TCP => "tcp",
            TransportProtocol::UDP => "udp",
            TransportProtocol::QUIC => "quic",
            TransportProtocol::KCP => "kcp",
        }
    }
}

#[cfg(feature = "service")]
pub enum TransporterWriter {
    Tcp(tcp::TcpWriter),
}

#[cfg(feature = "service")]
impl TransporterWriter {
    fn from_tcp(w: tcp::TcpWriter) -> Self {
        TransporterWriter::Tcp(w)
    }

    pub async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
        match self {
            Self::Tcp(w) => w.send(cmd, t).await,
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
    version: u8,
    index: u16,
    body: &[u8],
    remote_addr: String,
) -> Result<()> {
    let message =
        decode_to_message(version, index, body, remote_addr).map_err(|e| -> anyhow::Error {
            TransporterError::new(ErrorCode::DecodeError, e.to_string()).into()
        })?;

    tx.send(message).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::SendError, e.to_string()).into()
    })?;

    Ok(())
}

fn decode_to_message(
    version: u8,
    index: u16,
    body: &[u8],
    remote_addr: String,
) -> Result<TransportMessage> {
    let message = protocol::decode_message(version, index, body).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::UnknownMessageTypeError, e.to_string()).into()
    })?;
    Ok(TransportMessage {
        version,
        index,
        remote_addr,
        message: Arc::new(message),
    })
}
