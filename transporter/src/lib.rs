mod codec;
mod conn;
mod err;
mod manager;
mod tcp;

pub use manager::*;
#[cfg(feature = "service")]
use tokio::time::timeout;
#[cfg(feature = "service")]
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    conn::{ProtocolTransporterCloser as _, ProtocolTransporterWriter},
    err::{ErrorCode, TransporterError},
};
use anyhow::Result;
use protocol::*;
#[cfg(feature = "service")]
use std::net::SocketAddr;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct TransportMessage {
    pub version: u8,
    pub index: u16,
    pub conn_id: u64,
    pub message: Arc<Box<dyn EnDecoder>>,
}

impl TransportMessage {
    pub fn new_v1(index: u16, conn_id: u64, message: Box<dyn EnDecoder>) -> Self {
        TransportMessage {
            version: protocol::VERSION1,
            index,
            conn_id,
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
#[derive(Clone)]
pub struct TransporterWriter {
    pub(crate) tx: Sender<Bytes>,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) shotdown: CancellationToken,
}

#[cfg(feature = "service")]
impl TransporterWriter {
    pub async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
        let data = cmd.to_bytes()?;
        let data = Bytes::from(data);
        self.send_bytes(data, t).await
    }

    pub async fn send_raw(&self, data: Vec<u8>, t: Option<Duration>) -> Result<()> {
        let data = Bytes::from(data);
        self.send_bytes(data, t).await
    }

    pub async fn send_bytes(&self, data: Bytes, t: Option<Duration>) -> Result<()> {
        match t {
            Some(duration) => {
                timeout(duration, self.tx.send(data)).await??;
            }
            None => {
                self.tx.send(data).await?;
            }
        }
        Ok(())
    }

    pub async fn closed(&self) -> bool {
        self.shotdown.is_cancelled()
    }

    pub async fn close(&self) {
        self.shotdown.cancel();
    }
}

async fn handle_message(
    tx: Sender<TransportMessage>,
    version: u8,
    index: u16,
    body: &[u8],
    conn_id: u64,
) -> Result<()> {
    let message =
        decode_to_message(version, index, conn_id, body).map_err(|e| -> anyhow::Error {
            TransporterError::new(ErrorCode::DecodeError, e.to_string()).into()
        })?;

    tx.send(message).await.map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::SendError, e.to_string()).into()
    })?;

    Ok(())
}

fn decode_to_message(
    version: u8,
    index: u16,
    conn_id: u64,
    body: &[u8],
) -> Result<TransportMessage> {
    let message = protocol::decode_message(version, index, body).map_err(|e| -> anyhow::Error {
        TransporterError::new(ErrorCode::UnknownMessageTypeError, e.to_string()).into()
    })?;
    Ok(TransportMessage {
        version,
        index,
        conn_id,
        message: Arc::new(message),
    })
}
