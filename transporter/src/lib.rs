mod codec;
mod conn;
mod err;
mod manager;
mod tcp;

pub use manager::*;

use crate::err::{ErrorCode, TransporterError};
use anyhow::Result;
use protocol::*;
use std::net::SocketAddr;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;
use tokio_util::bytes::BytesMut;
use tokio_util::{bytes::Bytes, sync::CancellationToken};

/// Wire-format header length: 1 (version) + 2 (index) + 4 (body_length).
pub(crate) const HEAD_LENGTH: usize = 7;

/// A decoded message received from or to be sent over a transport connection.
#[derive(Clone)]
pub struct TransportMessage {
    pub version: u8,
    pub index: u16,
    pub conn_id: u64,
    pub message: Arc<Box<dyn EnDecoder>>,
}

impl TransportMessage {
    /// Construct a version-1 message ready for dispatch.
    pub fn new_v1(index: u16, conn_id: u64, message: Box<dyn EnDecoder>) -> Self {
        TransportMessage {
            version: protocol::VERSION1,
            index,
            conn_id,
            message: Arc::new(message),
        }
    }

    /// Serialize the message into its on-wire binary representation.
    ///
    /// Wire format:
    /// ```text
    /// [version:1][index:2][body_length:4][body:body_length]
    /// ```
    ///
    /// Returns a zero‑copy Bytes containing the on‑wire representation.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let data = self.message.encode()?;

        let mut buf = BytesMut::with_capacity(HEAD_LENGTH + data.len());
        buf.extend_from_slice(&[self.version]);
        buf.extend_from_slice(&self.index.to_be_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&data);

        return Ok(buf.freeze());
    }
}

/// Supported transport-layer protocols.
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
            _ => panic!("Unknown transport protocol: {}", s),
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

/// A cloneable write handle to a single logical connection.
///
/// Cheaply clone this handle to send from multiple tasks.
/// Call [`TransporterWriter::close`] explicitly to tear down the connection.
#[derive(Clone)]
pub struct TransporterWriter {
    pub(crate) tx: Sender<Bytes>,

    pub(crate) conn_id: u64,
    pub(crate) remote_addr: SocketAddr,

    pub(crate) shutdown: CancellationToken,
}

impl TransporterWriter {
    /// Serialize `cmd` and enqueue it for writing.
    pub async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
        self.send_bytes(cmd.to_bytes()?, t).await
    }

    /// Enqueue raw bytes for writing without serialization overhead.
    pub async fn send_raw(&self, data: Vec<u8>, t: Option<Duration>) -> Result<()> {
        self.send_bytes(Bytes::from(data), t).await
    }

    /// Enqueue a pre-built [`Bytes`] value. Zero-copy for `Arc`-backed allocations.
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

    /// Returns `true` if the underlying connection has been closed.
    pub fn closed(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Signal the associated [`TcpWriteHalf`] task to shut down gracefully.
    pub fn close(&self) {
        self.shutdown.cancel();
    }

    /// The unique numeric ID assigned to this connection.
    pub fn conn_id(&self) -> u64 {
        self.conn_id
    }

    /// The remote peer address for this connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
}

pub(crate) fn decode_to_message(
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
