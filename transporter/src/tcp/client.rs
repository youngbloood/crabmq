use crate::TransporterWriter;
use crate::client::TransporterClientConfig;
use crate::conn::ProtocolTransporterService;
use crate::conn::{self, ProtocolTransporterClient};
use crate::tcp::{TcpReadHalf, TcpWriteHalf};
use crate::{
    TransportMessage,
    codec::TransportCodec,
    conn::{
        ProtocolTransporterCloser, ProtocolTransporterShutdown, ProtocolTransporterWriter,
        get_conn_id,
    },
    decode_to_message,
    err::{ErrorCode, TransporterError},
    handle_message,
};
use anyhow::Result;
use dashmap::DashMap;
use log::error;
use std::{net::SocketAddr, net::ToSocketAddrs, sync::Arc, time::Duration};
use tokio::time::timeout;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::mpsc::{Receiver, Sender},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
};
use tokio_stream::StreamExt;
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;

/**
 * TcpClient 定义了连接到远端地址的接口，建立连接并加入到管理器中
 * 可以链接多个远端地址，每个远端地址对应一个连接
 */

pub(crate) struct TcpClient {
    remote_addr: SocketAddr,
    conf: TransporterClientConfig,
    permit: OwnedSemaphorePermit,
    tx: Sender<TransportMessage>,
    shutdown: CancellationToken,
}

impl TcpClient {
    pub fn new(
        remote_addr: SocketAddr,
        conf: TransporterClientConfig,
        permit: OwnedSemaphorePermit,
        tx: Sender<TransportMessage>,
    ) -> Self {
        TcpClient {
            remote_addr,
            conf,
            permit,
            tx,
            shutdown: CancellationToken::new(),
        }
    }
}

impl Drop for TcpClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterClient for TcpClient {
    // 连接到远端地址，建立连接并加入到管理器中
    async fn connect(&self, remote_addr: SocketAddr, t: Duration) -> Result<TransporterWriter> {
        let stream = tokio::time::timeout(t, tokio::net::TcpStream::connect(&remote_addr))
            .await
            .map_err(|_| TransporterError::from_code(ErrorCode::ConnectTimeout))?
            .map_err(|e| TransporterError::new(ErrorCode::ConnectError, e.to_string()))?;

        stream.set_nodelay(true).unwrap_or_else(|e| {
            error!("[Transporter]: Failed to set nodelay: {}", e);
        });

        let (rh, wh) = stream.into_split();
        let tx = self.tx.clone();
        // 直接使用 self.shutdown，让客户端的生命周期真正驱动连接
        let shutdown = self.shutdown.clone();
        let conn_id = get_conn_id();

        // 启动 tcp reader 任务
        let reader = TcpReadHalf {
            tx,
            conn_id,
            remote_addr,
            shutdown: shutdown.clone(),
            max_frame_body_size: self.conf.max_frame_body_size,
            idle_timeout: self.conf.idle_timeout,
        };
        tokio::spawn(reader.loop_handle(rh));

        // 启动 tcp writer 任务
        let (wtx, wrx) = tokio::sync::mpsc::channel(self.conf.send_message_buffer_size);
        let wh = TcpWriteHalf::new(
            wh,
            self.conf.send_timeout,
            conn_id,
            remote_addr,
            shutdown.clone(),
        );
        tokio::spawn(wh.loop_handle(wrx));

        Ok(TransporterWriter {
            tx: wtx,
            conn_id,
            remote_addr,
            shotdown: shutdown,
        })
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterShutdown for TcpClient {
    async fn shutdown(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.shutdown.cancel();
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterCloser for TcpClient {
    // 关闭指定连接
    async fn close(&self) {
        self.shutdown.cancel();
    }
    // 检查连接是否已关闭
    async fn closed(&self) -> bool {
        self.shutdown.is_cancelled()
    }
}
