use crate::TransporterWriter;
use crate::conn::ProtocolTransporterService;
use crate::conn::{self, ProtocolTransporterClient};
use crate::service::TransporterServiceConfig;
use crate::tcp::{TcpReadHalf, TcpWriteHalf};
use crate::{
    TransportMessage,
    codec::TransportCodec,
    conn::{
        ProtocolGetRemoteAddr, ProtocolTransporterCloser, ProtocolTransporterShutdown,
        ProtocolTransporterWriter, get_conn_id,
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

pub struct TcpService {
    addr: SocketAddr,
    conf: TransporterServiceConfig,
    sema: Arc<Semaphore>, // 用于限制最大连接数的信号量

    tx: Sender<TransportMessage>,
    // conn_id -> WriteHalf
    conns: Arc<DashMap<u64, TransporterWriter>>,

    shutdown: CancellationToken,
}

impl TcpService {
    pub fn new(
        addr: SocketAddr,
        conf: TransporterServiceConfig,
        tx: Sender<TransportMessage>,
    ) -> Self {
        let incoming_max_connections = conf.incoming_max_connections;
        TcpService {
            addr,
            conf,
            tx,
            sema: Arc::new(Semaphore::new(incoming_max_connections)),
            conns: Arc::new(DashMap::new()),
            shutdown: CancellationToken::new(),
        }
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterService for TcpService {
    async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        let tx = self.tx.clone();
        let incoming = self.conns.clone();
        let idle_timeout = self.conf.idle_timeout;
        let sema = self.sema.clone();
        let shutdown = self.shutdown.clone();
        let max_frame_body_size = self.conf.max_frame_body_size;
        let send_timeout = self.conf.send_timeout;
        let send_message_buffer_size = self.conf.send_message_buffer_size;
        tokio::spawn(async move {
            loop {
                select! {
                    sp = sema.clone().acquire_owned() => {
                        if sp.is_err() {
                            let e = TransporterError::from_code(ErrorCode::MaxIncomingReached);
                            error!("[Transporter]: Acquire semaphore failed: {}", e);
                            continue;
                        }
                        let sp = sp.unwrap();
                        select!{
                            res = listener.accept() => {
                                if res.is_err() {
                                    let e = TransporterError::new(ErrorCode::AcceptError, res.unwrap_err().to_string());
                                    error!("[Transporter]: Failed to accept connection: {}", e);
                                    continue;
                                }

                                let (stream, remote_addr) = res.unwrap();
                                stream.set_nodelay(true).unwrap_or_else(|e| {
                                    error!("[Transporter]: Failed to set nodelay: {}", e);
                                });

                                let (rh, wh) = stream.into_split();
                                let tx = tx.clone();
                                let shutdown = CancellationToken::new();
                                let conn_id = get_conn_id();

                                // 启动 tcp reader 任务
                                let reader = TcpReadHalf {
                                    tx,
                                    conn_id,
                                    remote_addr,
                                    max_frame_body_size,
                                    shutdown: shutdown.clone(),

                                    idle_timeout,
                                };
                                tokio::spawn(reader.loop_handle(rh));

                                // 启动 tcp writer 任务
                                let (wtx, wrx) = tokio::sync::mpsc::channel(send_message_buffer_size);
                                let wh = TcpWriteHalf::new(wh, send_timeout, conn_id, remote_addr, shutdown.clone());
                                tokio::spawn(wh.loop_handle(wrx));


                                incoming.insert(conn_id, TransporterWriter{ tx: wtx, conn_id, remote_addr:remote_addr, shotdown: shutdown });
                            }

                            _ = shutdown.cancelled() => {
                                break;
                            }
                        }
                    }

                    _ = shutdown.cancelled() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    // 广播
    async fn broadcast(&self, cmd: &TransportMessage) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        let data = cmd.to_bytes()?;
        let data = Bytes::from(data.clone());
        for cell in self.conns.iter_mut() {
            let mut writer = cell.value().clone();
            let msg = data.clone();
            tokio::spawn(async move {
                let _ = writer.send_bytes(msg, None).await;
            });
        }
        Ok(())
    }

    async fn split_writer(&self, conn_id: u64) -> Option<TransporterWriter> {
        if let Some(cell) = self.conns.get(&conn_id) {
            Some(cell.value().clone())
        } else {
            None
        }
    }

    // 关闭指定连接
    async fn close(&self, conn_id: u64) -> Result<()> {
        if let Some(pair) = self.conns.remove(&conn_id) {
            pair.1.close().await;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpService {
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        if let Some(cell) = self.conns.get(&cmd.conn_id) {
            cell.value().send(cmd, t).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterShutdown for TcpService {
    async fn shutdown(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.shutdown.cancel();
        // 关闭所有入站连接
        for cell in self.conns.iter_mut() {
            cell.value().close().await;
        }
    }
}

impl ProtocolGetRemoteAddr for TcpService {
    fn get_remote_addr(&self, conn_id: u64) -> Option<SocketAddr> {
        if let Some(cell) = self.conns.get(&conn_id) {
            Some(cell.value().remote_addr)
        } else {
            None
        }
    }
}
