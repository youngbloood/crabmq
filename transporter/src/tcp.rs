#[cfg(feature = "service")]
use crate::TransporterWriter;
#[cfg(feature = "service")]
use crate::conn::ProtocolTransporterService;
#[cfg(feature = "client")]
use crate::conn::{self, ProtocolTransporterClient};

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

#[cfg(feature = "service")]
pub struct TcpService {
    max_connection: usize,      // 接受的最大连接数
    max_frame_body_size: usize, // 每个消息最大 body 大小
    sema: Arc<Semaphore>,       // 用于限制最大连接数的信号量

    addr: String,
    tx: Sender<TransportMessage>,

    // conn_id -> WriteHalf
    conns: Arc<DashMap<u64, TransporterWriter>>,

    shutdown: CancellationToken,
}

#[cfg(feature = "service")]
impl TcpService {
    pub fn new(
        addr: String,
        max_connection: usize,
        max_frame_body_size: usize,
        tx: Sender<TransportMessage>,
    ) -> Self {
        TcpService {
            addr,
            tx,
            max_connection,
            max_frame_body_size,
            sema: Arc::new(Semaphore::new(max_connection)),
            conns: Arc::new(DashMap::new()),
            shutdown: CancellationToken::new(),
        }
    }
}

#[cfg(feature = "service")]
#[async_trait::async_trait]
impl ProtocolTransporterService for TcpService {
    async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        let tx = self.tx.clone();
        let incoming = self.conns.clone();
        let sema = self.sema.clone();
        let shutdown = self.shutdown.clone();
        let max_frame_body_size = self.max_frame_body_size;
        tokio::spawn(async move {
            loop {
                select! {
                    sp = sema.clone().acquire_owned() => {
                        if sp.is_err() {
                            let e = TransporterError::from_code(ErrorCode::MaxIncomingReached);
                            error!("{}", e);
                            continue;
                        }
                        let sp = sp.unwrap();
                        select!{
                            res = listener.accept() => {
                                if res.is_err() {
                                    let e = TransporterError::new(ErrorCode::AcceptError, res.unwrap_err().to_string());
                                    error!("{}", e);
                                    continue;
                                }

                                let (stream, remote_addr) = res.unwrap();
                                stream.set_nodelay(true).unwrap_or_else(|e| {
                                    error!("Failed to set nodelay: {}", e);
                                });

                                let (rh, wh) = stream.into_split();
                                let tx = tx.clone();
                                let shutdown = CancellationToken::new();
                                let conn_id = get_conn_id();
                                let reader = TcpReadHalf {
                                    tx,
                                    conn_id,
                                    remote_addr,
                                    max_frame_body_size,
                                    shutdown: shutdown.clone(),
                                };
                                tokio::spawn(reader.loop_handle(rh));
                                let (wtx,wrx) = tokio::sync::mpsc::channel(10);
                                let wh = TcpWriteHalf::new(wh, remote_addr, shutdown.clone());
                                tokio::spawn(wh.loop_handle(wrx));
                                incoming.insert(conn_id, TransporterWriter{ tx: wtx,remote_addr:remote_addr, shotdown: shutdown });
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

#[cfg(feature = "service")]
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

#[cfg(feature = "service")]
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

#[cfg(feature = "service")]
impl ProtocolGetRemoteAddr for TcpService {
    fn get_remote_addr(&self, conn_id: u64) -> Option<SocketAddr> {
        if let Some(cell) = self.conns.get(&conn_id) {
            Some(cell.value().remote_addr)
        } else {
            None
        }
    }
}

/**
 * TcpClient 定义了连接到远端地址的接口，建立连接并加入到管理器中
 * 可以链接多个远端地址，每个远端地址对应一个连接
 */
#[cfg(feature = "client")]
pub(crate) struct TcpClient {
    remote_addr: SocketAddr,
    tx: Sender<TransportMessage>,
    max_frame_body_size: usize,
    conn: std::sync::Mutex<Option<crate::TransporterWriter>>,
    _permit: OwnedSemaphorePermit,
    shutdown: CancellationToken,
}

#[cfg(feature = "client")]
impl TcpClient {
    pub fn new(
        remote_addr: SocketAddr,
        tx: Sender<TransportMessage>,
        permit: OwnedSemaphorePermit,
        max_frame_body_size: usize,
    ) -> Self {
        TcpClient {
            remote_addr,
            tx,
            conn: std::sync::Mutex::new(None),
            _permit: permit,
            max_frame_body_size,
            shutdown: CancellationToken::new(),
        }
    }
}

#[cfg(feature = "client")]
impl Drop for TcpClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(feature = "client")]
#[async_trait::async_trait]
impl ProtocolTransporterClient for TcpClient {
    // 连接到远端地址，建立连接并加入到管理器中
    async fn connect(&self, remote_addr: SocketAddr, t: Duration) -> Result<u64> {
        let shutdown = self.shutdown.clone();
        let mut conn_id = 0;
        select! {
            stream =  tokio::net::TcpStream::connect(&remote_addr) => {
                if stream.is_err(){
                    return Err(TransporterError::new(ErrorCode::ConnectError, stream.unwrap_err().to_string()).into());
                }
                let stream = stream.unwrap();
                let (rh, wh) = stream.into_split();
                let tx = self.tx.clone();
                let shutdown = CancellationToken::new();
                conn_id = get_conn_id();

                // 启动 tcp reader 任务
                let reader = TcpReadHalf {
                    tx,
                    conn_id,
                    remote_addr,
                    shutdown: shutdown.clone(),
                    max_frame_body_size: self.max_frame_body_size,
                };
                tokio::spawn(reader.loop_handle(rh));

                // 启动 tcp writer 任务
                let (wtx, wrx) = tokio::sync::mpsc::channel(10);
                let wh = TcpWriteHalf::new(wh, remote_addr, shutdown.clone());
                tokio::spawn(wh.loop_handle(wrx));

                let mut guard = self.conn.lock().unwrap();
                *guard = Some(crate::TransporterWriter {
                    tx: wtx,
                    remote_addr,
                    shotdown: shutdown.clone(),
                });
            }

            _ = tokio::time::timeout(t, shutdown.cancelled()) => {
                return Err(TransporterError::from_code(ErrorCode::ConnectTimeout).into());
            }
        }
        Ok(conn_id)
    }
}

// #[cfg(feature = "client")]
// #[async_trait::async_trait]
// impl ProtocolTransporterReader for TcpClient {
//     // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
//     async fn recv(&mut self, timeout: Option<Duration>) -> Option<TransportMessage> {
//         let shutdown = self.shutdown.clone();
//         if timeout.is_none() {
//             select! {
//                 msg = self.rx.recv() => msg,
//                 _ = shutdown.cancelled() => None,
//             }
//         } else {
//             select! {
//                 msg = tokio::time::timeout(timeout.unwrap(), self.rx.recv()) => {
//                     msg.ok().flatten()
//                 },
//                 _ = shutdown.cancelled() => None,
//             }
//         }
//     }
// }

#[cfg(feature = "client")]
#[async_trait::async_trait]
impl ProtocolTransporterShutdown for TcpClient {
    async fn shutdown(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        self.shutdown.cancel();
    }
}

#[cfg(feature = "client")]
#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpClient {
    async fn send(&self, cmd: &TransportMessage, timeout: Option<Duration>) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        let conn_opt = { self.conn.lock().unwrap().clone() };
        if conn_opt.is_none() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }

        let shutdown = self.shutdown.clone();
        select! {
            res = async move {
                let conn = conn_opt.unwrap();
                conn.send(cmd, timeout).await
            } => { res }

            _ = shutdown.cancelled() => {
                Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into())
            }
        }
    }
}

#[cfg(feature = "client")]
#[async_trait::async_trait]
impl ProtocolTransporterCloser for TcpClient {
    // 关闭指定连接
    async fn close(&self) {
        todo!()
    }
    // 检查连接是否已关闭
    async fn closed(&self) -> bool {
        todo!()
    }
}

pub(crate) struct TcpReadHalf {
    tx: Sender<TransportMessage>,
    max_frame_body_size: usize,
    conn_id: u64,
    remote_addr: SocketAddr,
    shutdown: CancellationToken,
}

impl Drop for TcpReadHalf {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TcpReadHalf {
    async fn loop_handle(mut self, rh: OwnedReadHalf) {
        let mut framed = FramedRead::new(rh, TransportCodec::new(self.max_frame_body_size));
        let conn_id = self.conn_id;
        let tx = self.tx.clone();
        loop {
            select! {
                frame_res = framed.next() => {
                    if frame_res.is_none() {
                        // 网络断开了，或者说对端主动断开了连接
                        return;
                    }
                    match frame_res.unwrap() {
                        Ok((version, index, body)) => {
                            let msg = decode_to_message(version, index, conn_id, &body).map_err(|e| -> anyhow::Error {
                                TransporterError::new(ErrorCode::DecodeError, e.to_string()).into()
                            });

                            if let Err(e) = msg {
                                error!("Failed to decode message: {}", e);
                                // 解析爆错了，直接断开连接
                                return;
                            }

                            let msg = msg.unwrap();
                            tx.send(msg).await.unwrap_or_else(|e| {
                                error!("Failed to send message to channel: {}", e);
                            })
                        }

                        Err(e) => {
                            // 网络断开或解析爆错了，直接断开连接
                            return;
                        }
                    }
                }

                _ = self.shutdown.cancelled() => {
                    return;
                }
            }
        }

        self.shutdown.cancel();
    }
}

// #[derive(Clone)]
// pub(crate) struct TcpWriter {
//     // 连接地址
//     conn_id: u64,
//     remote_addr: String,
//     w: TcpWriteHalf,
// }

// unsafe impl Send for TcpWriter {}
// unsafe impl Sync for TcpWriter {}

// #[async_trait::async_trait]
// impl ProtocolTransporterWriter for TcpWriter {
//     async fn send(&self, cmd: &TransportMessage, timeout: Option<Duration>) -> Result<()> {
//         if self.w.closed() {
//             return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
//         }
//         if let Err(e) = self.w.w.lock().await.write_all(&cmd.to_bytes()?).await {
//             return Err(TransporterError::new(ErrorCode::WriteError, e.to_string()).into());
//         }
//         Ok(())
//     }
// }

// #[async_trait::async_trait]
// impl ProtocolTransporterCloser for TcpWriter {
//     async fn closed(&self) -> bool {
//         self.w.closed()
//     }

//     async fn close(&self) {
//         self.w.close();
//     }
// }

pub struct TcpWriteHalf {
    w: OwnedWriteHalf,
    remote_addr: SocketAddr,
    shutdown: CancellationToken,
}

impl TcpWriteHalf {
    fn new(w: OwnedWriteHalf, remote_addr: SocketAddr, shutdown: CancellationToken) -> Self {
        TcpWriteHalf {
            w,
            remote_addr,
            shutdown,
        }
    }

    // fn closed(&self) -> bool {
    //     self.shutdown.is_cancelled()
    // }

    // fn close(&self) {
    //     self.shutdown.cancel();
    // }

    // async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
    //     if self.closed() {
    //         return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
    //     }

    //     if t.is_none() {
    //         if let Err(e) = self.w.write_all(&cmd.to_bytes()?).await {
    //             return Err(TransporterError::new(ErrorCode::WriteError, e.to_string()).into());
    //         }
    //         return Ok(());
    //     }

    //     timeout(t.unwrap(), self.w.write_all(&cmd.to_bytes()?))
    //         .await
    //         .map_err(|e| -> anyhow::Error {
    //             TransporterError::new(ErrorCode::WriteTimeoutError, e.to_string()).into()
    //         })?;
    //     Ok(())
    // }

    async fn loop_handle(mut self, mut rx: Receiver<Bytes>) {
        loop {
            select! {
                msg = rx.recv() => {
                    if msg.is_none() {
                        // channel 断了，说明不再有消息要发送了，可以关闭连接了
                        // self.close();
                        self.shutdown.cancel();
                        return;
                    }
                    let msg = msg.unwrap();
                    if let Err(e) = self.w.write_all(&msg).await {
                        error!("Failed to send message: {}", e);
                        // 发送消息失败了，说明连接有问题了，可以关闭连接了
                        self.shutdown.cancel();
                        return;
                    }
                }

                _ = self.shutdown.cancelled() => {
                    return;
                }
            }
        }
        let _ = self.w.shutdown().await;
    }
}
