use crate::{
    TransportMessage, TransporterWriter,
    conn::{
        ProtocolTransporterClient, ProtocolTransporterCloser, ProtocolTransporterReader,
        ProtocolTransporterService, ProtocolTransporterShutdown, ProtocolTransporterWriter,
    },
    err::{ErrorCode, TransporterError},
    handle_message,
};
use anyhow::Result;
use dashmap::DashMap;
use log::error;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::{
        Mutex, OwnedSemaphorePermit, Semaphore,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    time::timeout,
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "service")]
pub struct TcpService {
    max_connection: usize, // 接受的最大连接数
    sema: Arc<Semaphore>,  // 用于限制最大连接数的信号量

    addr: String,
    tx: UnboundedSender<TransportMessage>,
    rx: UnboundedReceiver<TransportMessage>,

    conns: Arc<DashMap<String, WriteHalf>>,

    shutdown: CancellationToken,
}

#[cfg(feature = "service")]
impl TcpService {
    pub fn new(addr: String, max_connection: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        TcpService {
            addr,
            tx,
            rx,
            max_connection,
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

                                let (stream, addr) = res.unwrap();
                                let (rh, wh) = stream.into_split();
                                let tx = tx.clone();
                                let shutdown = CancellationToken::new();
                                let addr = addr.to_string();
                                let reader = TcpReader {
                                    r: rh,
                                    tx,
                                    remote_addr: addr.clone(),
                                    shutdown: shutdown.clone(),
                                };
                                tokio::spawn(reader.loop_handle());
                                incoming.insert(addr, WriteHalf::new(wh, shutdown));
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
        for cell in self.conns.iter_mut() {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        Ok(())
    }

    async fn split_writer(&self, remote: &str) -> Option<TransporterWriter> {
        if let Some(cell) = self.conns.get(remote) {
            Some(TransporterWriter::Tcp(TcpWriter {
                remote_addr: remote.to_string(),
                w: cell.value().clone(),
            }))
        } else {
            None
        }
    }

    // // 广播到所有连接至本地的 endpoints
    // async fn broadcast_incoming(&self, cmd: &TransportMessage) -> Result<()> {
    //     if self.shutdown.is_cancelled() {
    //         return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
    //     }
    //     for cell in self.incoming.iter_mut() {
    //         cell.value()
    //             .w
    //             .lock()
    //             .await
    //             .write_all(&cmd.to_bytes()?)
    //             .await?;
    //     }
    //     Ok(())
    // }

    // // 广播到所有连接至远端的 endpoints
    // async fn broadcast_outgoing(&self, cmd: &TransportMessage) -> Result<()> {
    //     if self.shutdown.is_cancelled() {
    //         return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
    //     }
    //     for cell in self.outgoing.iter_mut() {
    //         cell.value()
    //             .w
    //             .lock()
    //             .await
    //             .write_all(&cmd.to_bytes()?)
    //             .await?;
    //     }
    //     Ok(())
    // }

    // // 连接到远端地址，建立连接并加入到管理器中
    // async fn connect(&self, remote_addr: &str) -> Result<()> {
    //     let shutdown = self.shutdown.clone();
    //     let outgoing = self.outgoing.clone();
    //     select! {
    //         sp =  self.outgoing_sema.clone().acquire_owned() => {
    //             if sp.is_err() {
    //                 return  Err(TransporterError::from_code(ErrorCode::MaxOutgoingReached).into());
    //             }
    //             let sp = sp.unwrap();
    //             select!{
    //                 stream =  tokio::net::TcpStream::connect(&remote_addr) => {
    //                     if stream.is_err(){
    //                         return Err(TransporterError::new(ErrorCode::ConnectError, stream.unwrap_err().to_string()).into());
    //                     }
    //                     let stream = stream.unwrap();
    //                     let (rh, wh) = stream.into_split();
    //                     let tx = self.tx.clone();
    //                     let shutdown = CancellationToken::new();
    //                     let reader = TcpReader {
    //                         r: rh,
    //                         tx,
    //                         sp,
    //                         remote_addr: remote_addr.to_string(),
    //                         shutdown: shutdown.clone(),
    //                     };
    //                     tokio::spawn(reader.loop_handle());
    //                     outgoing.insert(remote_addr.to_string(), WriteHalf::new(wh, shutdown));
    //                 }

    //                 _ = shutdown.cancelled() => {
    //                     return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
    //                 }
    //             }
    //         }

    //         _ = shutdown.cancelled() => {
    //             return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
    //         }
    //     }
    //     Ok(())
    // }

    // async fn send(&self, cmd: &TransportMessage) -> Result<()> {
    //     if self.shutdown.is_cancelled() {
    //         return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
    //     }
    //     if let Some(cell) = self.outgoing.get(&cmd.remote_addr) {
    //         cell.value()
    //             .w
    //             .lock()
    //             .await
    //             .write_all(&cmd.to_bytes()?)
    //             .await?;
    //     }

    //     if let Some(cell) = self.incoming.get(&cmd.remote_addr) {
    //         cell.value()
    //             .w
    //             .lock()
    //             .await
    //             .write_all(&cmd.to_bytes()?)
    //             .await?;
    //     }

    //     Ok(())
    // }

    // 关闭指定连接
    async fn close(&self, remote_addr: &str) -> Result<()> {
        if let Some(pair) = self.conns.remove(remote_addr) {
            pair.1.close();
        }
        Ok(())
    }
}

#[cfg(feature = "service")]
#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpService {
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn send(&self, cmd: &TransportMessage, timeout: Option<Duration>) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        if let Some(cell) = self.conns.get(&cmd.remote_addr) {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        Ok(())
    }
}

#[cfg(feature = "service")]
#[async_trait::async_trait]
impl ProtocolTransporterReader for TcpService {
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn recv(&mut self, timeout: Option<Duration>) -> Option<TransportMessage> {
        let shutdown = self.shutdown.clone();
        if timeout.is_none() {
            select! {
                msg = self.rx.recv() => msg,
                _ = shutdown.cancelled() => None,
            }
        } else {
            select! {
                msg = tokio::time::timeout(timeout.unwrap(), self.rx.recv()) => {
                    msg.ok().flatten()
                },
                _ = shutdown.cancelled() => None,
            }
        }
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
            cell.value().close();
        }
    }
}

/**
 * TcpClient 定义了连接到远端地址的接口，建立连接并加入到管理器中
 * 可以链接多个远端地址，每个远端地址对应一个连接
 */
#[cfg(feature = "client")]
pub(crate) struct TcpClient {
    sema: OwnedSemaphorePermit, // 用于限制最大连接数的信号量
    tx: UnboundedSender<TransportMessage>,
    rx: UnboundedReceiver<TransportMessage>,

    conn: Option<WriteHalf>,

    shutdown: CancellationToken,
}

#[cfg(feature = "client")]
impl TcpClient {
    pub fn new(addr: String, sema: OwnedSemaphorePermit) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        TcpClient {
            tx,
            rx,
            conn: None,
            sema,
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
    async fn connect(&self, remote_addr: &str) -> Result<()> {
        let shutdown = self.shutdown.clone();
        select! {
            stream =  tokio::net::TcpStream::connect(&remote_addr) => {
                if stream.is_err(){
                    return Err(TransporterError::new(ErrorCode::ConnectError, stream.unwrap_err().to_string()).into());
                }
                let stream = stream.unwrap();
                let (rh, wh) = stream.into_split();
                let tx = self.tx.clone();
                let shutdown = CancellationToken::new();
                let reader = TcpReader {
                    r: rh,
                    tx,
                    remote_addr: remote_addr.to_string(),
                    shutdown: shutdown.clone(),
                };
                tokio::spawn(reader.loop_handle());
            }

            _ = shutdown.cancelled() => {
                return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
            }
        }
        Ok(())
    }
}

#[cfg(feature = "client")]
#[async_trait::async_trait]
impl ProtocolTransporterReader for TcpClient {
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn recv(&mut self, timeout: Option<Duration>) -> Option<TransportMessage> {
        let shutdown = self.shutdown.clone();
        if timeout.is_none() {
            select! {
                msg = self.rx.recv() => msg,
                _ = shutdown.cancelled() => None,
            }
        } else {
            select! {
                msg = tokio::time::timeout(timeout.unwrap(), self.rx.recv()) => {
                    msg.ok().flatten()
                },
                _ = shutdown.cancelled() => None,
            }
        }
    }
}

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
        if self.conn.is_none() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }

        let shutdown = self.shutdown.clone();
        select! {
            _ = async {
                let conn = self.conn.as_ref().unwrap();
                conn.send(cmd, timeout)
            } => {}

            _ = shutdown.cancelled() => {
                return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
            }
        }
        Ok(())
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

pub(crate) struct TcpReader {
    r: OwnedReadHalf,
    tx: UnboundedSender<TransportMessage>,
    remote_addr: String,
    shutdown: CancellationToken,
}

impl Drop for TcpReader {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TcpReader {
    async fn loop_handle(mut self) {
        // head[0]: version
        // head[1..3]: index
        // head[3..7]: body length
        let mut head = [0_u8; 7];
        loop {
            select! {
                head_res = self.r.read_exact(&mut head) => {
                    match head_res {
                        Ok(n) if n ==0 || n != 7 => break, // 连接关闭
                        Ok(_) => {
                            let version = head[0];
                            let index = u16::from_be_bytes(head[1..3].try_into().unwrap());
                            let length = u32::from_be_bytes(head[3..7].try_into().unwrap());

                            let mut body = vec![0_u8; length as usize];

                            select!{
                                body_res = self.r.read_exact(&mut body) => {
                                    match body_res {
                                        Ok(n) if n == 0 || n != length as usize => break, // 连接关闭
                                        Ok(_n) =>  {
                                            if let Err(e) = handle_message(self.tx.clone(),version, index, &body, self.remote_addr.clone()) {
                                                error!("{}", e);
                                                break;
                                            }
                                        },
                                        Err(e) => {
                                            let e = TransporterError::new(ErrorCode::ReadError, e.to_string());
                                            error!("{}", e);
                                            break;
                                        }
                                    }
                                }

                                _ = self.shutdown.cancelled() => {
                                    let e = TransporterError::from_code(ErrorCode::ConnectionClosed);
                                    error!("{}", e);
                                    break;
                                }
                            }
                        }

                        Err(e) => {
                            let e = TransporterError::new(ErrorCode::ReadError, e.to_string());
                            error!("{}", e);
                            break;
                        }
                    }
                }

                _ = self.shutdown.cancelled() => {
                    break;
                }
            }
        }

        self.shutdown.cancel();
    }
}

#[derive(Clone)]
pub(crate) struct TcpWriter {
    // 连接地址
    remote_addr: String,
    w: WriteHalf,
}

unsafe impl Send for TcpWriter {}
unsafe impl Sync for TcpWriter {}

#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpWriter {
    async fn send(&self, cmd: &TransportMessage, timeout: Option<Duration>) -> Result<()> {
        if self.w.closed() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }
        if let Err(e) = self.w.w.lock().await.write_all(&cmd.to_bytes()?).await {
            return Err(TransporterError::new(ErrorCode::WriteError, e.to_string()).into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterCloser for TcpWriter {
    async fn closed(&self) -> bool {
        self.w.closed()
    }

    async fn close(&self) {
        self.w.close();
    }
}

#[derive(Clone)]
pub struct WriteHalf {
    w: Arc<Mutex<OwnedWriteHalf>>,
    shutdown: CancellationToken,
}

impl WriteHalf {
    fn new(w: OwnedWriteHalf, shutdown: CancellationToken) -> Self {
        WriteHalf {
            w: Arc::new(Mutex::new(w)),
            shutdown,
        }
    }

    fn closed(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    fn close(&self) {
        self.shutdown.cancel();
    }

    async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()> {
        if self.closed() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }

        if t.is_none() {
            if let Err(e) = self.w.lock().await.write_all(&cmd.to_bytes()?).await {
                return Err(TransporterError::new(ErrorCode::WriteError, e.to_string()).into());
            }
            return Ok(());
        }

        timeout(t.unwrap(), self.w.lock().await.write_all(&cmd.to_bytes()?))
            .await
            .map_err(|e| -> anyhow::Error {
                TransporterError::new(ErrorCode::WriteTimeoutError, e.to_string()).into()
            })?;
        Ok(())
    }
}
