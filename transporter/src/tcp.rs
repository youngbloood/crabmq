use crate::{
    ProtocolTransporterManager, ProtocolTransporterWriter, TransportMessage,
    TransporterWriter,
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

pub struct Tcp {
    incoming_max_connection: usize, // 接受的最大连接数
    incoming_sema: Arc<Semaphore>,  // 用于限制最大连接数的信号量

    outgoing_max_connection: usize, // 连接的最大连接数
    outgoing_sema: Arc<Semaphore>,  // 用于限制最大连接数的信号量

    addr: String,
    tx: UnboundedSender<TransportMessage>,
    rx: UnboundedReceiver<TransportMessage>,

    incoming: Arc<DashMap<String, WriteHalf>>,
    outgoing: Arc<DashMap<String, WriteHalf>>,

    shutdown: CancellationToken,
}

impl Tcp {
    pub fn new(
        addr: String,
        incoming_max_connection: usize,
        outgoing_max_connection: usize,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Tcp {
            addr,
            tx,
            rx,
            incoming_max_connection,
            incoming_sema: Arc::new(Semaphore::new(incoming_max_connection)),
            outgoing_max_connection,
            outgoing_sema: Arc::new(Semaphore::new(outgoing_max_connection)),
            incoming: Arc::new(DashMap::new()),
            outgoing: Arc::new(DashMap::new()),
            shutdown: CancellationToken::new(),
        }
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterManager for Tcp {
    async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        let tx = self.tx.clone();
        let incoming = self.incoming.clone();
        let sema = self.incoming_sema.clone();
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
                                    sp,
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

    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn recv(&mut self, timeout: u64) -> Option<TransportMessage> {
        let shutdown = self.shutdown.clone();
        if timeout == 0 {
            select! {
                msg = self.rx.recv() => msg,
                _ = shutdown.cancelled() => None,
            }
        } else {
            select! {
                msg = tokio::time::timeout(std::time::Duration::from_secs(timeout), self.rx.recv()) => {
                    msg.ok().flatten()
                },
                _ = shutdown.cancelled() => None,
            }
        }
    }
    // 将本地监听服务分离出一个新的 TransporterWriter，remote_addr 是要分离的连接地址
    async fn split_writer(&self, remote_addr: &str) -> Option<TransporterWriter> {
        if let Some(pair) = self.incoming.remove(remote_addr) {
            let tcp_writer = TcpWriter {
                remote_addr: pair.0.to_string(),
                w: pair.1,
            };
            Some(TransporterWriter::from_tcp(tcp_writer))
        } else {
            None
        }
    }

    // 广播
    async fn broadcast_all(&self, cmd: &TransportMessage) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        for cell in self.incoming.iter_mut() {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        for cell in self.outgoing.iter_mut() {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        Ok(())
    }

    // 广播到所有连接至本地的 endpoints
    async fn broadcast_incoming(&self, cmd: &TransportMessage) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        for cell in self.incoming.iter_mut() {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        Ok(())
    }

    // 广播到所有连接至远端的 endpoints
    async fn broadcast_outgoing(&self, cmd: &TransportMessage) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        for cell in self.outgoing.iter_mut() {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }
        Ok(())
    }

    // 连接到远端地址，建立连接并加入到管理器中
    async fn connect(&self, remote_addr: &str) -> Result<()> {
        let shutdown = self.shutdown.clone();
        let outgoing = self.outgoing.clone();
        select! {
            sp =  self.outgoing_sema.clone().acquire_owned() => {
                if sp.is_err() {
                    return  Err(TransporterError::from_code(ErrorCode::MaxOutgoingReached).into());
                }
                let sp = sp.unwrap();
                select!{
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
                            sp,
                            remote_addr: remote_addr.to_string(),
                            shutdown: shutdown.clone(),
                        };
                        tokio::spawn(reader.loop_handle());
                        outgoing.insert(remote_addr.to_string(), WriteHalf::new(wh, shutdown));
                    }

                    _ = shutdown.cancelled() => {
                        return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
                    }
                }
            }

            _ = shutdown.cancelled() => {
                return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
            }
        }
        Ok(())
    }

    async fn send(&self, cmd: &TransportMessage) -> Result<()> {
        if self.shutdown.is_cancelled() {
            return Err(TransporterError::from_code(ErrorCode::ServiceShutdown).into());
        }
        if let Some(cell) = self.outgoing.get(&cmd.remote_addr) {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }

        if let Some(cell) = self.incoming.get(&cmd.remote_addr) {
            cell.value()
                .w
                .lock()
                .await
                .write_all(&cmd.to_bytes()?)
                .await?;
        }

        Ok(())
    }

    // 关闭指定连接
    async fn close(&self, remote_addr: &str) {
        if let Some(pair) = self.incoming.remove(remote_addr) {
            pair.1.close();
        }
        if let Some(pair) = self.outgoing.remove(remote_addr) {
            pair.1.close();
        }
    }

    async fn shutdown(&self) {
        self.shutdown.cancel();
        // 关闭所有入站连接
        for cell in self.incoming.iter_mut() {
            cell.value().close();
        }
        // 关闭所有出站连接
        for cell in self.outgoing.iter_mut() {
            cell.value().close();
        }
    }
}

pub(crate) struct TcpReader {
    r: OwnedReadHalf,
    tx: UnboundedSender<TransportMessage>,
    remote_addr: String,
    sp: OwnedSemaphorePermit,
    shutdown: CancellationToken,
}

impl TcpReader {
    async fn loop_handle(mut self) {
        let mut head = [0_u8; 5];
        loop {
            select! {
                head_res = self.r.read_exact(&mut head) => {
                    match head_res {
                        Ok(n) if n ==0 || n != 5 => break, // 连接关闭
                        Ok(_) => {
                            let index = head[0];
                            let length = u32::from_be_bytes(head[1..5].try_into().unwrap());

                            let mut body = vec![0_u8; length as usize];

                            select!{
                                body_res = self.r.read_exact(&mut body) => {
                                    match body_res {
                                        Ok(n) if n == 0 || n != length as usize => break, // 连接关闭
                                        Ok(_n) =>  {
                                            if let Err(e) = handle_message(self.tx.clone(), index, &body, self.remote_addr.clone()) {
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
        self.sp.forget();
    }
}

#[derive(Clone)]
pub struct TcpWriter {
    // 连接地址
    remote_addr: String,
    w: WriteHalf,
}

unsafe impl Send for TcpWriter {}
unsafe impl Sync for TcpWriter {}

#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpWriter {
    async fn send(&self, cmd: &TransportMessage) -> Result<()> {
        if self.w.closed() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }
        if let Err(e) = self.w.w.lock().await.write_all(&cmd.to_bytes()?).await {
            return Err(TransporterError::new(ErrorCode::WriteError, e.to_string()).into());
        }
        Ok(())
    }

    async fn send_timeout(&self, cmd: &TransportMessage, t: Duration) -> Result<()> {
        if self.w.closed() {
            return Err(TransporterError::from_code(ErrorCode::ConnectionClosed).into());
        }

        timeout(t, self.w.w.lock().await.write_all(&cmd.to_bytes()?))
            .await
            .map_err(|e| -> anyhow::Error {
                TransporterError::new(ErrorCode::WriteTimeoutError, e.to_string()).into()
            })?
            .map_err(|e| -> anyhow::Error {
                TransporterError::new(ErrorCode::WriteError, e.to_string()).into()
            })?;

        Ok(())
    }

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
}
