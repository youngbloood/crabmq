use crate::{
    ProtocolTransporterManager, ProtocolTransporterWriter, TransportMessage, TransportProtocol,
    TransporterWriter,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use log::error;
use protocolv2::{BrokerCooHeartbeatRequest, Decoder};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::{
        Mutex, OwnedSemaphorePermit, Semaphore, SemaphorePermit,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
};
use tokio_util::sync::CancellationToken;

pub struct Tcp {
    incoming_max_connection: usize, // 接受的最大连接数
    sema: Arc<Semaphore>,           // 用于限制最大连接数的信号量
    addr: String,
    tx: UnboundedSender<TransportMessage>,
    rx: UnboundedReceiver<TransportMessage>,

    incoming: Arc<DashMap<String, WriteHalf>>,
    outgoing: Arc<DashMap<String, WriteHalf>>,
}

impl Tcp {
    pub fn new(addr: String, incoming_max_connection: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Tcp {
            addr,
            tx,
            rx,
            incoming_max_connection,
            sema: Arc::new(Semaphore::new(incoming_max_connection)),
            incoming: Arc::new(DashMap::new()),
            outgoing: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ProtocolTransporterManager for Tcp {
    async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        let tx = self.tx.clone();
        let incoming = self.incoming.clone();
        let sema = self.sema.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let sp = sema.acquire_owned().await.unwrap();
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
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        Ok(())
    }
    // 本地的监听服务启动后，从 channel 中获取消息，timeout 为 0 表示一直等待直到有消息到来
    async fn recv(&mut self, timeout: u64) -> Option<TransportMessage> {
        if timeout == 0 {
            self.rx.recv().await
        } else {
            tokio::time::timeout(std::time::Duration::from_secs(timeout), self.rx.recv())
                .await
                .ok()
                .flatten()
        }
    }
    // 将本地监听服务分离出一个新的 TransporterWriter，remote_addr 是要分离的连接地址
    async fn split_writer(&self, remote_addr: &str) -> Option<TransporterWriter> {
        if let Some(pair) = self.incoming.remove(remote_addr) {
            Some(TransporterWriter {
                p: TransportProtocol::TCP,
                w: Box::new(TcpWriter {
                    remote_addr: pair.0.to_string(),
                    w: pair.1,
                }) as Box<dyn ProtocolTransporterWriter>,
            })
        } else {
            None
        }
    }

    // 广播
    async fn broadcast_all(&self, cmd: &TransportMessage) -> Result<()> {
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
    async fn connect(&self, remote_addr: String) -> Result<()> {
        let stream = tokio::net::TcpStream::connect(&remote_addr).await?;
        let (rh, wh) = stream.into_split();
        let tx = self.tx.clone();
        let shutdown = CancellationToken::new();
        let reader = TcpReader {
            r: rh,
            tx,
            remote_addr: remote_addr.clone(),
            shutdown: shutdown.clone(),
        };
        tokio::spawn(reader.loop_handle());
        self.outgoing
            .insert(remote_addr.clone(), WriteHalf::new(wh, shutdown));
        Ok(())
    }

    async fn send(&self, cmd: &TransportMessage) -> Result<()> {
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
                            let index = head[1];
                            let length = u32::from_be_bytes(head[1..5].try_into().unwrap());

                            let mut body = vec![0_u8; length as usize];

                            select!{
                                body_res = self.r.read_exact(&mut body) => {
                                    match body_res {
                                        Ok(n) if n == 0 || n != length as usize => break, // 连接关闭
                                        Ok(n) => match index {
                                            protocolv2::BROKER_COO_HEARTBEAT_REQUEST_INDEX => {
                                                match BrokerCooHeartbeatRequest::decode(&body) {
                                                    Ok(message) => {
                                                        let _ = self.tx.send(TransportMessage {
                                                            index,
                                                            remote_addr: self.remote_addr.clone(),
                                                            message: Box::new(message)
                                                                as Box<dyn protocolv2::EnDecoder>,
                                                        });
                                                    }
                                                    Err(_) => todo!(),
                                                }
                                            }

                                            _ => {
                                                eprintln!("Unknown message index: {}", index);
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("Failed to read message body: {}", e);
                                            break;
                                        }
                                    }
                                }

                                _ = self.shutdown.cancelled() => {
                                    break;
                                }
                            }
                        }

                        Err(e) => {
                            error!("Transporter read err: {}",e);
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
pub(crate) struct TcpWriter {
    // 连接地址
    remote_addr: String,
    w: WriteHalf,
}

#[async_trait::async_trait]
impl ProtocolTransporterWriter for TcpWriter {
    async fn send(&mut self, cmd: &TransportMessage) -> Result<()> {
        if self.w.closed() {
            return Err(anyhow!("Tcp connect {} has been closed", self.remote_addr));
        }
        self.w.w.lock().await.write_all(&cmd.to_bytes()?).await?;
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
