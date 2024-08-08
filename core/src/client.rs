use crate::config::Config;
use crate::conn::Conn;
use crate::crab::Crab;
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use common::Weight;
use protocol::protocol::Protocol;
use protocol::protocol::ProtocolOperation as _;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::Interval;
use tokio::{select, time};
use tracing::{debug, error, info, warn};

/**
 * 1 byte:
 *       1 bit: need identity?
 *       1 bit: need auth?
 */
struct ClientState(u8);
impl Default for ClientState {
    fn default() -> Self {
        let mut state = 0;
        (&mut state).set_1(7);
        (&mut state).set_1(6);
        Self(state)
    }
}

impl ClientState {
    fn is_need_identity(&self) -> bool {
        self.0.is_1(7)
    }

    fn set_need_identity(&mut self, need: bool) -> &mut Self {
        if need {
            (&mut self.0).set_1(7);
        } else {
            (&mut self.0).set_0(7);
        }
        self
    }

    fn is_need_auth(&self) -> bool {
        self.0.is_1(6)
    }

    fn set_need_auth(&mut self, need: bool) -> &mut Self {
        if need {
            (&mut self.0).set_1(6);
        } else {
            (&mut self.0).set_0(6);
        }
        self
    }
}

#[derive(Clone)]
pub struct ClientWrapper {
    inner: Guard<Client>,
}

impl Weight for ClientWrapper {
    fn get_weight(&self) -> usize {
        self.inner.get().get_weight()
    }
}

impl Deref for ClientWrapper {
    type Target = Guard<Client>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ClientWrapper {
    pub fn new(guard: Guard<Client>) -> Self {
        ClientWrapper { inner: guard }
    }
}

// 表示一个链接的客户端
pub struct Client {
    // 该客户端对应的链接
    conn: UnsafeCell<Conn>,
    // 链接远程地址
    remote_addr: SocketAddr,
    //
    opt: Guard<Config>,
    // 超时ticker
    ticker: UnsafeCell<Interval>,
    defeat_count: AtomicU16,

    msg_rx: UnsafeCell<Receiver<Protocol>>,
    msg_tx: UnsafeCell<Sender<Protocol>>,

    state: Guard<ClientState>,

    weight: usize,
    crab: Guard<Crab>,
}

unsafe impl Sync for Client {}
unsafe impl Send for Client {}

impl Drop for Client {
    fn drop(&mut self) {
        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "drop the client");
    }
}

impl Client {
    pub fn new(
        socket: TcpStream,
        remote_addr: SocketAddr,
        opt: Guard<Config>,
        crab: Guard<Crab>,
    ) -> Self {
        let addr = remote_addr.to_string();
        info!("new client {addr:?}");
        debug!(addr = addr, "new client");

        let (tx, rx) = mpsc::channel(1);
        Client {
            remote_addr,
            crab,
            conn: UnsafeCell::new(Conn::new(socket)),
            ticker: UnsafeCell::new(time::interval(Duration::from_secs(
                opt.get().global.client_heartbeat_interval as _,
            ))),
            defeat_count: AtomicU16::new(0),
            state: Guard::new(ClientState::default()),
            opt,

            msg_rx: UnsafeCell::new(rx),
            msg_tx: UnsafeCell::new(tx),
            weight: 0,
        }
    }

    pub fn builder(self) -> Guard<Self> {
        Guard::new(self)
    }

    pub fn get_weight(&self) -> usize {
        self.weight
    }

    pub fn is_need_identity(&self) -> bool {
        self.state.get().is_need_identity()
    }

    pub fn set_has_identity(&self) {
        self.state.get_mut().set_need_identity(false);
    }

    pub fn is_need_auth(&self) -> bool {
        self.state.get().is_need_auth()
    }

    pub fn set_has_auth(&self) {
        self.state.get_mut().set_need_auth(false);
    }

    pub async fn send_msg(&self, prot: Protocol) -> Result<()> {
        let sender = unsafe { self.msg_tx.get().as_ref() }.unwrap();
        sender.send(prot).await?;
        Ok(())
    }

    pub async fn recv_msg(&self) -> Protocol {
        let recver = unsafe { self.msg_rx.get().as_mut() }.unwrap();
        recver.recv().await.unwrap()
    }

    pub async fn io_loop(&self, sender: Sender<(String, Protocol)>) {
        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "start client io_loop");

        let ticker = unsafe { self.ticker.get().as_mut() }.unwrap();
        let mut write_timeout_count = 0;
        let mut read_timeout_count = 0;

        loop {
            if self.defeat_count.load(Ordering::Relaxed) > self.opt.get().global.client_expire_count
            {
                info!(addr = addr, "not response, then will disconnect");
                return;
            }
            if read_timeout_count > self.opt.get().global.client_read_timeout_count {
                info!(addr = addr, "read timeout count exceed maxnium config");
                return;
            }
            if write_timeout_count > self.opt.get().global.client_write_timeout_count {
                info!(addr = addr, "write timeout count exceed maxnium config");
                return;
            }

            let conn = unsafe { self.conn.get().as_mut() }.unwrap();
            select! {
                // 全局取消信号
                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }

                // 不断从链接中解析数据
                result = conn.read_parse(self.opt.get().global.client_read_timeout) => {
                    match result{
                        Ok(prot)=>{
                            match sender.send((self.remote_addr.to_string(),prot)).await{
                                Ok(())=>{
                                    info!(addr = addr, "send msg to crabmq success");
                                    continue
                                }
                                Err(e) =>{
                                    read_timeout_count += 1;
                                    error!(addr = addr, "send msg to crabmq err: {e:?}");
                                    continue
                                }
                            }
                        }
                        Err(e)=>{
                            error!(addr = addr, "read_parse err: {e:?}");
                            return;
                        }
                    }
                }

                // 超时ticker
                _ = ticker.tick() => {
                    let count = self.defeat_count.load(Ordering::Relaxed);
                    warn!(addr = addr, "timeout, count: {count}");
                    // self.defeat_count += 1;
                    self.defeat_count.store(count+1, Ordering::Relaxed)
                }

                // 从self.channel中获取数据并返回给client
                prot = self.recv_msg() => {
                    if let Err(e) =conn.write(&prot.as_bytes(), self.opt.get().global.client_write_timeout).await{
                        write_timeout_count += 1;
                        error!(addr = addr, "write msg err: {e:?}");
                    }
                }
            }
        }
    }
}

pub async fn io_loop(guard: Guard<Client>, sender: Sender<(String, Protocol)>) {
    let client = guard.get();
    client.io_loop(sender).await;
}
