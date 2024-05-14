use crate::conn::Conn;
use crate::message::Message;
use crate::tsuixuq::{Tsuixuq, TsuixuqOption};
use anyhow::Result;
use common::global::{Guard, CANCEL_TOKEN};
use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::Interval;
use tokio::{select, time};
use tracing::{debug, error, info, warn};

type ClientState = u8;
const CLIENT_STATE_NOT_READY: ClientState = 1;

pub trait ClientResp {
    fn send(&mut self);
}

// 表示一个链接的客户端
pub struct Client {
    // 该客户端对应的链接
    conn: UnsafeCell<Conn>,
    // 链接远程地址
    remote_addr: SocketAddr,
    //
    opt: Guard<TsuixuqOption>,
    // 超时ticker
    ticker: UnsafeCell<Interval>,
    defeat_count: AtomicU16,

    msg_rx: UnsafeCell<Receiver<Message>>,
    msg_tx: UnsafeCell<Sender<Message>>,

    state: ClientState,

    tsuixuq: Guard<Tsuixuq>,
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
        opt: Guard<TsuixuqOption>,
        tsuixuq: Guard<Tsuixuq>,
    ) -> Self {
        let addr = remote_addr.to_string();
        debug!(addr = addr, "new client");

        let (tx, rx) = mpsc::channel(1);
        Client {
            remote_addr,
            tsuixuq,
            conn: UnsafeCell::new(Conn::new(socket)),
            ticker: UnsafeCell::new(time::interval(Duration::from_secs(
                opt.get().client_heartbeat_interval as _,
            ))),
            defeat_count: AtomicU16::new(0),
            state: CLIENT_STATE_NOT_READY,
            opt,

            msg_rx: UnsafeCell::new(rx),
            msg_tx: UnsafeCell::new(tx),
        }
    }

    pub fn builder(self: Self) -> Guard<Self> {
        Guard::new(self)
    }

    pub async fn send_msg(&self, msg: Message) -> Result<()> {
        let sender = unsafe { self.msg_tx.get().as_ref() }.unwrap();
        sender.send(msg).await?;
        Ok(())
    }

    pub async fn recv_msg(&self) -> Message {
        let recver = unsafe { self.msg_rx.get().as_mut() }.unwrap();
        recver.recv().await.unwrap()
    }

    pub async fn io_loop(&self, sender: Sender<(String, Message)>) {
        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "start client io_loop");

        let ticker = unsafe { self.ticker.get().as_mut() }.unwrap();
        let mut write_timeout_count = 0;
        let mut read_timeout_count = 0;

        loop {
            if self.defeat_count.load(Ordering::Relaxed) > self.opt.get().client_expire_count {
                info!(addr = addr, "not response, then will disconnect");
                return;
            }
            if read_timeout_count > self.opt.get().client_read_timeout_count {
                info!(addr = addr, "read timeout count exceed maxnium config");
                return;
            }
            if write_timeout_count > self.opt.get().client_write_timeout_count {
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
                result = conn.read_parse(self.opt.get().client_read_timeout) => {
                    match result{
                        Ok(msg)=>{
                            match sender.send((self.remote_addr.to_string(),msg)).await{
                                Ok(())=>{
                                    info!(addr = addr, "send msg to tsuixuq success");
                                    continue
                                }
                                Err(e) =>{
                                    read_timeout_count += 1;
                                    error!(addr = addr, "send msg to tsuixuq err: {e:?}");
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
                msg = self.recv_msg() => {
                    if let Err(e) =conn.write(&msg.as_bytes(), self.opt.get().client_write_timeout).await{
                        write_timeout_count += 1;
                        error!(addr = addr, "write msg err: {e:?}");
                    }
                }
            }
        }
    }
}

pub async fn io_loop(guard: Guard<Client>, sender: Sender<(String, Message)>) {
    let client = guard.get();
    client.io_loop(sender).await;
}
