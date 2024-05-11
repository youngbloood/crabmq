use crate::conn::{read_parse, write, Conn};
use crate::message::Message;
use crate::protocol::{
    ACTION_AUTH, ACTION_CLS, ACTION_FIN, ACTION_NOP, ACTION_PUB, ACTION_RDY, ACTION_REQ,
    ACTION_SUB, ACTION_TOUCH,
};
use crate::tsuixuq::{Tsuixuq, TsuixuqOption};
use anyhow::Result;
use common::global::CANCEL_TOKEN;
use common::ArcMux;
use std::cell::UnsafeCell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
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
    opt: Arc<TsuixuqOption>,
    // 超时ticker
    ticker: UnsafeCell<Interval>,
    defeat_count: AtomicU16,

    msg_rx: UnsafeCell<Receiver<Message>>,
    msg_tx: UnsafeCell<Sender<Message>>,

    state: ClientState,

    mutex: Mutex<()>,
    tsuixuq: ArcMux<Tsuixuq>,
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
        opt: Arc<TsuixuqOption>,
        tsuixuq: ArcMux<Tsuixuq>,
    ) -> Self {
        let addr = remote_addr.to_string();
        debug!(addr = addr, "new client");

        let (tx, rx) = mpsc::channel(1);
        Client {
            remote_addr,
            tsuixuq,
            conn: UnsafeCell::new(Conn::new(socket)),
            ticker: UnsafeCell::new(time::interval(Duration::from_secs(
                opt.client_timeout as u64,
            ))),
            defeat_count: AtomicU16::new(0),
            state: CLIENT_STATE_NOT_READY,
            opt,
            mutex: Mutex::default(),

            msg_rx: UnsafeCell::new(rx),
            msg_tx: UnsafeCell::new(tx),
        }
    }

    pub fn builder(self: Self) -> ClientGuard {
        ClientGuard {
            inner: Arc::new(self),
        }
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

    pub async fn io_loop(&self, guard: ClientGuard) {
        // let client = client.borrow_mut();
        // 用于处理从客户端接收到的消息流
        let (tx, mut rx) = mpsc::channel(1);

        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "start client io_loop");

        let conn = unsafe { self.conn.get().as_mut() }.unwrap();
        let ticker = unsafe { self.ticker.get().as_mut() }.unwrap();

        loop {
            if self.defeat_count.load(Ordering::Relaxed) > self.opt.client_timeout_count {
                info!(addr = addr, "not response, then will disconnect");
                return;
            }
            select! {
                // 全局取消信号
                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }

                // 不断从链接中解析数据
                result = read_parse(&mut conn.reader) =>{
                    match result{
                        Ok(msg)=>{
                            match tx.send(msg).await{
                                Ok(())=>{
                                    info!(addr = addr, "send msg to tsuixuq success");
                                    continue
                                }
                                Err(e) =>{
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
                    if let Err(e) = write(&mut conn.writer,&msg.as_bytes()).await{
                        error!(addr = addr, "write msg err: {e:?}");
                    }
                }

                // 处理从链接中收到的msg
                msg_opt = rx.recv() =>{
                    if msg_opt.is_none(){
                        continue;
                    }
                    let msg = msg_opt.unwrap();
                    let ref_clone = guard.clone();
                    match msg.action() {
                        ACTION_FIN => self.fin(msg).await,
                        ACTION_RDY => self.rdy(msg).await,
                        ACTION_REQ => self.req(msg).await,
                        ACTION_PUB => self.publish(msg).await,
                        ACTION_NOP => self.nop(msg).await,
                        ACTION_TOUCH => self.touch(msg).await,
                        ACTION_SUB => self.sub(msg,ref_clone).await,
                        ACTION_CLS =>self.cls(msg).await,
                        ACTION_AUTH => self.auth(msg).await,
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    //============================ Handle Action ==============================//
    pub async fn fin(&self, msg: Message) {}

    pub async fn rdy(&self, msg: Message) {}

    pub async fn publish(&self, msg: Message) {
        let mut daemon = self.tsuixuq.lock().await;
        let _ = daemon.send_message(msg).await;
        info!("发布消息成功");
    }
    pub async fn req(&self, msg: Message) {}

    pub async fn nop(&self, msg: Message) {}

    pub async fn touch(&self, msg: Message) {}

    pub async fn sub(&self, msg: Message, guard: ClientGuard) {
        info!("订阅消息  msg = {msg:?}");
        let topic_name = msg.get_topic();
        let chan_name = msg.get_channel();

        let tsuixuq = self.tsuixuq.clone();
        let mut daemon = tsuixuq.lock().await;
        let topic = daemon
            .get_or_create_topic(topic_name)
            .expect("get topic err");
        let chan = topic.get_create_mut_channel(chan_name);

        info!("将client设置到channel中");
        chan.lock()
            .await
            .set_client(self.remote_addr.to_string(), guard);
        info!("将client设置到channel中  成功");
    }

    pub async fn cls(&self, msg: Message) {}

    pub async fn auth(&self, msg: Message) {}
    //============================ Handle Action ==============================//
}

pub struct ClientGuard {
    inner: Arc<Client>,
}

unsafe impl Sync for ClientGuard {}
unsafe impl Send for ClientGuard {}

impl ClientGuard {
    pub fn clone(&self) -> Self {
        ClientGuard {
            inner: self.inner.clone(),
        }
    }

    pub fn get(&self) -> Arc<Client> {
        self.inner.clone()
    }
}

impl ClientGuard {
    pub async fn io_loop(&self) {
        let client_arc = self.inner.clone();
        let client_guard = self.clone();
        client_arc.io_loop(client_guard).await;
    }
}
