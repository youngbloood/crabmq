use crate::conn::{read_parse, write, Conn};
use crate::tsuixuq::TsuixuqOption;
use crate::{channel::Channel, message::Message};
use common::global::CANCEL_TOKEN;
use common::ArcMux;
use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::time::Interval;
use tokio::{select, time};
use tracing::{debug, error, info, warn};

type ClientState = u8;
const CLIENT_STATE_NOT_READY: ClientState = 1;
// 表示一个链接的客户端
pub struct Client {
    conn: Conn,
    remote_addr: SocketAddr,
    opt: Arc<TsuixuqOption>,
    channel: ArcMux<Channel>,
    ticker: Interval,
    defeat_count: u16,

    state: ClientState,
}

impl Drop for Client {
    fn drop(&mut self) {
        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "drop the client");
    }
}

impl Client {
    pub fn new(socket: TcpStream, remote_addr: SocketAddr, opt: Arc<TsuixuqOption>) -> Self {
        let addr = remote_addr.to_string();
        debug!(addr = addr, "new client");
        Client {
            remote_addr,
            conn: Conn::new(socket),
            channel: Arc::new(Mutex::new(Channel::new())),
            ticker: time::interval(Duration::from_secs(opt.client_timeout as u64)),
            defeat_count: 0,
            state: CLIENT_STATE_NOT_READY,
            opt,
        }
    }

    pub fn set_channel(&mut self, channel: ArcMux<Channel>) {
        self.channel = channel;
    }

    pub async fn io_loop(&mut self, sender: Sender<Message>) {
        let addr = self.remote_addr.to_string();
        debug!(addr = addr, "start client io_loop");

        loop {
            if self.defeat_count > self.opt.client_timeout_count {
                info!(addr = addr, "not response, then will disconnect");
                return;
            }
            select! {
                result = read_parse(&mut self.conn.reader) =>{
                    match result{
                        Ok(params)=>{
                            debug!("params={params:?}");
                            match sender.send(params).await{
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

                _ = sender.closed() => {
                    error!(addr = addr, "tsuixuq sender is closed");
                    return;
                }

                _ = self.ticker.tick() => {
                    let count = self.defeat_count;
                    warn!(addr = addr, "timeout, count: {count}");
                    self.defeat_count += 1;
                }

                msg = async {
                    let mut chan = self.channel.lock().await;
                    return chan.borrow_mut().recv_msg().await;
                } => {
                    if let Err(e) = write(&mut self.conn.writer,&msg.as_bytes()).await{
                        error!(addr = addr, "write msg err: {e:?}");
                    }
                }

                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }
            }
        }
    }
}
