use crate::client::{io_loop, Client};
use crate::message::Message;
use crate::protocol::*;
use crate::tsuixuq::{Tsuixuq, TsuixuqOption};
use common::global::{Guard, CANCEL_TOKEN, CLIENT_DROP_GUARD};
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::{net::TcpListener, select};
use tracing::{info, warn};

pub struct TcpServer {
    // 控制消息流入(从客户端流入MQ)
    in_sender: Sender<(String, Message)>,
    in_recver: Receiver<(String, Message)>,

    // 控制消息流出(从MQ响应至客户端)
    out_sender: Sender<(String, Message)>,
    out_recver: Receiver<(String, Message)>,

    opt: Guard<TsuixuqOption>,
    tcp_listener: TcpListener,
    clients: HashMap<String, Guard<Client>>,
    tsuixuq: Guard<Tsuixuq>,
}

impl TcpServer {
    pub fn new(
        opt: Guard<TsuixuqOption>,
        mut tcp_listener: TcpListener,
        tsuixuq: Guard<Tsuixuq>,
    ) -> Self {
        let (in_tx, in_rx) = mpsc::channel(10000);
        let (out_tx, out_rx) = mpsc::channel(10000);
        TcpServer {
            in_sender: in_tx,
            in_recver: in_rx,

            out_sender: out_tx,
            out_recver: out_rx,

            opt,
            tcp_listener: tcp_listener,
            clients: HashMap::new(),
            tsuixuq,
        }
    }

    pub async fn serve(&mut self) {
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return
                }

                handle = self.tcp_listener.accept() => {
                    match handle{
                        Ok((socket,addr)) => {
                            info!("recieve connection from {addr:?}");
                            let opt = self.opt.clone();
                            let tsuixuq = self.tsuixuq.clone();

                            let client = Client::new(socket,addr,opt,tsuixuq);
                            let guard = client.builder();
                            self.clients.insert(addr.to_string(),guard.clone());

                            let in_sender_clone = self.in_sender.clone();
                            // 每个socket交由单独的一个Future处理
                            tokio::spawn(async move {
                                io_loop(guard,in_sender_clone).await;
                                // 该client遇到某些错误，结束了io_loop，将其发送至client_tx中，等待删除
                                let _ = CLIENT_DROP_GUARD.send(addr.to_string().as_str()).await;
                            });
                        }
                        Err(err)=>{
                           info!("socket exit with err: {err:?}");
                           continue
                        }
                    }
                }

                // 处理从客户端收到的消息
                msg_opt = self.in_recver.recv() => {
                    if msg_opt.is_none(){
                        continue;
                    }
                    let (addr, msg) = msg_opt.unwrap();
                    let (resp, passed) = self.validate(addr.as_str(), &msg);
                    if !passed {
                        let _ = self.out_sender.send((addr, resp.unwrap()));
                        continue;
                    }

                    let client_guard = self.clients.get(addr.as_str()).unwrap().clone();
                    match msg.action() {
                        ACTION_FIN => self.fin(addr, msg).await,
                        ACTION_RDY => self.rdy(addr, msg).await,
                        ACTION_REQ => self.req(addr, msg).await,
                        ACTION_PUB => self.publish(addr, msg).await,
                        ACTION_NOP => self.nop(addr, msg).await,
                        ACTION_TOUCH => self.touch(addr, msg).await,
                        ACTION_SUB => self.sub(addr, msg, client_guard).await,
                        ACTION_CLS =>self.cls(addr, msg).await,
                        ACTION_AUTH => self.auth(addr, msg).await,
                        _ => unreachable!(),
                    }
                }

                // 处理响应至客户端的消息
                msg_opt = self.out_recver.recv() => {
                   let (addr,msg) = msg_opt.unwrap();
                   let client_guard = self.clients.get(addr.as_str()).unwrap().clone();
                   let _ = client_guard.get_mut().send_msg(msg).await;
                }

                // 删除收到的client
                addr = CLIENT_DROP_GUARD.recv() => {
                    let address = addr.as_str();
                    self.clients.remove(addr.as_str());
                    self.tsuixuq.get_mut().delete_client_from_channel(addr.as_str()).await;
                    info!("client[{address}] timeout, remove it from TcpServer and Channel");
                }
            }
        }
    }

    fn validate(&self, addr: &str, msg: &Message) -> (Option<Message>, bool) {
        match msg.validate(u8::MAX, u64::MAX) {
            Ok(_) => {}
            Err(e) => match msg.clone() {
                Message::Null => unreachable!(),
                Message::V1(mut v1) => {
                    v1.head.set_flag_resq(true);
                    v1.head.set_reject_code(e.code);
                    warn!(addr = addr, "{e}");
                    return (Some(Message::V1(v1)), false);
                }
            },
        }

        (None, true)
    }

    //============================ Handle Action ==============================//
    pub async fn fin(&self, addr: String, msg: Message) {}

    pub async fn rdy(&self, addr: String, msg: Message) {}

    pub async fn publish(&self, addr: String, msg: Message) {
        let daemon = self.tsuixuq.get_mut();
        let _ = daemon.send_message(self.out_sender.clone(), msg).await;
    }
    pub async fn req(&self, addr: String, msg: Message) {}

    pub async fn nop(&self, addr: String, msg: Message) {}

    pub async fn touch(&self, addr: String, msg: Message) {}

    pub async fn sub(&self, addr: String, msg: Message, guard: Guard<Client>) {
        let topic_name = msg.get_topic();
        let chan_name = msg.get_channel();

        let tsuixuq = self.tsuixuq.clone();
        let daemon = tsuixuq.get_mut();
        let topic = daemon
            .get_or_create_topic(topic_name)
            .expect("get topic err");
        let chan = topic.get_mut().get_create_mut_channel(chan_name);
        chan.get_mut().set_client(addr.as_str(), guard);
        info!(
            addr = addr.as_str(),
            "sub topic: {topic_name}, channel: {chan_name}",
        );
        let _ = self.out_sender.send((addr, msg_with_resp(msg))).await;
    }

    pub async fn cls(&self, addr: String, msg: Message) {}

    pub async fn auth(&self, addr: String, msg: Message) {}
    //============================ Handle Action ==============================//
}

fn msg_with_resp(msg: Message) -> Message {
    let mut resp_msg = msg.clone();
    match &mut resp_msg {
        Message::Null => unreachable!(),
        Message::V1(ref mut v1) => v1.head.set_flag_resq(true),
    };
    resp_msg
}
