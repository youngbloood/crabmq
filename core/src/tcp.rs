use crate::client::{io_loop, Client};
use crate::crab::{Crab, CrabMQOption};
use common::global::{Guard, CANCEL_TOKEN, CLIENT_DROP_GUARD};
use protocol::message::Message;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::{net::TcpListener, select};
use tracing::{error, info, warn};

pub struct TcpServer {
    // 控制消息流入(从客户端流入MQ)
    in_sender: Sender<(String, Message)>,
    in_recver: Receiver<(String, Message)>,

    // 控制消息流出(从MQ响应至客户端)
    out_sender: Sender<(String, Message)>,
    out_recver: Receiver<(String, Message)>,

    opt: Guard<CrabMQOption>,
    tcp_listener: TcpListener,
    clients: HashMap<String, Guard<Client>>,
    crab: Guard<Crab>,
}

impl TcpServer {
    pub fn new(opt: Guard<CrabMQOption>, tcp_listener: TcpListener, crab: Guard<Crab>) -> Self {
        let (in_tx, in_rx) = mpsc::channel(10000);
        let (out_tx, out_rx) = mpsc::channel(10000);
        crab.get_mut().out_sender = Some(out_tx.clone());
        TcpServer {
            in_sender: in_tx,
            in_recver: in_rx,

            out_sender: out_tx,
            out_recver: out_rx,

            opt,
            tcp_listener,
            clients: HashMap::new(),
            crab,
        }
    }

    pub async fn serve(&mut self) {
        info!("start tcp serve");
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    info!("recieve global cancelled");
                    return
                }

                handle = self.tcp_listener.accept() => {
                    info!("accept a tcp handler");
                    match handle{
                        Ok((socket,addr)) => {
                            info!("recieve connection from {addr:?}");
                            let opt = self.opt.clone();
                            let crab = self.crab.clone();

                            let client = Client::new(socket,addr,opt,crab);
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
                    info!("recieve a message from in_recver");
                    if msg_opt.is_none(){
                        continue;
                    }
                    let (addr,mut msg) = msg_opt.unwrap();
                    let (resp, passed) = self.validate(addr.as_str(), &msg);
                    if !passed {
                        let _ = self.out_sender.send((addr, resp.unwrap())).await;
                        continue;
                    }
                    if let Err(e) = msg.init(){
                        error!(addr = addr, "init msg error: {e}");
                        continue;
                    }
                    let client_guard = self.clients.get(addr.as_str()).unwrap().clone();
                    self.crab.get_mut().handle_message(client_guard,self.out_sender.clone(),addr.as_str(),msg).await;
                }

                // 处理响应至客户端的消息
                msg_opt = self.out_recver.recv() => {
                    info!("recieve a message from out_recver");
                    let (addr,msg) = msg_opt.unwrap();
                    let client_guard = self.clients.get(addr.as_str()).unwrap().clone();
                    let _ = client_guard.get_mut().send_msg(msg).await;
                }

                // 删除收到的client
                addr = CLIENT_DROP_GUARD.recv() => {
                    info!("drop client message");
                    let address = addr.as_str();
                    self.clients.remove(addr.as_str());
                    self.crab.get_mut().delete_client_from_channel(addr.as_str()).await;
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
}
