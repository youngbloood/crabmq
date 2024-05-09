use crate::message::Message;
use crate::tsuixuq::TsuixuqOption;
use crate::{client::Client, tsuixuq::Tsuixuq};
use common::global::{self, CANCEL_TOKEN};
use common::{ArcMux, ArcMuxRefCell};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    net::TcpListener,
    select,
    sync::mpsc::{self, Receiver, Sender},
};
use tracing::info;

pub struct TcpServer {
    opt: Arc<TsuixuqOption>,
    tcp_listener: TcpListener,
    clients: HashMap<String, ArcMux<Client>>,
    daemon: ArcMuxRefCell<Tsuixuq>,
}

impl TcpServer {
    pub fn new(
        opt: Arc<TsuixuqOption>,
        mut tcp_listener: TcpListener,
        daemon: ArcMuxRefCell<Tsuixuq>,
    ) -> Self {
        TcpServer {
            opt,
            tcp_listener: tcp_listener,
            clients: HashMap::new(),
            daemon,
        }
    }

    pub async fn serve(&mut self, msg_sender: Sender<Message>) {
        let (tx, mut rx) = mpsc::channel(10000);
        tokio::spawn(handle_params(rx, msg_sender, self.daemon.clone()));
        let (client_tx, mut client_rx) = mpsc::channel(1);

        loop {
            select! {
                handle = self.tcp_listener.accept() => {
                    match handle{
                        Ok((socket,addr)) => {
                            info!("recieve connection from {addr:?}");
                            let sender = tx.clone();
                            let opt = self.opt.clone();
                            // 每个socket交由单独的一个Future处理
                            let client = Client::new(socket,addr,opt);
                            let client_arc = Arc::new(Mutex::new(client));
                            let addr_str = addr.to_string();
                            self.clients.insert(addr.to_string(), client_arc.clone());
                            let client_tx = client_tx.clone();
                            tokio::spawn(async move {
                                // let mut client = Client::new(socket,opt);
                                client_arc.lock().await.io_loop(sender).await;
                                // 该client遇到某些错误，结束了io_loop，将其发送至client_tx中，等待删除
                                let _ = client_tx.send(addr_str).await;
                            });
                        }
                        Err(err)=>{
                           info!("socket exit with err: {err:?}");
                           continue
                        }
                    }
                }

                // 删除收到的client
                addr_str = client_rx.recv() => {
                    self.clients.remove(addr_str.as_ref().unwrap().as_str());
                }

                _ = CANCEL_TOKEN.cancelled() => {
                    return
                }
            }
        }
    }
}

// 在tcp层统一处理进入消息，包含鉴权等
async fn handle_params(
    mut recver: Receiver<Message>,
    sender: Sender<Message>,
    daemon: ArcMuxRefCell<Tsuixuq>,
) {
    loop {
        select! {
            msg_opt = recver.recv() => {
                let msg = msg_opt.unwrap();
                match msg.action(){
                    ACTION_FIN => daemon.lock().await.borrow_mut().fin(),
                    ACTION_RDY => daemon.lock().await.borrow_mut().rdy(),
                    // ACTION_REQ => daemon.lock().await.borrow_mut().req(),

                    ACTION_PUB => daemon.lock().await.borrow_mut().publish(),
                    ACTION_MPUB => daemon.lock().await.borrow_mut().mpub(),
                    ACTION_DPUB => daemon.lock().await.borrow_mut().dpub(),
                    ACTION_NOP => daemon.lock().await.borrow_mut().nop(),
                    ACTION_TOUCH => daemon.lock().await.borrow_mut().touch(),
                    ACTION_SUB => daemon.lock().await.borrow_mut().sub(),
                    ACTION_CLS => daemon.lock().await.borrow_mut().cls(),
                    ACTION_AUTH => daemon.lock().await.borrow_mut().auth(),
                    _=> panic!("not get here"),
                }
            }

            _ = global::CANCEL_TOKEN.cancelled() => {
                break;
            }
        }
    }
}
