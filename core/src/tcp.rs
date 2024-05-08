use crate::message::Message;
use crate::tsuixuq::TsuixuqOption;
use crate::{client::Client, tsuixuq::Tsuixuq};
use common::global::CANCEL_TOKEN;
use common::ArcMuxRefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    select,
    sync::mpsc::{self, Receiver, Sender},
};
use tracing::info;

pub struct TcpServer {
    opt: Arc<TsuixuqOption>,
    tcp_listener: TcpListener,
    clients: HashMap<String, Client>,
}

impl TcpServer {
    pub fn new(opt: Arc<TsuixuqOption>, mut tcp_listener: TcpListener) -> Self {
        TcpServer {
            opt,
            tcp_listener: tcp_listener,
            clients: HashMap::new(),
        }
    }

    pub async fn serve(&mut self, msg_sender: Sender<Message>, daemon: ArcMuxRefCell<Tsuixuq>) {
        let (tx, mut rx) = mpsc::channel(10000);
        tokio::spawn(handle_params(rx, msg_sender));
        loop {
            select! {
                handle = self.tcp_listener.accept() => {
                    match handle{
                        Ok((socket,addr)) => {
                            info!("recieve connection from {addr:?}");
                            let sender = tx.clone();
                            let daemon_clone = daemon.clone();
                            // 每个socket交由单独的一个Future处理
                            tokio::spawn(async {
                                let mut client = Client::new(socket,daemon_clone);
                                client.io_loop(sender).await;
                            });
                        }
                        Err(err)=>{
                           info!("socket exit with err: {err:?}");
                           continue
                        }
                    }
                }
                _ = CANCEL_TOKEN.cancelled() => {
                    return
                }
            }
        }
    }

    fn fin(&mut self) {}

    fn rdy(&mut self) {}

    // fn req(&mut self    fn publish(&mut self) {}

    fn mpub(&mut self) {}

    fn dpub(&mut self) {}

    fn nop(&mut self) {}

    fn touch(&mut self) {}

    fn sub(&mut self) {}

    fn cls(&mut self) {}

    fn auth(&mut self) {}
}

// 在tcp层统一处理进入消息，包含鉴权等
async fn handle_params(mut recver: Receiver<Message>, sender: Sender<Message>) {
    while let msg = recver.recv().await.unwrap() {
        // TODO: handle params
        sender.send(msg).await;
    }
}
