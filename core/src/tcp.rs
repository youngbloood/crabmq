use crate::client::{Client, ClientGuard};
use crate::tsuixuq::{Tsuixuq, TsuixuqOption};
use common::global::CANCEL_TOKEN;
use common::ArcMux;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    select,
    sync::mpsc::{self},
};
use tracing::info;

pub struct TcpServer {
    opt: Arc<TsuixuqOption>,
    tcp_listener: TcpListener,
    clients: HashMap<String, ClientGuard>,
    tsuixuq: ArcMux<Tsuixuq>,
}

impl TcpServer {
    pub fn new(
        opt: Arc<TsuixuqOption>,
        mut tcp_listener: TcpListener,
        tsuixuq: ArcMux<Tsuixuq>,
    ) -> Self {
        TcpServer {
            opt,
            tcp_listener: tcp_listener,
            clients: HashMap::new(),
            tsuixuq,
        }
    }

    pub async fn serve(&mut self) {
        // 用于从TcpServer中删除Client的消息流
        let (client_tx, mut client_rx) = mpsc::channel(1);

        loop {
            select! {
                handle = self.tcp_listener.accept() => {
                    match handle{
                        Ok((socket,addr)) => {
                            info!("recieve connection from {addr:?}");
                            let opt = self.opt.clone();
                            let tsuixuq = self.tsuixuq.clone();

                            let client = Client::new(socket,addr,opt,tsuixuq);
                            let guard = client.builder();
                            self.clients.insert(addr.to_string(),guard.clone());

                            let client_tx = client_tx.clone();
                                // 每个socket交由单独的一个Future处理
                            tokio::spawn(async move {
                                guard.io_loop().await;
                                // 该client遇到某些错误，结束了io_loop，将其发送至client_tx中，等待删除
                                let _ = client_tx.send(addr.to_string()).await;
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
