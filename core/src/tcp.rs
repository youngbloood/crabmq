use crate::client::{io_loop, Client};
use crate::tsuixuq::{Tsuixuq, TsuixuqOption};
use common::global::{Guard, CANCEL_TOKEN, CLIENT_DROP_GUARD};
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
    clients: HashMap<String, Guard<Client>>,
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

                                // 每个socket交由单独的一个Future处理
                            tokio::spawn(async move {
                                io_loop(guard).await;
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

                // 删除收到的client
                addr = CLIENT_DROP_GUARD.recv() => {
                    let address = addr.as_str();
                    info!("从tpc clients中删除{address}成功");
                    self.clients.remove(addr.as_str());
                    self.tsuixuq.lock().await.delete_client_from_channel(addr.as_str()).await;
                    info!("从 tsuixuq 中删除{address}成功");
                }


                _ = CANCEL_TOKEN.cancelled() => {
                    return
                }
            }
        }
    }
}
