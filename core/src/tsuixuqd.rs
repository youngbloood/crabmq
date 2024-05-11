use crate::{
    tcp::TcpServer,
    tsuixuq::{Tsuixuq, TsuixuqOption},
};
use anyhow::{anyhow, Result};
use common::{global, ArcMux};
use std::sync::Arc;
use tokio::{net::TcpListener, select, sync::Mutex};
use tracing::info;

pub struct Tsuixuqd {
    opt: Arc<TsuixuqOption>,
    tsuixuq: ArcMux<Tsuixuq>,
}

impl Tsuixuqd {
    pub fn new(mut opt: TsuixuqOption) -> Self {
        let opt_arc = Arc::new(opt);
        Tsuixuqd {
            opt: opt_arc.clone(),
            tsuixuq: Arc::new(Mutex::new(Tsuixuq::new(opt_arc.clone()))),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn serve(&mut self) -> Result<()> {
        select! {
            // tcp server
            r1 =  tokio::spawn(Self::serve_tcp(
                self.opt.clone(),
                self.tsuixuq.clone(),
            ))=>{
                match r1{
                    Ok(v)=>{
                        if let Err(e)=v{
                            return Err(anyhow!(e));
                        }
                    }
                    Err(e)=>{
                        return Err(anyhow!(e));
                    }
                }
            }
            // TODO: http server

            _=global::CANCEL_TOKEN.cancelled()=>{
                return Err(anyhow!("process stopped"))
            }
        }
        Ok(())
    }

    async fn serve_tcp(opt: Arc<TsuixuqOption>, tsuixuq: ArcMux<Tsuixuq>) -> Result<()> {
        // start tcp serve
        let tcp_port = opt.tcp_port;
        let tsuixuq_clone: Arc<Mutex<Tsuixuq>> = tsuixuq.clone();
        let opt_arc = opt.clone();

        match tokio::spawn(async move {
            info!("start listen port: {}", tcp_port);
            match TcpListener::bind(format!("127.0.0.1:{}", tcp_port)).await {
                Err(err) => {
                    return Err(anyhow!("listen port failed: {err}"));
                }
                Ok(tcp_listener) => {
                    // 将处理tcp_listener单独放到一个Future中处理
                    tokio::spawn(async move {
                        let mut tcp_server = TcpServer::new(opt_arc, tcp_listener, tsuixuq_clone);
                        tcp_server.serve().await;
                    });
                    return Ok(());
                }
            }
        })
        .await
        {
            Ok(v) => {
                if let Err(e) = v {
                    return Err(anyhow!(e));
                }
            }
            Err(e) => {
                return Err(anyhow!(e));
            }
        }

        global::CANCEL_TOKEN.cancelled().await;
        // info!("start recieve params from socket");
        // while let Some((remote_addr, msg)) = rx.recv().await {
        //     info!("收到消息: {remote_addr},{msg:?}");
        //     match msg.action() {
        //         ACTION_FIN => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.fin(remote_addr, msg).await;
        //         }
        //         ACTION_RDY => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.rdy(remote_addr, msg).await;
        //         }
        //         ACTION_REQ => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.req(remote_addr, msg).await
        //         }
        //         ACTION_PUB => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.publish(remote_addr, msg).await;
        //             drop(daemon);
        //         }
        //         ACTION_NOP => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.nop(remote_addr, msg).await;
        //         }
        //         ACTION_TOUCH => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.touch(remote_addr, msg).await;
        //         }
        //         ACTION_SUB => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.sub(remote_addr, msg).await;
        //             drop(daemon);
        //         }
        //         ACTION_CLS => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.cls(remote_addr, msg).await;
        //         }
        //         ACTION_AUTH => {
        //             let mut daemon = tsuixuq.lock().await;
        //             daemon.auth(remote_addr, msg).await;
        //         }
        //         _ => unreachable!(),
        //     }
        // }
        Ok(())
    }
}
