use crate::{
    tcp::TcpServer,
    tsuixuq::{Tsuixuq, TsuixuqOption},
};
use anyhow::{anyhow, Result};
use common::global::{self, Guard};
use tokio::{net::TcpListener, select};
use tracing::info;

pub struct Tsuixuqd {
    opt: Guard<TsuixuqOption>,
    tsuixuq: Guard<Tsuixuq>,
}

impl Tsuixuqd {
    pub fn new(opt: Guard<TsuixuqOption>) -> Self {
        Tsuixuqd {
            tsuixuq: Guard::new(Tsuixuq::new(opt.clone())),
            opt,
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

    async fn serve_tcp(opt: Guard<TsuixuqOption>, tsuixuq: Guard<Tsuixuq>) -> Result<()> {
        // start tcp serve
        let tcp_port = opt.get().tcp_port;
        let tsuixuq_clone: Guard<Tsuixuq> = tsuixuq.clone();
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
        Ok(())
    }
}
