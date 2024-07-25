use crate::{config::Config, crab::Crab, tcp::TcpServer};
use anyhow::{anyhow, Result};
use common::global::{self, Guard};
use tokio::{net::TcpListener, select};
use tracing::info;

pub struct CrabMQD {
    opt: Guard<Config>,
    crab: Guard<Crab>,
}

impl CrabMQD {
    pub fn new(opt: Guard<Config>) -> Result<Self> {
        Ok(CrabMQD {
            crab: Guard::new(Crab::new(opt.clone())?),
            opt,
        })
    }

    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn serve(&mut self) -> Result<()> {
        select! {
            // tcp server
            r1 =  tokio::spawn(Self::serve_tcp(
                self.opt.clone(),
                self.crab.clone(),
            )) => {
                match r1 {
                    Ok(v) => {
                        if let Err(e) = v {
                            return Err(anyhow!(e));
                        }
                    }
                    Err(e) => {
                        return Err(anyhow!(e));
                    }
                }
            }

            // TODO: http server
            _ = global::CANCEL_TOKEN.cancelled() => {
                return Err(anyhow!("process stopped"))
            }
        }
        Ok(())
    }

    async fn serve_tcp(opt: Guard<Config>, crab: Guard<Crab>) -> Result<()> {
        // start tcp serve
        let tcp_port = opt.get().global.tcp_port;
        let crab_clone: Guard<Crab> = crab.clone();
        let opt_arc = opt.clone();

        match tokio::spawn(binding(tcp_port, opt_arc, crab_clone)).await {
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

async fn binding(port: u32, opt: Guard<Config>, crab: Guard<Crab>) -> Result<()> {
    info!("start listen port: {}", port);
    match TcpListener::bind(format!("127.0.0.1:{}", port)).await {
        Err(err) => Err(anyhow!("listen port failed: {err}")),
        Ok(tcp_listener) => {
            // 将处理tcp_listener单独放到一个Future中处理
            tokio::spawn(async move {
                let mut tcp_server = TcpServer::new(opt, tcp_listener, crab);
                tcp_server.serve().await;
            });

            Ok(())
        }
    }
}
