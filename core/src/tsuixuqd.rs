use crate::{
    message::Message,
    tcp::TcpServer,
    tsuixuq::{Tsuixuq, TsuixuqOption},
};
use anyhow::{anyhow, Result};
use common::{global, ArcMuxRefCell};
use std::{cell::RefCell, sync::Arc};
use tokio::{
    net::TcpListener,
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
};
use tracing::info;

pub struct Tsuixuqd {
    opt: Arc<TsuixuqOption>,
    tsuixuq: ArcMuxRefCell<Tsuixuq>,
}

impl Tsuixuqd {
    pub fn new(mut opt: TsuixuqOption) -> Self {
        let opt_cell = Arc::new(opt);
        Tsuixuqd {
            opt: opt_cell.clone(),
            tsuixuq: Arc::new(Mutex::new(RefCell::new(Tsuixuq::new(opt_cell.clone())))),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn serve(&mut self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(10000);

        select! {
            // tcp server
            r1 =  tokio::spawn(Self::serve_tcp(
                self.opt.clone(),
                self.tsuixuq.clone(),
                tx.clone(),
                rx,
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

            _=global::CANCEL_TOKEN.cancelled()=>{
                return Err(anyhow!("process stopped"))
            }
        }
        Ok(())
    }

    async fn serve_tcp(
        opt: Arc<TsuixuqOption>,
        tsuixuq: ArcMuxRefCell<Tsuixuq>,
        tx: Sender<Message>,
        mut rx: Receiver<Message>,
    ) -> Result<()> {
        // start tcp serve
        let tcp_tx = tx.clone();
        let tcp_port = opt.tcp_port;
        let tsuixuq_clone: Arc<Mutex<RefCell<Tsuixuq>>> = tsuixuq.clone();
        let opt_arc = opt.clone();

        match tokio::spawn(async move {
            info!("start listen port: {}", tcp_port);
            match TcpListener::bind(format!("127.0.0.1:{}", tcp_port)).await {
                Err(err) => {
                    return Err(anyhow!("listen port failed: {err}"));
                }
                Ok(tcp_listener) => {
                    // 将处理tcp_listener单独放到一个Future中处理
                    tokio::spawn(async {
                        let mut tcp_server = TcpServer::new(opt_arc, tcp_listener);
                        tcp_server.serve(tcp_tx, tsuixuq_clone).await;
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

        info!("start recieve params from socket");
        while let Some(msg) = rx.recv().await {
            let mut tsuixuq = tsuixuq.lock().await;
            tsuixuq.get_mut().send_message(msg).await?;
        }
        Ok(())
    }
}
