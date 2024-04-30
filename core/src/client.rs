use std::sync::Arc;

use crate::conn::Conn;
use crate::tsuixuq::Tsuixuq;
use crate::{channel::Channel, message::Message};
use anyhow::Result;
use common::global::CANCEL_TOKEN;
use common::ArcMuxRefCell;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

// 表示一个链接的客户端
pub struct Client {
    conn: Conn,
    channel: Option<Arc<Channel>>,
    daemon: ArcMuxRefCell<Tsuixuq>,
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("drop the client");
    }
}

impl Client {
    pub fn new(socket: TcpStream, daemon: ArcMuxRefCell<Tsuixuq>) -> Self {
        debug!("new client");
        Client {
            conn: Conn::new(socket),
            channel: None,
            daemon,
        }
    }

    pub async fn io_loop(&mut self, sender: Sender<Message>) {
        debug!("start client io_loop");
        loop {
            select! {
                result = self.conn.read_parse() =>{
                    match result{
                        Ok(params)=>{
                            debug!("params={params:?}");
                            match sender.send(params).await{
                                Ok(())=>{
                                    continue
                                }
                                Err(_) =>return
                            }
                        }
                        Err(err)=>{
                            return ;
                        }
                    }
                }

                // resp_msg = async {
                //     if self.channel.is_some(){
                //         return Some(self.channel.as_ref().unwrap());
                //     }
                //     return None
                // } => {
                //     if let Some(msg) = resp_msg{
                //         self.conn.writer.write(&msg.as_bytes());
                //     }
                // }

                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }
            }
        }
    }
}
