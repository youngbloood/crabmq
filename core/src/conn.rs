use anyhow::Result;
use common::util::execute_timeout;
use std::{net::SocketAddr, pin::Pin};
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

use crate::{
    message::Message,
    protocol::{ProtocolBodys, ProtocolHead},
};

// 定义一个链接，包含读写端
pub struct Conn {
    pub reader: OwnedReadHalf,
    pub writer: OwnedWriteHalf,
    pub addr: SocketAddr,
}

unsafe impl Send for Conn {}
unsafe impl Sync for Conn {}

impl Conn {
    pub fn new(conn: TcpStream) -> Self {
        let addr = conn.peer_addr().unwrap();
        let (reader, writer) = conn.into_split();
        Conn {
            addr,
            reader,
            writer,
        }
    }

    // 循环
    pub async fn read_parse(&mut self, timeout: u64) -> Result<Message> {
        let mut head = ProtocolHead::new();
        let mut reader = Pin::new(&mut self.reader);
        execute_timeout::<()>(head.read_parse(&mut reader), timeout).await?;
        let mut bodys = ProtocolBodys::new();
        execute_timeout::<()>(bodys.read_parse(&mut self.reader, head.msg_num()), timeout).await?;

        Ok(Message::with(head, bodys))
    }

    pub async fn write(&mut self, body: &[u8], timeout: u64) -> Result<()> {
        execute_timeout(
            async {
                self.writer.write_all(body).await?;
                Ok(())
            },
            timeout,
        )
        .await
    }
}
