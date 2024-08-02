use anyhow::Result;
use common::util::execute_timeout;
use protocol::{
    message::Message,
    protocol::{parse_protocol_from_reader, Protocol},
};
use std::{net::SocketAddr, pin::Pin};
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
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
    pub async fn read_parse(&mut self, timeout: u64) -> Result<Protocol> {
        let mut reader = Pin::new(&mut self.reader);
        let prot =
            execute_timeout::<Protocol>(parse_protocol_from_reader(&mut reader), timeout).await?;
        Ok(prot)
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
