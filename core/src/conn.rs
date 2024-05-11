use anyhow::Result;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};
use tracing::debug;

use crate::{
    message::Message,
    protocol::{
        ProtocolBody, ProtocolBodys, ProtocolHead, PROTOCOL_BODY_HEAD_LEN, PROTOCOL_HEAD_LEN,
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

    pub async fn write(&mut self, body: &[u8]) -> Result<()> {
        write(&mut self.writer, body).await
    }
    // 循环
    pub async fn read_parse(&mut self) -> Result<Message> {
        read_parse(&mut self.reader).await
    }
}

pub async fn write(writer: &mut OwnedWriteHalf, body: &[u8]) -> Result<()> {
    writer.write_all(body).await?;
    Ok(())
}

// 循环
pub async fn read_parse(reader: &mut OwnedReadHalf) -> Result<Message> {
    let head: ProtocolHead = read_protocol_head(reader).await?;
    let body = read_protocol_body(reader, head.msg_num()).await?;
    Ok(Message::with(head, body))
}

/// 读取ProtocolHead
async fn read_protocol_head(reader: &mut OwnedReadHalf) -> Result<ProtocolHead> {
    let mut ph: ProtocolHead = ProtocolHead::new();
    let mut buf = BytesMut::new();

    // debug!(addr = "{self.addr:?}", "read protocol head");
    // parse head
    buf.resize(PROTOCOL_HEAD_LEN, 0);
    reader.read_exact(&mut buf).await?;
    ph.set_head(
        buf.to_vec()
            .try_into()
            .expect("convert BytesMut to array failed"),
    );

    debug!(addr = "{self.addr:?}", "parse topic name");
    // parse topic name
    buf.resize(ph.topic_len() as usize, 0);
    reader.read_exact(&mut buf).await?;
    let topic: String = String::from_utf8(buf.to_vec()).expect("illigal topic name");
    ph.set_topic(topic.as_str())?;

    debug!(addr = "{self.addr:?}", "parse channel name");
    // parse channel name
    buf.resize(ph.channel_len() as usize, 0);
    reader.read_exact(&mut buf).await?;
    let channel = String::from_utf8(buf.to_vec()).expect("illigal channel name");
    ph.set_channel(channel.as_str())?;

    debug!(addr = "{self.addr:?}", "parse token");
    // parse token
    buf.resize(ph.token_len() as usize, 0);
    reader.read_exact(&mut buf).await?;
    let token = String::from_utf8(buf.to_vec()).expect("illigal token value");
    ph.set_token(token.as_str())?;

    Ok(ph)
}

/// 读取ProtocolBody
async fn read_protocol_body(reader: &mut OwnedReadHalf, mut msg_num: u8) -> Result<ProtocolBodys> {
    let mut pbs = ProtocolBodys::new();
    while msg_num != 0 {
        msg_num -= 1;

        let mut pb = ProtocolBody::new();
        let mut buf = BytesMut::new();

        // parse protocol body head
        buf.resize(PROTOCOL_BODY_HEAD_LEN, 0);
        reader.read_exact(&mut buf).await?;
        pb.set_head(
            buf.to_vec()
                .try_into()
                .expect("convert to protocol body head failed"),
        );

        // parse defer time
        if pb.is_defer() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            let defer_time = u64::from_be_bytes(
                buf.to_vec()
                    .try_into()
                    .expect("convert to defer time failed"),
            );
            pb.set_defer_time(defer_time);
        }

        // parse id
        let id_len = pb.id_len();
        if id_len != 0 {
            buf.resize(id_len as usize, 0);
            reader.read_exact(&mut buf).await?;
            let id = String::from_utf8(buf.to_vec()).expect("illigal id value");
            pb.set_id(id.as_str())?;
        }
        // parse body
        let body_len = pb.body_len();
        if body_len != 0 {
            buf.resize(body_len as usize, 0);
            reader.read_exact(&mut buf).await?;
            let bts: Bytes = Bytes::copy_from_slice(buf.as_ref());
            pb.set_body(bts)?;
        }

        pbs.push(pb);
    }

    Ok(pbs)
}
