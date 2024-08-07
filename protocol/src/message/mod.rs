pub mod v1;
use crate::consts::PROPTOCOL_V1;
use crate::protocol::{v1::touch::Touch as TouchV1, Head, Protocol, HEAD_LENGTH};
use anyhow::Result;
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, BufReader};
use v1::MessageV1;

#[enum_dispatch]
pub trait MessageOperation {
    fn as_bytes(&self) -> Vec<u8>;
    fn convert_to_protocol(self) -> Protocol;
    fn get_topic(&self) -> &str;
    fn get_channel(&self) -> &str;
    fn get_id(&self) -> &str;
    fn defer_time(&self) -> u64;
    fn is_notready(&self) -> bool;
    fn is_ack(&self) -> bool;
    fn is_persist(&self) -> bool;
    fn is_deleted(&self) -> bool;
    fn is_consumed(&self) -> bool;
}

#[derive(Clone, Debug)]
#[enum_dispatch(MessageOperation)]
pub enum Message {
    V1(v1::MessageV1),
}
impl Default for Message {
    fn default() -> Self {
        Message::V1(v1::MessageV1::default())
    }
}

impl Message {
    pub async fn parse_from_reader(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut buf = BytesMut::new();
        buf.resize(HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        let head = Head::with(buf.to_vec().try_into().expect("convert to head failed"));
        match head.get_version() {
            PROPTOCOL_V1 => Ok(Self::V1(
                MessageV1::parse_from_reader(reader, head.clone()).await?,
            )),

            _ => unreachable!(),
        }
    }

    pub async fn parse_from_vec(bts: &[u8]) -> Result<Self> {
        let mut buf = BufReader::new(bts);
        let mut reader = Pin::new(&mut buf);
        Self::parse_from_reader(&mut reader).await
    }
}

pub enum TopicCustom {
    V1(TouchV1),
}
