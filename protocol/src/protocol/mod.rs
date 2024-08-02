pub mod v1;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use std::pin::Pin;
use tokio::io::AsyncReadExt;
use v1::{parse_protocolv1_from_reader, PROPTOCOL_V1, V1};

use crate::message::Message;

pub const HEAD_LENGTH: usize = 2;

pub trait Builder {
    fn build(self) -> Protocol;
}

#[derive(Clone, Debug)]
pub struct Head([u8; HEAD_LENGTH]);

impl Default for Head {
    fn default() -> Self {
        Head([0, 1])
    }
}

impl Head {
    pub fn with(head: [u8; HEAD_LENGTH]) -> Self {
        Head(head)
    }

    pub fn get_version(&self) -> u8 {
        self.0[0]
    }

    pub fn set_version(&mut self, v: u8) {
        self.0[0] = v;
    }

    pub fn get_action(&self) -> u8 {
        self.0[1]
    }

    pub fn set_action(&mut self, action: u8) {
        self.0[1] = action;
    }
}

#[enum_dispatch]
pub trait ProtolOperation {
    fn get_version(&self) -> u8;
    fn get_action(&self) -> u8;
    fn convert_to_message(&self) -> Result<Message>;
}

#[derive(Debug, Clone)]
#[enum_dispatch(ProtolOperation)]
pub enum Protocol {
    V1(V1),
}

impl Protocol {
    pub fn as_bytes(&self) -> Vec<u8> {
        vec![]
    }
}

pub async fn parse_protocol_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
) -> Result<Protocol> {
    let mut buf = BytesMut::new();
    buf.resize(HEAD_LENGTH, 0);
    reader.read_exact(&mut buf).await?;
    let head = Head::with(buf.to_vec().try_into().expect("convert to head failed"));

    match head.get_version() {
        PROPTOCOL_V1 => Ok(Protocol::V1(
            parse_protocolv1_from_reader(reader, head.clone()).await?,
        )),
        _ => Err(anyhow!("unsupport protocol version")),
    }
}
