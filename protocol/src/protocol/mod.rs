pub mod v1;

use crate::consts::{ACTION_PATCH, PROPTOCOL_V1};
use crate::error::ProtError;
use crate::message::Message;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::io::AsyncReadExt;
use tracing::debug;
use v1::reply::{Reply, ReplyBuilder};
use v1::V1;

pub const HEAD_LENGTH: usize = 2;

pub trait Builder {
    fn build(self) -> Protocol;
}

#[derive(Clone)]
pub struct Head([u8; HEAD_LENGTH]);

impl Debug for Head {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Head")
            .field("version", &self.get_version())
            .field("action", &self.get_action())
            .finish()
    }
}

impl Default for Head {
    fn default() -> Self {
        Head([0, 1])
    }
}

impl Head {
    pub fn with(head: [u8; HEAD_LENGTH]) -> Self {
        Head(head)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn get_version(&self) -> u8 {
        self.0[0]
    }

    pub fn set_version(&mut self, v: u8) -> &mut Self {
        self.0[0] = v;
        self
    }

    pub fn get_action(&self) -> u8 {
        self.0[1]
    }

    pub fn set_action(&mut self, action: u8) -> &mut Self {
        self.0[1] = action;
        self
    }

    pub fn validate(&self) -> Result<()> {
        match self.get_version() {
            PROPTOCOL_V1 => Ok(()),
            _ => Err(anyhow!("not support protocol version")),
        }
    }
}

#[enum_dispatch]
pub trait ProtocolOperation {
    fn get_version(&self) -> u8;
    fn get_action(&self) -> u8;
    fn convert_to_message(&self) -> Result<Vec<Message>>;
    fn as_bytes(&self) -> Vec<u8>;

    fn validate_for_client(&self) -> Result<()>;
    fn validate_for_server(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
#[enum_dispatch(ProtocolOperation)]
pub enum Protocol {
    V1(V1),
}

impl ReplyBuilder for Protocol {
    fn build_reply_ok(&self) -> Reply {
        match self {
            Protocol::V1(v1) => v1.build_reply_ok(),
        }
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        match self {
            Protocol::V1(v1) => v1.build_reply_err(err_code),
        }
    }
}

impl Protocol {
    pub fn validate_for_server_with_resp(&self) -> Option<Protocol> {
        if let Err(e) = self.validate_for_server() {
            let err: ProtError = e.into();
            return match self {
                Protocol::V1(v1) => match v1.get_action() {
                    ACTION_PATCH => {
                        if let Some(patch) = v1.get_patch() {
                            if patch.is_nnr() {
                                debug!("No-Need-Reply");
                                return None;
                            }
                        }
                        Some(v1.build_reply_err(err.code).build())
                    }
                    _ => Some(v1.build_reply_err(err.code).build()),
                },
            };
        }

        None
    }
}

/// returns:
/// 0: the correct Protocol,
/// 1: the reply Protocol,
pub async fn parse_protocol_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
) -> Result<Protocol> {
    let mut buf = BytesMut::new();
    buf.resize(HEAD_LENGTH, 0);
    reader.read_exact(&mut buf).await?;
    let head = Head::with(buf.to_vec().try_into().expect("convert to head failed"));

    println!("head = {head:?}");
    match head.get_version() {
        PROPTOCOL_V1 => Ok(Protocol::V1(V1::parse_from(reader, head.clone()).await?)),
        _ => Err(anyhow!("unsupport protocol version")),
    }
}
