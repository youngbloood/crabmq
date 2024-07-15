pub mod v1;

use self::v1::MessageV1;
use crate::{
    error::ProtocolError,
    protocol::{ProtocolBody, ProtocolBodys, ProtocolHead},
};
use anyhow::Result;
use bytes::Bytes;
use std::{pin::Pin, result::Result as StdResult};
use tokio::io::{duplex, AsyncWriteExt, BufReader};

#[derive(Debug)]
pub enum Message {
    Null,
    V1(MessageV1),
}
impl Message {
    pub fn new() -> Self {
        Message::Null
    }

    pub async fn parse_from(bts: &[u8]) -> Result<Self> {
        let mut reader = BufReader::new(bts);
        let mut preader = Pin::new(&mut reader);

        let head = ProtocolHead::parse_from(&mut preader).await?;

        let mut bodys = ProtocolBodys::new();
        while let Ok(body) = ProtocolBody::parse_from(&mut preader).await {
            bodys.push(body);
        }

        Ok(Self::with(head, bodys))
    }

    pub fn init(&mut self) -> Result<()> {
        match self {
            Message::Null => unreachable!(),
            Message::V1(ref mut v1) => v1.init(),
        }
    }

    pub fn reset_body(&mut self) -> Result<()> {
        match self {
            Message::Null => unreachable!(),
            Message::V1(ref mut v1) => {
                let iter = v1.bodys.list.iter_mut();
                for body in iter {
                    let _ = body.with_body(Bytes::new());
                }
                Ok(())
            }
        }
    }

    pub fn with(head: ProtocolHead, bodys: ProtocolBodys) -> Self {
        match head.version() {
            1 => Message::V1(MessageV1::with(head, bodys)),
            _ => Message::Null,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        match self {
            Self::V1(v1) => Message::V1(v1.clone()),
            _ => unreachable!(),
        }
    }

    pub fn get_topic(&self) -> &str {
        match self {
            Self::V1(v1) => v1.get_topic(),
            _ => unreachable!(),
        }
    }

    pub fn topic_ephemeral(&self) -> bool {
        match self {
            Self::V1(v1) => v1.head.topic_ephemeral(),
            _ => unreachable!(),
        }
    }

    pub fn get_channel(&self) -> &str {
        match self {
            Self::V1(v1) => v1.get_channel(),
            _ => unreachable!(),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::V1(v1) => v1.as_bytes(),
            _ => unreachable!(),
        }
    }

    pub fn action(&self) -> u8 {
        match self {
            Self::V1(v1) => v1.action(),
            _ => unreachable!(),
        }
    }

    pub fn validate(&self, max_msg_num: u8, max_msg_len: u64) -> StdResult<(), ProtocolError> {
        match self {
            Self::V1(v1) => Ok(v1.validate(max_msg_num, max_msg_len)?),
            _ => unreachable!(),
        }
    }

    pub fn calc_len(&self) -> usize {
        match self {
            Self::V1(v1) => {
                let mut size = v1.head.calc_len();
                v1.bodys.list.iter().for_each(|body| {
                    size += body.calc_len();
                });

                size
            }
            _ => unreachable!(),
        }
    }

    pub fn set_resp(&mut self) -> Result<()> {
        self.reset_body()?;
        match self {
            Self::V1(v1) => {
                v1.head.set_flag_resq(true);
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    /// split head-bodys into head-body, head-body, head-body...
    pub fn split(&self) -> Vec<Message> {
        match self {
            Self::V1(v1) => {
                let mut list = vec![];
                v1.bodys.list.iter().for_each(|pb| {
                    let head = v1.head.clone();
                    let body = pb.clone();
                    let msg = MessageV1::with_one(head, body);
                    list.push(Message::V1(msg));
                });
                list
            }
            _ => unreachable!(),
        }
    }

    //====================== just for head-body message ========================
    pub fn with_one(head: ProtocolHead, body: ProtocolBody) -> Self {
        match head.version() {
            1 => Message::V1(MessageV1::with_one(head, body)),
            _ => Message::Null,
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().id.as_str(),
            _ => unreachable!(),
        }
    }

    pub fn defer_time(&self) -> u64 {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().defer_time(),
            _ => unreachable!(),
        }
    }

    pub fn is_defer(&self) -> bool {
        match self {
            Self::V1(v1) => {
                if v1.bodys.list.is_empty() {
                    return false;
                }
                return v1.bodys.list.first().unwrap().is_defer();
            }
            _ => unreachable!(),
        }
    }

    pub fn is_deleted(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_delete(),
            _ => unreachable!(),
        }
    }

    pub fn is_consumed(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_consume(),
            _ => unreachable!(),
        }
    }

    pub fn is_not_ready(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_notready(),
            _ => unreachable!(),
        }
    }
    //====================== just for head-body message ========================
}

pub fn convert_to_resp(msg: Message) -> Message {
    let mut resp_msg = msg.clone();
    match &mut resp_msg {
        Message::Null => unreachable!(),
        Message::V1(ref mut v1) => v1.head.set_flag_resq(true),
    };
    resp_msg
}
