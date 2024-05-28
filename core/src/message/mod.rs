pub mod compress;
pub mod v1;

use self::v1::MessageV1;
use crate::{
    error::ProtocolError,
    protocol::{ProtocolBody, ProtocolBodys, ProtocolHead},
};
use anyhow::Result;
use bytes::Bytes;
use std::result::Result as StdResult;

#[derive(Debug)]
pub enum Message {
    Null,
    V1(MessageV1),
}
impl Message {
    pub fn new() -> Self {
        Message::Null
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
                let mut iter = v1.bodys.list.iter_mut();
                while let Some(body) = iter.next() {
                    let _ = body.with_body(Bytes::new());
                }
                Ok(())
            }
        }
    }

    pub fn with(head: ProtocolHead, bodys: ProtocolBodys) -> Self {
        match head.version() {
            1 => return Message::V1(MessageV1::with(head, bodys)),
            _ => return Message::Null,
        }
    }

    pub fn with_one(head: ProtocolHead, body: ProtocolBody) -> Self {
        match head.version() {
            1 => return Message::V1(MessageV1::with_one(head, body)),
            _ => return Message::Null,
        }
    }

    pub fn clone(&self) -> Self {
        match self {
            Self::V1(v1) => return Message::V1(v1.clone()),
            _ => unreachable!(),
        }
    }

    pub fn get_topic(&self) -> &str {
        match self {
            Self::V1(v1) => return v1.get_topic(),
            _ => unreachable!(),
        }
    }

    pub fn topic_ephemeral(&self) -> bool {
        match self {
            Self::V1(v1) => return v1.head.topic_ephemeral(),
            _ => unreachable!(),
        }
    }

    pub fn is_defer(&self) -> bool {
        match self {
            Self::V1(v1) => {
                if v1.bodys.list.len() == 0 {
                    return false;
                }
                return v1.bodys.list.get(0).unwrap().is_defer();
            }
            _ => unreachable!(),
        }
    }

    pub fn get_channel(&self) -> &str {
        match self {
            Self::V1(v1) => return v1.get_channel(),
            _ => unreachable!(),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::V1(v1) => return v1.as_bytes(),
            _ => unreachable!(),
        }
    }

    pub fn action(&self) -> u8 {
        match self {
            Self::V1(v1) => return v1.action(),
            _ => unreachable!(),
        }
    }

    pub fn validate(&self, max_msg_num: u8, max_msg_len: u64) -> StdResult<(), ProtocolError> {
        match self {
            Self::V1(v1) => return Ok(v1.validate(max_msg_num, max_msg_len)?),
            _ => unreachable!(),
        }
    }

    pub fn calc_len(&self) -> usize {
        match self {
            Self::V1(v1) => {
                let mut size = v1.head.calc_len();
                let head_size = size;
                let mut body_size = 0;
                v1.bodys.list.iter().for_each(|body| {
                    size += body.calc_len();
                    body_size += body.calc_len();
                });

                println!(
                    "all-size={}, head-size={}, body-size={}",
                    head_size + body_size,
                    head_size,
                    body_size
                );
                size
            }
            _ => unreachable!(),
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::V1(v1) => v1.bodys.list.get(0).unwrap().id.as_str(),
            _ => unreachable!(),
        }
    }

    pub fn defer_time(&self) -> u64 {
        match self {
            Self::V1(v1) => v1.bodys.list.get(0).unwrap().defer_time(),
            _ => unreachable!(),
        }
    }

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

    pub fn set_resp(&mut self) -> Result<()> {
        self.reset_body()?;
        match self {
            Self::V1(v1) => {
                v1.head.set_flag_resq(true);
                return Ok(());
            }
            _ => unreachable!(),
        }
    }
}
