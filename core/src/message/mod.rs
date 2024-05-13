use self::v1::MessageV1;
use crate::{
    error::ProtocolError,
    protocol::{ProtocolBodys, ProtocolHead},
};
use std::result::Result as StdResult;
pub mod v1;

#[derive(Debug)]
pub enum Message {
    Null,
    V1(MessageV1),
}
impl Message {
    pub fn new() -> Self {
        Message::Null
    }

    pub fn with(head: ProtocolHead, body: ProtocolBodys) -> Self {
        match head.version() {
            1 => return Message::V1(MessageV1::with(head, body)),
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

    pub fn get_channel(&self) -> &str {
        match self {
            Self::V1(v1) => return v1.get_channel(),
            _ => unreachable!(),
        }
    }

    pub fn post_fill(&mut self) {
        match self {
            Self::V1(v1) => return v1.post_fill(),
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
}
