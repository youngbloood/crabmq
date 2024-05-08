use crate::protocol::{ProtocolBodys, ProtocolHead};
use anyhow::{anyhow, Result};

use self::v1::MessageV1;
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
            _ => return Message::Null,
        }
    }

    pub fn get_topic(&self) -> &str {
        match self {
            Self::V1(v1) => return v1.get_topic(),
            _ => panic!("not get here"),
        }
    }

    pub fn post_fill(&mut self) {
        match self {
            Self::V1(v1) => return v1.post_fill(),
            _ => panic!("not get here"),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::V1(v1) => return v1.as_bytes(),
            _ => panic!("not get here"),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::V1(v1) => return v1.validate(),
            _ => return Err(anyhow!("not support message version")),
        }
    }
}

// pub fn convert_to_message(head: ProtocolHead, bodys: ProtocolBodys) -> Result<Message> {
//     match head.version() {
//         1 => return Ok(Message::V1(MessageV1::new())),
//         _ => return Err(anyhow!("not support protocol version")),
//     }
// }
