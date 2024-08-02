pub mod v1;

use self::v1::MessageV1;
use crate::error::ProtError;
use crate::protocol::{ProtocolBody, ProtocolBodys, ProtocolHead};
use crate::v1::ProtocolBodysV1;
use crate::{parse_body_from_reader, parse_head_from_reader};
use anyhow::Result;
use bytes::Bytes;
use std::{pin::Pin, result::Result as StdResult};
use tokio::io::BufReader;

#[derive(Debug, Clone)]
pub enum Message {
    V1(MessageV1),
}

impl Message {
    pub async fn parse_from(bts: &[u8]) -> Result<Self> {
        let mut reader = BufReader::new(bts);
        let mut preader = Pin::new(&mut reader);

        let head = parse_head_from_reader(&mut preader).await?;
        let bodys = parse_body_from_reader(&mut preader, &head).await?;
        Self::with(head, bodys)
    }

    pub fn init(&mut self) -> Result<()> {
        match self {
            Message::V1(ref mut v1) => v1.init(),
        }
    }

    pub fn reset_body(&mut self) -> Result<()> {
        match self {
            Message::V1(ref mut v1) => {
                let iter = v1.bodys.list.iter_mut();
                for body in iter {
                    let _ = body.with_body(Bytes::new());
                }
                Ok(())
            }
        }
    }

    pub fn with(head: ProtocolHead, bodys: ProtocolBodys) -> Result<Self> {
        match head {
            ProtocolHead::V1(head_v1) => match bodys {
                ProtocolBodys::V1(bodys_v1) => Ok(Message::V1(MessageV1::with(head_v1, bodys_v1))),
            },
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        match self {
            Self::V1(v1) => Message::V1(v1.clone()),
        }
    }

    pub fn set_reject_code(&mut self, reject_code: u8) {
        match self {
            Message::V1(v1) => v1.head.set_reject_code(reject_code),
        };
    }

    pub fn get_topic(&self) -> &str {
        match self {
            Self::V1(v1) => v1.get_topic(),
        }
    }

    pub fn prohibit_instant(&self) -> bool {
        match self {
            Self::V1(v1) => v1.head.prohibit_instant(),
        }
    }

    pub fn prohibit_defer(&self) -> bool {
        match self {
            Self::V1(v1) => v1.head.prohibit_defer(),
        }
    }

    pub fn defer_message_format(&self) -> &str {
        match self {
            Self::V1(v1) => v1.head.get_defer_msg_format(),
        }
    }

    pub fn topic_ephemeral(&self) -> bool {
        match self {
            Self::V1(v1) => v1.head.topic_ephemeral(),
        }
    }

    pub fn get_channel(&self) -> &str {
        match self {
            Self::V1(v1) => v1.get_channel(),
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::V1(v1) => v1.as_bytes(),
        }
    }

    pub fn action(&self) -> u8 {
        match self {
            Self::V1(v1) => v1.action(),
        }
    }

    pub fn validate(&self, max_msg_num: u64, max_msg_len: u64) -> StdResult<(), ProtError> {
        match self {
            Self::V1(v1) => Ok(v1.validate(max_msg_num, max_msg_len)?),
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
        }
    }

    pub fn set_resp(&mut self) -> Result<()> {
        self.reset_body()?;
        match self {
            Self::V1(v1) => {
                v1.head.set_flag_resq(true);
                Ok(())
            }
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
        }
    }

    //====================== just for head-body message ========================
    pub fn with_one(head: ProtocolHead, body: ProtocolBody) -> Self {
        match head {
            ProtocolHead::V1(head_v1) => match body {
                ProtocolBody::V1(body_v1) => {
                    let mut bodys_v1 = ProtocolBodysV1::new();
                    bodys_v1.push(body_v1);
                    Message::V1(MessageV1::with(head_v1, bodys_v1))
                }
            },
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().id.as_str(),
        }
    }

    pub fn defer_time(&self) -> u64 {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().defer_time(),
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
        }
    }

    pub fn is_deleted(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_delete(),
        }
    }

    pub fn is_consumed(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_consume(),
        }
    }

    pub fn is_not_ready(&self) -> bool {
        match self {
            Self::V1(v1) => v1.bodys.list.first().unwrap().is_notready(),
        }
    }

    pub fn get_head(&self) -> ProtocolHead {
        match self {
            Self::V1(v1) => ProtocolHead::V1(v1.head.clone()),
        }
    }
    //====================== just for head-body message ========================
}

pub fn convert_to_resp(msg: Message) -> Message {
    let mut resp_msg = msg.clone();
    match &mut resp_msg {
        Message::V1(ref mut v1) => v1.head.set_flag_resq(true),
    };
    resp_msg
}
