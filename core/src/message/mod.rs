use self::v1::{MessageV1, MessageV1Unit};
use crate::{
    error::ProtocolError,
    protocol::{ProtocolBody, ProtocolBodys, ProtocolHead},
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::{collections::BinaryHeap, pin::Pin, result::Result as StdResult};
use tokio::{
    fs::{write, File},
    io::AsyncReadExt,
};
pub mod sub;
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

    pub fn split(&self) -> Vec<MessageUnit> {
        match self {
            Message::Null => todo!(),
            Message::V1(v1) => {
                let mut list = vec![];
                let mut v1list = v1.split();
                while let Some(v1unit) = v1list.pop() {
                    list.push(MessageUnit::V1(v1unit));
                }
                list
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum MessageUnit {
    Null,
    V1(MessageV1Unit),
}

impl MessageUnit {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Self::Null => unreachable!(),
            Self::V1(v1) => {
                let mut bts = vec![];
                bts.extend_from_slice(&v1.head.as_bytes());
                bts.extend_from_slice(&v1.body.as_bytes());
                bts
            }
        }
    }

    pub fn with(head: ProtocolHead, body: ProtocolBody) -> Self {
        match head.version() {
            1 => return MessageUnit::V1(MessageV1Unit::with(head, body)),
            _ => return MessageUnit::Null,
        }
    }

    pub async fn parse_from_file<T: AsyncReadExt>(fd: &mut Pin<&mut T>) -> Result<Self> {
        let head = ProtocolHead::parse_from(fd).await?;
        let body = ProtocolBody::parse_from(fd).await?;
        Ok(Self::with(head, body))
    }
}

// impl Eq for MessageUnit {}

// impl Ord for MessageUnit {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.body.defer_time().cmp(&other.body.defer_time())
//     }
// }

// impl PartialEq for MessageUnit {
//     fn eq(&self, other: &Self) -> bool {
//         self.body.defer_time() == other.body.defer_time()
//     }
// }

// impl PartialOrd for MessageUnit {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         todo!()
//     }

//     fn lt(&self, other: &Self) -> bool {
//         matches!(self.partial_cmp(other), Some(Less))
//     }

//     fn le(&self, other: &Self) -> bool {
//         matches!(self.partial_cmp(other), Some(Less | Equal))
//     }

//     fn gt(&self, other: &Self) -> bool {
//         matches!(self.partial_cmp(other), Some(Greater))
//     }

//     fn ge(&self, other: &Self) -> bool {
//         matches!(self.partial_cmp(other), Some(Greater | Equal))
//     }
// }

/**
 * [`MessageUnitHeap`] 延时消息的小根堆
 * [`start_offset`] 标识该文件从哪个位置开始读，为了提高性能，文件仅允许append，不允许删除
 */
#[derive(Debug)]
pub struct MessageUnitHeap {
    filename: String,
    start_offset: u64, // 起始位置的offset
    write_offset: u64, // 写入的起始位置，由于可能存在push后，小根堆位置发生变化，所以记录该位置
    inner: BinaryHeap<MessageUnit>,
}

impl MessageUnitHeap {
    pub fn new(filename: &str) -> Self {
        MessageUnitHeap {
            filename: filename.to_string(),
            inner: BinaryHeap::new(),
            start_offset: 0,
            write_offset: 0,
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        let mut fd = File::open(self.filename.as_str()).await?;
        let mut pfd = Pin::new(&mut fd);
        loop {
            match MessageUnit::parse_from_file(&mut pfd).await {
                Ok(mu) => match mu {
                    MessageUnit::Null => todo!(),
                    MessageUnit::V1(mu) => {
                        self.inner.push(MessageUnit::V1(mu));
                    }
                },
                Err(e) => {
                    if e.to_string().contains("eof") {
                        break;
                    }
                    return Err(anyhow!(e));
                }
            }
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        let mut bts = vec![];
        let mut iter = self.inner.iter();
        while let Some(mu) = iter.next() {
            bts.append(&mut mu.as_bytes());
        }

        write(self.filename.as_str(), bts).await?;
        Ok(())
    }

    pub fn peek(&self) -> Option<&MessageUnit> {
        self.inner.peek()
    }

    pub fn pop(&mut self) -> Option<MessageUnit> {
        self.inner.pop()
    }

    pub fn push(&mut self, msg: MessageUnit) {
        self.inner.push(msg);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_message_unit_heap_persist() {
        let mut muh = MessageUnitHeap::new("./message_unit_persist");
        assert_eq!(muh.peek().is_none(), true);

        for i in 0..10 {
            let mut head = ProtocolHead::new();
            // 设置版本
            assert_eq!(head.set_version(1).is_ok(), true);
            // 设置msg_num
            assert_eq!(head.set_msg_num(1).is_ok(), true);
            assert_eq!(head.set_topic(format!("topic{i}").as_str()).is_ok(), true);
            assert_eq!(
                head.set_channel(format!("channel{i}").as_str()).is_ok(),
                true
            );
            assert_eq!(head.set_token(format!("token{i}").as_str()).is_ok(), true);

            head.set_channel_ephemeral(true)
                .set_topic_ephemeral(true)
                .set_heartbeat(true);
            head.set_reject_code(101);

            let mut body = ProtocolBody::new();
            // 设置id
            assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
            body.with_ack(true)
                .with_defer_time(1000)
                .with_not_ready(false)
                .with_persist(true);
            assert_eq!(
                body.with_body(Bytes::from_iter(format!("2000{i}").bytes()))
                    .is_ok(),
                true
            );
            muh.push(MessageUnit::with(head, body));
        }

        match muh.peek().unwrap() {
            MessageUnit::Null => todo!(),
            MessageUnit::V1(v1) => {
                println!("defer_time = {}", v1.body.defer_time());
            }
        }

        assert_eq!(muh.persist().await.is_ok(), true);
    }

    #[tokio::test]
    async fn test_message_unit_heap_load() {
        let mut muh = MessageUnitHeap::new("./message_unit_persist");
        let r = muh.load().await;
        assert_eq!(r.is_ok(), true);
        println!("muh = {muh:?}");
    }
}
