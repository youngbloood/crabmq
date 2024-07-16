use crate::error::*;
use crate::protocol::*;
use anyhow::Error;
use anyhow::Result;
use v1::ProtocolBodyV1;
use v1::ProtocolBodysV1;
use v1::ProtocolHeadV1;
// 一个标准的消息体
#[derive(Debug, Default)]
pub struct MessageV1 {
    remote_addr: String,
    pub head: ProtocolHeadV1,
    pub bodys: ProtocolBodysV1,
}

impl MessageV1 {
    pub fn with(mut head: ProtocolHeadV1, bodys: ProtocolBodysV1) -> Self {
        let _ = head.set_msg_num(bodys.len() as u8);
        MessageV1 {
            head,
            bodys,
            remote_addr: "".to_string(),
        }
    }

    pub fn with_one(mut head: ProtocolHeadV1, body: ProtocolBodyV1) -> Self {
        let _ = head.set_msg_num(1);
        let mut bodys = ProtocolBodysV1::new();
        bodys.push(body);
        MessageV1 {
            head,
            bodys,
            remote_addr: "".to_string(),
        }
    }

    pub fn set_remote_addr(&mut self, remote_add: &str) {
        self.remote_addr = remote_add.to_string();
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        let mut msg = Self::default();
        msg.head = self.head.clone();
        msg.bodys = self.bodys.clone();

        msg
    }

    pub fn get_topic(&self) -> &str {
        self.head.topic()
    }

    pub fn get_channel(&self) -> &str {
        self.head.channel()
    }

    pub fn action(&self) -> u8 {
        self.head.action()
    }

    pub fn init(&mut self) -> Result<()> {
        self.head.init()?;
        self.bodys.init()?;
        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.head.as_bytes());
        result.extend(self.bodys.as_bytes());

        result
    }

    /// 该message是否合法
    pub fn validate(&self, max_msg_num: u64, max_msg_len: u64) -> Result<()> {
        self.head.validate()?;
        if self.head.msg_num() as usize != self.bodys.len() {
            return Err(Error::from(ProtError::new(ERR_MSG_NUM_NOT_EQUAL)));
        }
        self.bodys.validate(max_msg_num, max_msg_len)?;
        Ok(())
    }

    pub fn split(&self) -> Vec<MessageV1Unit> {
        let mut list = vec![];
        let iter = self.bodys.list.iter();
        for body in iter {
            let mut head = self.head.clone();
            let _ = head.set_msg_num(1);
            list.push(MessageV1Unit::with(head, body.clone()));
        }
        list
    }
}

/**
 * [`MessageV1Unit`]，由[`MessageV1`]切割而来
 * 持久化到磁盘时使用该对象
 */
#[derive(Debug)]
pub struct MessageV1Unit {
    pub head: ProtocolHeadV1,
    pub body: ProtocolBodyV1,
}

impl MessageV1Unit {
    pub fn with(head: ProtocolHeadV1, body: ProtocolBodyV1) -> Self {
        MessageV1Unit { head, body }
    }
}

impl Eq for MessageV1Unit {}

impl Ord for MessageV1Unit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.body.defer_time().cmp(&other.body.defer_time())
    }
}

impl PartialEq for MessageV1Unit {
    fn eq(&self, other: &Self) -> bool {
        self.body.defer_time() == other.body.defer_time()
    }
}

impl PartialOrd for MessageV1Unit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
