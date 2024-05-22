use crate::{error::*, protocol::*};
use anyhow::Result;
use std::result::Result as StdResult;
// 一个标准的消息体
#[derive(Debug)]
pub struct MessageV1 {
    remote_addr: String,
    pub head: ProtocolHead,
    pub bodys: ProtocolBodys,
}

impl MessageV1 {
    pub fn new() -> Self {
        MessageV1 {
            head: ProtocolHead::default(),
            bodys: ProtocolBodys::new(),
            remote_addr: "".to_string(),
        }
    }

    pub fn with(mut head: ProtocolHead, bodys: ProtocolBodys) -> Self {
        let _ = head.set_msg_num(bodys.len() as u8);
        MessageV1 {
            head,
            bodys,
            remote_addr: "".to_string(),
        }
    }

    pub fn with_one(mut head: ProtocolHead, body: ProtocolBody) -> Self {
        let _ = head.set_msg_num(1);
        let mut bodys = ProtocolBodys::new();
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

    pub fn clone(&self) -> Self {
        let mut msg = Self::new();
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
    pub fn validate(&self, max_msg_num: u8, max_msg_len: u64) -> StdResult<(), ProtocolError> {
        self.head.validate(max_msg_num)?;
        self.bodys.validate(max_msg_len)?;
        Ok(())
    }

    pub fn split(&self) -> Vec<MessageV1Unit> {
        let mut list = vec![];
        let mut iter = self.bodys.list.iter();
        while let Some(body) = iter.next() {
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
    pub head: ProtocolHead,
    pub body: ProtocolBody,
}

impl MessageV1Unit {
    pub fn with(head: ProtocolHead, body: ProtocolBody) -> Self {
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
