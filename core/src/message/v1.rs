use crate::{error::*, protocol::*};
use std::result::Result as StdResult;

// 一个标准的消息体
#[derive(Debug)]
pub struct MessageV1 {
    remote_addr: String,
    pub head: ProtocolHead,
    bodys: ProtocolBodys,
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

    pub fn post_fill(&mut self) {
        self.head.post_fill();
        self.bodys.post_fill();
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
}
