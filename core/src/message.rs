use std::io::Read;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use common::global;
use rsbit::{BitFlagOperation, BitOperation};
use tracing::warn;

/// 固定的6字节协议头
pub const PROTOCOL_HEAD_LEN: usize = 10;
/// 固定的3字节协议体的头长度
pub const PROTOCOL_BODY_HEAD_LEN: usize = 10;

/// fin action
pub const ACTION_FIN: u8 = 1;

/// rdy action
pub const ACTION_RDY: u8 = 2;

/// req action
pub const ACTION_REQ: u8 = 3;

/// pub action
pub const ACTION_PUB: u8 = 4;

/// mpub action
pub const ACTION_MPUB: u8 = 5;

/// dpub action
pub const ACTION_DPUB: u8 = 6;

/// nop action
pub const ACTION_NOP: u8 = 7;

/// touch action
pub const ACTION_TOUCH: u8 = 8;

/// sub action
pub const ACTION_SUB: u8 = 9;

/// cls action
pub const ACTION_CLS: u8 = 10;

/// auth action
pub const ACTION_AUTH: u8 = 11;
/**
### FIXED HEAD LENGTH(at least 6 bytes):
* 1st byte: action[fin, rdy, req, pub , mpub, dpub, nop, touch, sub, cls, auth]
* 2nd byte: global flags:
*           1bit: req flag: 0 represent is a req, 1 represent is a resp
*           1bit: topic is ephemeral: true mean the topic is ephemeral, it will be delete then there is no publishers.
*           1bit: channel is ephemeral: true mean the channel is ephemeral, it will be delete then there is no subcribers.
*           1bit: is heartbeat.
*           1bit: reject connect. 1st flag must be resp, and the 8th byte mean the reject code.
*           left 5 bits: extend flags.
* 3rd byte: extend flags
* 4th byte:
*           4bit: version: protocol version.
*           4bit: message number.
* 5th byte: topic length.
* 6th byte: channel length.
* 7th byte: token length.
* 8th byte: reject code.
* 9th bytes: extend bytes.
* optional: topic value.
* optional: channel value.
* optional: token value.
*/
#[derive(Debug)]
pub struct ProtocolHead {
    head: [u8; PROTOCOL_HEAD_LEN],
    topic: String,
    channel: String,
    token: String,
    reject_code: u8,
}

impl Default for ProtocolHead {
    fn default() -> Self {
        Self {
            head: Default::default(),
            topic: Default::default(),
            channel: Default::default(),
            token: Default::default(),
            reject_code: 0,
        }
    }
}

impl ProtocolHead {
    pub fn new() -> Self {
        ProtocolHead {
            head: [0_u8; PROTOCOL_HEAD_LEN],
            topic: String::new(),
            channel: String::new(),
            token: String::new(),
            reject_code: 0,
        }
    }

    pub fn clone(&self) -> Self {
        let mut ph = Self::new();
        ph.set_head(self.head);
        let _ = ph.set_topic(&self.topic.as_str());
        let _ = ph.set_channel(&self.channel.as_str());
        let _ = ph.set_token(&self.token.as_str());

        ph
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.head);
        result.extend(self.topic.as_bytes());
        result.extend(self.channel.as_bytes());
        result.extend(self.token.as_bytes());

        result
    }

    /// fill the null value after the parse
    pub fn post_fill(&mut self) {
        match self.head[0] as u8 {
            ACTION_SUB | ACTION_PUB | ACTION_MPUB => {
                if self.topic_len() == 0 {
                    let _ = self.set_topic("default");
                }
                if self.channel_len() == 0 {
                    let _ = self.set_channel("default");
                }
            }

            _ => {
                warn!("illigal action");
            }
        }
    }

    pub fn set_head(&mut self, head: [u8; PROTOCOL_HEAD_LEN]) -> &mut Self {
        self.head = head;
        self
    }

    pub fn set_action(&mut self, action: u8) -> &mut Self {
        self.head[0] = action;
        self
    }

    pub fn is_req(&self) -> bool {
        self.head[1].is_0(7)
    }

    pub fn topic_ephemeral(&self) -> bool {
        self.head[1].is_1(6)
    }

    pub fn channel_ephemeral(&self) -> bool {
        self.head[1].is_1(5)
    }

    pub fn heartbeat(&self) -> bool {
        self.head[1].is_1(4)
    }

    pub fn reject(&self) -> bool {
        self.head[1].is_1(4)
    }

    pub fn topic_len(&self) -> u8 {
        self.head[4] as u8
    }

    pub fn channel_len(&self) -> u8 {
        self.head[5] as u8
    }

    pub fn token_len(&self) -> u8 {
        self.head[6]
    }

    pub fn version(&self) -> u8 {
        self.head[3] & 0b11110000
    }

    pub fn set_version(&mut self, v: u8) -> Result<()> {
        if v > 0b00001111 {
            return Err(anyhow!("illigal protocol version number"));
        }
        self.head[3] = self.head[3] | v << 4;

        Ok(())
    }

    pub fn msg_num(&self) -> u8 {
        self.head[3] & 0b00001111
    }

    pub fn set_msg_num(&mut self, num: u8) -> Result<()> {
        if num > 0b00001111 {
            return Err(anyhow!("illigal protocol version number"));
        }
        self.head[3] = self.head[3] | num;

        Ok(())
    }

    pub fn set_topic_ephemeral(&mut self, ephemeral: bool) -> &mut Self {
        let mut flag = self.head[1];
        if ephemeral {
            (&mut flag).set_1(6);
        } else {
            (&mut flag).set_0(6);
        }
        self.head[1] = flag;
        self
    }

    pub fn set_channel_ephemeral(&mut self, ephemeral: bool) -> &mut Self {
        let mut flag = self.head[1];
        if ephemeral {
            (&mut flag).set_1(5);
        } else {
            (&mut flag).set_0(5);
        }
        self.head[1] = flag;
        self
    }

    pub fn set_topic(&mut self, topic: &str) -> Result<()> {
        if topic.len() > u8::MAX as usize {
            return Err(anyhow!("topic len exceed max length 256"));
        }
        self.topic = topic.to_string();
        self.head[4] = topic.len() as u8;
        Ok(())
    }

    pub fn set_channel(&mut self, channel: &str) -> Result<()> {
        if channel.len() > u8::MAX as usize {
            return Err(anyhow!("channel len exceed max length 256"));
        }
        self.channel = channel.to_string();
        self.head[5] = channel.len() as u8;
        Ok(())
    }

    pub fn set_token(&mut self, token: &str) -> Result<()> {
        if token.len() > u8::MAX as usize {
            return Err(anyhow!("token len exceed max length 256"));
        }
        self.token = token.to_string();
        self.head[6] = token.len() as u8;
        Ok(())
    }
}

/**
* ProtocolBodys:
*/
#[derive(Debug)]
pub struct ProtocolBodys {
    sid: i64, // 标识一批次的message
    list: Vec<ProtocolBody>,
}

impl ProtocolBodys {
    pub fn new() -> Self {
        ProtocolBodys {
            list: Vec::new(),
            sid: global::SNOWFLAKE.get_id(),
        }
    }

    pub fn clone(&self) -> Self {
        let mut pbs = Self::new();
        pbs.sid = self.sid;
        self.list.iter().for_each(|pb| {
            pbs.push(pb.clone());
        });

        pbs
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        self.list.iter().for_each(|pb| result.extend(pb.as_bytes()));

        result
    }

    pub fn push(&mut self, pb: ProtocolBody) -> &mut Self {
        self.list.push(pb);
        self
    }

    pub fn pop(&mut self) -> Option<ProtocolBody> {
        self.list.pop()
    }

    /// fill the null value after the parse
    pub fn post_fill(&mut self) {
        self.list.iter_mut().for_each(|pb| {
            pb.set_sid(self.sid);
            pb.post_fill();
        })
    }
}
/**
### Every body has the same structure:
* 1st byte: flag:
*           1bit: is defer: true is defer message.
*           1bit: is a ack message: true represent this message is a ack.
*           1bit: is persist immediately: if true, the message will persist to disk right now.
*           1bit: is delete: true mean the message will be deleted before consumed.(must with MSG_ID)（优先级高于is ready）
*           1bit: is ready: true mean the message can be consumed.(must with MSG_ID)
*           left 3bits: extend
* 2nd byte: ID-LENGTH
* 3-10th bytes: BODY-LENGTH(8 bytes)
* optional:
*           8byte defer time.
*           id value(length determine by ID-LENGTH)
*           body value(length determine by BODY-LENGTH)
*/
#[derive(Debug)]
pub struct ProtocolBody {
    head: [u8; PROTOCOL_BODY_HEAD_LEN],
    sid: i64,        // session_id: generate by ProtocolBodys
    defer_time: u64, // 8 bytes
    id: String,
    body: Bytes,
}

impl ProtocolBody {
    pub fn new() -> Self {
        ProtocolBody {
            head: [0_u8; PROTOCOL_BODY_HEAD_LEN],
            sid: 0,
            defer_time: 0,
            id: String::new(),
            body: Bytes::new(),
        }
    }

    pub fn clone(&self) -> Self {
        let mut pb = Self::new();
        pb.set_head(self.head);
        let _ = pb.set_sid(self.sid);
        let _ = pb.set_defer_time(self.defer_time);
        let _ = pb.set_id(&self.id.as_str());
        let _ = pb.set_body(self.body.clone());

        pb
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(&self.head);
        result.extend(self.defer_time.to_be_bytes());
        result.extend(self.id.as_bytes());
        result.extend(self.body.as_ref().into_iter());

        result
    }

    /// fill the null value after the parse
    pub fn post_fill(&mut self) {
        if self.id.len() != 0 && self.id_len() != 0 {
            return;
        }
        let id = global::SNOWFLAKE.get_id().to_string();
        let _ = self.set_id(id.as_str());
    }
    // pub fn clone(&self) -> Self {
    //     let new_pb = Self::new();
    //     new_pb.head
    // }

    pub fn set_sid(&mut self, sid: i64) {
        self.sid = sid;
    }

    pub fn set_head(&mut self, head: [u8; PROTOCOL_BODY_HEAD_LEN]) -> &mut Self {
        self.head = head;
        self
    }

    pub fn is_defer(&self) -> bool {
        self.head[0].is_1(7)
    }

    fn set_flag(&mut self, index: usize, pos: u8, on: bool) {
        if index >= self.head.len() || pos > 7 {
            return;
        }
        let mut flag = self.head[index];
        if on {
            (&mut flag).set_1(pos);
        } else {
            (&mut flag).set_0(pos);
        }
        self.head[index] = flag;
    }

    pub fn with_defer(&mut self, defer: bool) -> &mut Self {
        self.set_flag(0, 7, defer);
        self
    }

    pub fn is_ack(&self) -> bool {
        self.head[0].is_1(6)
    }

    pub fn with_ack(&mut self, ack: bool) -> &mut Self {
        self.set_flag(0, 6, ack);
        self
    }

    pub fn is_persist(&self) -> bool {
        self.head[0].is_1(5)
    }

    pub fn with_persist(&mut self, persist: bool) -> &mut Self {
        self.set_flag(0, 5, persist);
        self
    }

    pub fn is_delete(&self) -> bool {
        self.head[0].is_1(4)
    }

    pub fn with_delete(&mut self, delete: bool) -> &mut Self {
        self.set_flag(0, 4, delete);
        self
    }

    pub fn is_ready(&self) -> bool {
        self.head[0].is_1(3)
    }

    pub fn with_ready(&mut self, ready: bool) -> &mut Self {
        self.set_flag(0, 3, ready);
        self
    }

    pub fn id_len(&self) -> u8 {
        self.head[1]
    }

    pub fn set_id(&mut self, id: &str) -> Result<()> {
        if id.len() > u8::MAX as usize {
            return Err(anyhow!("id len exceed max length 128"));
        }
        self.id = id.to_string();
        self.head[1] = id.len() as u8;
        Ok(())
    }

    pub fn body_len(&self) -> u64 {
        u64::from_be_bytes(
            self.head[2..PROTOCOL_BODY_HEAD_LEN]
                .to_vec()
                .try_into()
                .expect("get body length failed"),
        )
    }

    pub fn set_body(&mut self, body: Bytes) -> Result<()> {
        // set the body length in head
        let body_len = body.len().to_be_bytes();
        let mut new_head = self.head[..2].to_vec();
        new_head.extend(body_len);
        self.head = new_head
            .try_into()
            .expect("convert to protocol body head failed");

        self.body = body;

        Ok(())
    }

    pub fn set_defer_time(&mut self, defer_time: u64) {
        if defer_time == 0 {
            return;
        }
        self.defer_time = defer_time;
        self.with_defer(true);
    }
}

// 一个标准的消息体
#[derive(Debug)]
pub struct Message {
    head: ProtocolHead,
    body: ProtocolBodys,
}

impl Message {
    pub fn new() -> Self {
        Message {
            head: ProtocolHead::default(),
            body: ProtocolBodys::new(),
        }
    }

    pub fn with(head: ProtocolHead, body: ProtocolBodys) -> Self {
        Message { head, body }
    }

    pub fn clone(&self) -> Self {
        let mut msg = Self::new();
        msg.head = self.head.clone();
        msg.body = self.body.clone();

        msg
    }

    pub fn get_topic(&self) -> &str {
        self.head.topic.as_str()
    }

    pub fn post_fill(&mut self) {
        self.head.post_fill();
        self.body.post_fill();
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.head.as_bytes());
        result.extend(self.body.as_bytes());

        result
    }
    // 该message是否合法
    // pub fn is_valid(&self) -> Result<()> {
    //    matcch self.head.head[0]{
    //     case
    //    }

    //     Ok(())
    // }
}
