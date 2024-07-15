use crate::error::*;
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use common::global::{self, SNOWFLAKE};
use rsbit::{BitFlagOperation, BitOperation};
use std::{fmt::Debug, pin::Pin, result::Result as StdResult};
use tokio::{io::AsyncReadExt, net::tcp::OwnedReadHalf};
use tracing::debug;

pub const SUPPORT_PROTOCOLS: [u8; 1] = [1];

/// 固定的6字节协议头
pub const PROTOCOL_HEAD_LEN: usize = 10;
/// 固定的3字节协议体的头长度
pub const PROTOCOL_BODY_HEAD_LEN: usize = 12;

pub const SUPPORT_ACTIONS: [u8; 9] = [
    ACTION_FIN,
    ACTION_RDY,
    ACTION_REQ,
    ACTION_PUB,
    ACTION_NOP,
    ACTION_TOUCH,
    ACTION_SUB,
    ACTION_CLS,
    ACTION_AUTH,
];

/// fin action
pub const ACTION_FIN: u8 = 1;

/// rdy action
pub const ACTION_RDY: u8 = 2;

/// req action
pub const ACTION_REQ: u8 = 3;

/// pub action
pub const ACTION_PUB: u8 = 4;

/// nop action
pub const ACTION_NOP: u8 = 5;

/// touch action
pub const ACTION_TOUCH: u8 = 6;

/// sub action
pub const ACTION_SUB: u8 = 7;

/// cls action
pub const ACTION_CLS: u8 = 8;

/// auth action
pub const ACTION_AUTH: u8 = 9;
/**
### FIXED HEAD LENGTH([[`PROTOCOL_HEAD_LEN`] bytes):
* HEAD: 10 bytes:
* 1st byte: action[fin, rdy, req, pub , mpub, dpub, nop, touch, sub, cls, auth]
* 2nd byte: global flags:
*           1bit: req flag: 0 represent is a req, 1 represent is a resp
*           1bit: topic is ephemeral: true mean the topic is ephemeral, it will be delete then there is no publishers.
*           1bit: channel is ephemeral: true mean the channel is ephemeral, it will be delete then there is no subcribers.
*           1bit: is heartbeat.
*           1bit: reject connect. 1st flag must be resp, and the 8th byte mean the reject code.
*           1bit: prohibit send instant message (default is 0).
*           1bit: prohibit send defer message (default is 0).
*           left 3 bits: extend flags.
* 3rd byte: extend flags
* 4th byte:
*           4bit: version: protocol version.
*           4bit: message number. (max is 128)
* 5th byte: topic length.
* 6th byte: channel length.
* 7th byte: token length.
* 8th byte: reject code.
* 9th byte: custom defer message store format length.
* 10th bytes: extend bytes.
*
* optional:
*           topic value.
*           channel value.
*           token value.
*           custom defer message store format.
*/
#[derive(Default)]
pub struct ProtocolHead {
    head: [u8; PROTOCOL_HEAD_LEN],
    topic: String,
    channel: String,
    token: String,
    // reject_code: u8,
    defer_msg_format: String,
}

impl Debug for ProtocolHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolHead")
            .field("head:req", &self.is_req())
            .field("head:topic-ephemeral", &self.topic_ephemeral())
            .field("head:channel-ephemeral", &self.channel_ephemeral())
            .field("head:heartbeat", &self.heartbeat())
            .field("head:reject", &self.reject())
            .field("head:reject-code", &self.reject_code())
            .field("protocol-version", &self.version())
            .field("msg-number", &self.msg_num())
            .field("topic-name", &self.topic)
            .field("channel-name", &self.channel)
            .field("token", &self.token)
            .finish()
    }
}

// impl Default for ProtocolHead {
//     fn default() -> Self {
//         Self {
//             head: Default::default(),
//             topic: Default::default(),
//             channel: Default::default(),
//             token: Default::default(),
//         }
//     }
// }

impl ProtocolHead {
    pub fn new() -> Self {
        ProtocolHead {
            head: [0_u8; PROTOCOL_HEAD_LEN],
            topic: String::new(),
            channel: String::new(),
            token: String::new(),
            defer_msg_format: String::new(),
        }
    }

    pub fn calc_len(&self) -> usize {
        let mut size = PROTOCOL_HEAD_LEN;
        size += self.topic.len();
        size += self.channel.len();
        size += self.token.len();
        size += self.defer_msg_format.len();
        size
    }

    pub fn init(&mut self) -> Result<()> {
        if self.topic.is_empty() {
            self.set_topic("default")?;
        }
        if self.channel.is_empty() {
            self.set_channel("default")?;
        }

        Ok(())
    }

    /// [`parse_from`] read the protocol head from bts.
    // pub async fn parse_from_sync(fd: &mut Pin<&mut (impl Read + Send)>) -> Result<Self> {
    //     let (mut client, mut server) = duplex(10);

    //     tokio::spawn(async move {
    //         let mut buf = vec![];
    //         fd.read_to_end(&mut buf);
    //         client.write_all(&buf);
    //     });

    //     let mut pfd = Pin::new(&mut server);
    //     Self::parse_from(&mut pfd).await
    // }

    /// [`parse_from`] read the protocol head from bts.
    pub async fn parse_from(fd: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut head = ProtocolHead::new();
        head.read_parse(fd).await?;
        Ok(head)
    }

    /// [`read_parse`] read the protocol head from reader.
    pub async fn read_parse(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        // let mut ph: ProtocolHead = ProtocolHead::new();
        let mut buf = BytesMut::new();

        // debug!(addr = "{self.addr:?}", "read protocol head");
        // parse head
        buf.resize(PROTOCOL_HEAD_LEN, 0);
        reader.read_exact(&mut buf).await?;
        self.set_head(
            buf.to_vec()
                .try_into()
                .expect("convert BytesMut to array failed"),
        );
        debug!(addr = "{self.addr:?}", "parse topic name");

        // parse topic name
        buf.resize(self.topic_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let topic: String = String::from_utf8(buf.to_vec()).expect("illigal topic name");
        self.set_topic(topic.as_str())?;

        debug!(addr = "{self.addr:?}", "parse channel name");

        // parse channel name
        buf.resize(self.channel_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let channel = String::from_utf8(buf.to_vec()).expect("illigal channel name");
        self.set_channel(channel.as_str())?;

        debug!(addr = "{self.addr:?}", "parse token");
        // parse token
        buf.resize(self.token_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let token = String::from_utf8(buf.to_vec()).expect("illigal token value");
        self.set_token(token.as_str())?;

        // parse custom defer message format
        buf.resize(self.defer_format_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let fmt =
            String::from_utf8(buf.to_vec()).expect("illigal custom-defer-message-format value");
        self.set_defer_format(fmt.as_str())?;

        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        let mut ph = Self::new();
        ph.set_head(self.head);
        let _ = ph.set_topic(self.topic.as_str());
        let _ = ph.set_channel(self.channel.as_str());
        let _ = ph.set_token(self.token.as_str());
        let _ = ph.set_defer_format(self.defer_msg_format.as_str());

        ph
    }

    pub fn validate(&self, max_msg_num: u8) -> StdResult<(), ProtocolError> {
        let v = self.version();
        if !SUPPORT_PROTOCOLS.contains(&v) {
            return Err(ProtocolError::new(PROT_ERR_CODE_NOT_SUPPORT_VERSION));
        }
        if self.is_req() {
            return self.validate_req(max_msg_num);
        }

        self.validate_resq(max_msg_num)
    }

    /// [`validate_req`] validate the head is valid in protocol.
    fn validate_req(&self, max_msg_num: u8) -> StdResult<(), ProtocolError> {
        if !self.is_req() {
            return Err(ProtocolError::new(PROT_ERR_CODE_ZERO));
        }
        if self.reject() || self.reject_code() != 0 {
            return Err(ProtocolError::new(PROT_ERR_CODE_SHOULD_NOT_REJECT_CODE));
        }
        match self.action() {
            ACTION_PUB | ACTION_FIN | ACTION_RDY | ACTION_REQ => {
                if self.msg_num() == 0 {
                    return Err(ProtocolError::new(PROT_ERR_CODE_NEED_MSG));
                }
                if self.msg_num() > max_msg_num {
                    return Err(ProtocolError::new(PROT_ERR_CODE_EXCEED_MAX_NUM));
                }
            }
            _ => {
                if self.msg_num() != 0 {
                    return Err(ProtocolError::new(PROT_ERR_CODE_SHOULD_NOT_MSG));
                }
            }
        }
        Ok(())
    }

    /// [`validate_resq`] validate the head is valid in protocol.
    fn validate_resq(&self, max_msg_num: u8) -> StdResult<(), ProtocolError> {
        if self.is_req() {
            return Err(ProtocolError::new(PROT_ERR_CODE_ZERO));
        }
        if self.msg_num() > max_msg_num {
            return Err(ProtocolError::new(PROT_ERR_CODE_EXCEED_MAX_NUM));
        }
        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.head);
        result.extend(self.topic.as_bytes());
        result.extend(self.channel.as_bytes());
        result.extend(self.token.as_bytes());
        result.extend(self.defer_msg_format.as_bytes());

        result
    }

    pub fn set_head(&mut self, head: [u8; PROTOCOL_HEAD_LEN]) -> &mut Self {
        self.head = head;
        self
    }

    fn set_head_flag(&mut self, index: usize, pos: u8, on: bool) {
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

    pub fn set_flag_resq(&mut self, resp: bool) -> &mut Self {
        self.set_head_flag(1, 7, resp);
        self
    }

    pub fn action(&self) -> u8 {
        self.head[0]
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

    pub fn prohibit_instant(&self) -> bool {
        self.head[1].is_1(4)
    }

    pub fn set_prohibit_instant(&mut self, prohibit: bool) -> &mut Self {
        self.set_head_flag(1, 4, prohibit);
        self
    }

    pub fn prohibit_defer(&self) -> bool {
        self.head[1].is_1(3)
    }

    pub fn set_prohibit_defer(&mut self, prohibit: bool) -> &mut Self {
        self.set_head_flag(1, 3, prohibit);
        self
    }

    pub fn set_heartbeat(&mut self, hb: bool) -> &mut Self {
        self.set_head_flag(1, 4, hb);
        self
    }

    pub fn heartbeat(&self) -> bool {
        self.head[1].is_1(4)
    }

    pub fn reject(&self) -> bool {
        self.head[1].is_1(3)
    }

    pub fn topic_len(&self) -> u8 {
        self.head[4]
    }

    pub fn channel_len(&self) -> u8 {
        self.head[5]
    }

    pub fn token_len(&self) -> u8 {
        self.head[6]
    }

    pub fn reject_code(&self) -> u8 {
        self.head[7]
    }

    pub fn set_reject_code(&mut self, code: u8) -> &mut Self {
        let mut flag = self.head[1];
        if code == 0 {
            (&mut flag).set_0(3);
        } else {
            (&mut flag).set_1(3);
        }
        self.head[1] = flag;
        self.head[7] = code;

        self
    }

    pub fn defer_format_len(&self) -> u8 {
        self.head[8]
    }

    pub fn set_defer_format(&mut self, fmt: &str) -> Result<()> {
        if fmt.len() > u8::MAX as usize {
            return Err(anyhow!("exceed the max the u8"));
        }
        self.head[8] = fmt.len() as u8;
        self.defer_msg_format = fmt.to_string();
        Ok(())
    }

    pub fn version(&self) -> u8 {
        (self.head[3] & 0b11110000) >> 4
    }

    pub fn set_version(&mut self, v: u8) -> Result<()> {
        if v > 0b00001111 || v == 0 {
            return Err(anyhow!("illigal protocol version number"));
        }
        self.head[3] |= v << 4;

        Ok(())
    }

    pub fn msg_num(&self) -> u8 {
        self.head[3] & 0b00001111
    }

    pub fn set_msg_num(&mut self, num: u8) -> Result<()> {
        if num > 0b00001111 {
            return Err(anyhow!("num exceed maxnuim message number"));
        }
        self.head[3] |= num;

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

    pub fn topic(&self) -> &str {
        let tp = self.topic.as_str();
        if tp.is_empty() {
            return "default";
        }
        tp
    }

    pub fn channel(&self) -> &str {
        let chan = self.channel.as_str();
        if chan.is_empty() {
            return "default";
        }
        chan
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
#[derive(Debug, Default)]
pub struct ProtocolBodys {
    sid: i64, // 标识一批次的message
    pub list: Vec<ProtocolBody>,
}

impl ProtocolBodys {
    pub fn new() -> Self {
        ProtocolBodys {
            list: Vec::new(),
            sid: global::SNOWFLAKE.get_id(),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        if self.sid == 0 {
            self.sid = SNOWFLAKE.get_id();
        }
        let iter = self.list.iter_mut();
        for pb in iter {
            if pb.id_len() == 0 {
                pb.with_id(SNOWFLAKE.get_id().to_string().as_str())?;
            }
        }

        Ok(())
    }

    /// [`read_parse`] read the protocol bodys from reader.
    pub async fn read_parse(&mut self, reader: &mut OwnedReadHalf, mut msg_num: u8) -> Result<()> {
        // let mut pbs = ProtocolBodys::new();
        while msg_num != 0 {
            msg_num -= 1;

            let mut pb = ProtocolBody::new();
            let mut buf = BytesMut::new();

            // parse protocol body head
            buf.resize(PROTOCOL_BODY_HEAD_LEN, 0);
            reader.read_exact(&mut buf).await?;
            pb.with_head(
                buf.to_vec()
                    .try_into()
                    .expect("convert to protocol body head failed"),
            );

            // parse defer time
            if pb.is_defer() {
                buf.resize(8, 0);
                reader.read_exact(&mut buf).await?;
                let defer_time = u64::from_be_bytes(
                    buf.to_vec()
                        .try_into()
                        .expect("convert to defer time failed"),
                );
                if pb.is_defer_concrete() {
                    pb.with_defer_time_concrete(defer_time);
                } else {
                    pb.with_defer_time_offset(defer_time);
                }
            }

            // parse id
            let id_len = pb.id_len();
            if id_len != 0 {
                buf.resize(id_len as usize, 0);
                reader.read_exact(&mut buf).await?;
                let id = String::from_utf8(buf.to_vec()).expect("illigal id value");
                pb.with_id(id.as_str())?;
            }
            // parse body
            let body_len = pb.body_len();
            if body_len != 0 {
                buf.resize(body_len as usize, 0);
                reader.read_exact(&mut buf).await?;
                let bts: Bytes = Bytes::copy_from_slice(buf.as_ref());
                pb.with_body(bts)?;
            }

            self.push(pb);
        }

        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    #[allow(clippy::should_implement_trait)]
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

    pub fn validate(&self, max_msg_len: u64) -> StdResult<(), ProtocolError> {
        let iter = self.list.iter();
        for pb in iter {
            if pb.body_len() > max_msg_len {
                return Err(ProtocolError::new(PROT_ERR_CODE_EXCEED_MAX_LEN));
            }
        }
        Ok(())
    }
}
/**
### FIXED HEAD LENGTH([[`PROTOCOL_HEAD_LEN`] bytes), Every body has the same structure:
* head: 10 bytes:
* 1st byte: flag:
*           1bit: is update: only support [`delete`], [`consume`], [`notready`] flags.
*           1bit: is ack: mark the message weather need ack.
*           1bit: is persist immediately: mark this message need persist to [`Storage`].
*           1bit: is defer: mark this message is a defer message.
*           1bit: defer type: 0: offset expired timme; 1: concrete expired time.
*           1bit: is delete: mark this message is delete, then will not be consumed if it not consume.(must with MSG_ID)（优先级高于is ready）
*           1bit: is notready: mark this message if notready. false mean the message can't be consumed, true mean the message can be consumed.(must with MSG_ID)
*           1bit: is consume: mark the message has been consumed.
* 2nd byte: ID-LENGTH
* 3-10th bytes: BODY-LENGTH(8 bytes)
* 11-12th bytes: extend bytes
*
* optional:
*           8byte defer time.
*           id value(length determine by ID-LENGTH)
*           body value(length determine by BODY-LENGTH)
*/
#[derive(Default)]
pub struct ProtocolBody {
    head: [u8; PROTOCOL_BODY_HEAD_LEN],
    sid: i64,        // session_id: generate by ProtocolBodys
    defer_time: u64, // 8 bytes
    pub id: String,
    body: Bytes,
}

impl Debug for ProtocolBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolBody")
            .field("is_defer", &self.is_defer())
            .field("is_ack", &self.is_ack())
            .field("is_persist", &self.is_persist())
            .field("is_delete", &self.is_delete())
            .field("is_not_ready", &self.is_notready())
            .field("is_consume", &self.is_consume())
            .field("defer_time", &self.defer_time)
            .field("id", &self.id)
            .field("body", &self.body)
            .finish()
    }
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

    pub fn calc_len(&self) -> usize {
        let mut length = PROTOCOL_BODY_HEAD_LEN;
        if self.is_defer() {
            length += 8;
        }
        length += self.id_len() as usize;
        length += self.body_len() as usize;

        length
    }

    /// [`parse_from`] read the protocol bodys from bts.
    pub async fn parse_from<T: AsyncReadExt>(fd: &mut Pin<&mut T>) -> Result<Self> {
        let mut pb = Self::new();
        pb.read_parse(fd).await?;
        Ok(pb)
    }

    pub async fn read_parse<T: AsyncReadExt>(&mut self, fd: &mut Pin<&mut T>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse protocol body head
        buf.resize(PROTOCOL_BODY_HEAD_LEN, 0);
        fd.read_exact(&mut buf).await?;
        self.with_head(
            buf.to_vec()
                .try_into()
                .expect("convert to protocol body head failed"),
        );

        // parse defer time
        if self.is_defer() {
            buf.resize(8, 0);
            fd.read_exact(&mut buf).await?;
            let defer_time = u64::from_be_bytes(
                buf.to_vec()
                    .try_into()
                    .expect("convert to defer time failed"),
            );
            self.defer_time = defer_time;
        }

        // parse id
        let id_len = self.id_len();
        if id_len != 0 {
            buf.resize(id_len as usize, 0);
            fd.read_exact(&mut buf).await?;
            let id = String::from_utf8(buf.to_vec()).expect("illigal id value");
            self.with_id(id.as_str())?;
        }

        // parse body
        let body_len = self.body_len();
        if body_len != 0 {
            buf.resize(body_len as usize, 0);
            fd.read_exact(&mut buf).await?;
            let bts: Bytes = Bytes::copy_from_slice(buf.as_ref());
            self.with_body(bts)?;
        }

        Ok(())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        let mut pb = Self::new();
        pb.with_head(self.head);
        pb.set_sid(self.sid);
        let _ = pb.with_defer_time_concrete(self.defer_time);
        let _ = pb.with_id(self.id.as_str());
        let _ = pb.with_body(self.body.clone());

        pb
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(&self.head);
        if self.defer_time != 0 {
            result.extend(self.defer_time.to_be_bytes());
        }
        if !self.id.is_empty() {
            result.extend(self.id.as_bytes());
        }
        result.extend(self.body.as_ref());

        result
    }

    pub fn set_sid(&mut self, sid: i64) {
        self.sid = sid;
    }

    pub fn with_head(&mut self, head: [u8; PROTOCOL_BODY_HEAD_LEN]) -> &mut Self {
        self.head = head;
        self
    }

    pub fn defer_time(&self) -> u64 {
        self.defer_time
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

    // updated flag.
    pub fn is_update(&self) -> bool {
        self.head[0].is_1(7)
    }

    /// set the update flag value.
    pub fn with_update(&mut self, update: bool) -> &mut Self {
        self.set_flag(0, 7, update);
        self
    }

    // ack flag
    pub fn is_ack(&self) -> bool {
        self.head[0].is_1(6)
    }

    /// set the ack flag value.
    pub fn with_ack(&mut self, ack: bool) -> &mut Self {
        self.set_flag(0, 6, ack);
        self
    }

    /// persist flag.
    pub fn is_persist(&self) -> bool {
        self.head[0].is_1(5)
    }

    /// set the persist flag value.
    pub fn with_persist(&mut self, persist: bool) -> &mut Self {
        self.set_flag(0, 5, persist);
        self
    }

    /// defer flag.
    pub fn is_defer(&self) -> bool {
        self.head[0].is_1(4)
    }

    /// set the defer flag value.
    pub fn with_defer(&mut self, defer: bool) -> &mut Self {
        self.set_flag(0, 4, defer);
        self
    }

    /// defer_concrete flag.
    pub fn is_defer_concrete(&self) -> bool {
        self.head[0].is_1(3)
    }

    /// set the defer_concrete flag value.
    pub fn with_defer_concrete(&mut self, concrete: bool) -> &mut Self {
        self.set_flag(0, 3, concrete);
        self
    }

    /// delete flag.
    pub fn is_delete(&self) -> bool {
        self.head[0].is_1(2)
    }

    /// set the delete flag value.
    pub fn with_delete(&mut self, delete: bool) -> &mut Self {
        self.set_flag(0, 2, delete);
        self
    }

    /// notready flag
    pub fn is_notready(&self) -> bool {
        self.head[0].is_1(1)
    }

    /// set the notready flag value.
    pub fn with_notready(&mut self, notready: bool) -> &mut Self {
        self.set_flag(0, 1, notready);
        self
    }

    /// consume flag.
    pub fn is_consume(&self) -> bool {
        self.head[0].is_1(0)
    }

    /// set the consume flag value.
    pub fn with_consume(&mut self, consume: bool) -> &mut Self {
        self.set_flag(0, 0, consume);
        self
    }

    pub fn id_len(&self) -> u8 {
        self.head[1]
    }

    pub fn with_id(&mut self, id: &str) -> Result<()> {
        if id.len() > u8::MAX as usize {
            return Err(anyhow!("id len exceed max length 128"));
        }
        self.id = id.to_string();
        self.head[1] = id.len() as u8;
        Ok(())
    }

    pub fn body_len(&self) -> u64 {
        u64::from_be_bytes(
            self.head[2..PROTOCOL_BODY_HEAD_LEN - 2]
                .to_vec()
                .try_into()
                .expect("get body length failed"),
        )
    }

    pub fn with_body(&mut self, body: Bytes) -> Result<()> {
        // set the body length in head
        let body_len = body.len().to_be_bytes();
        let mut new_head = self.head[..2].to_vec();
        let old_head_tail = self.head[10..].to_vec();
        new_head.extend(body_len);
        new_head.extend(old_head_tail);
        self.head = new_head
            .try_into()
            .expect("convert to protocol body head failed");

        self.body = body;

        Ok(())
    }

    /// [`with_defer_time_offset`] set the offset expired time for [`ProtocolBody`]
    pub fn with_defer_time_offset(&mut self, offset: u64) -> &mut Self {
        if offset == 0 {
            return self;
        }
        let dt = Local::now();
        self.defer_time = dt.timestamp() as u64 + offset;
        self.with_defer(true);
        self.with_defer_concrete(false);
        self
    }

    /// [`with_defer_time_concrete`] set the concrete expired time for [`ProtocolBody`]
    pub fn with_defer_time_concrete(&mut self, concrete_time: u64) -> &mut Self {
        if concrete_time == 0 {
            return self;
        }
        self.defer_time = concrete_time;
        self.with_defer(true);
        self.with_defer_concrete(true);
        self
    }
}
