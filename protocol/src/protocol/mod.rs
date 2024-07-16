pub mod v1;

use crate::error::*;
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;
use common::global::{self, SNOWFLAKE};
use rsbit::{BitFlagOperation, BitOperation};
use std::{fmt::Debug, ops::Deref, pin::Pin};
use tokio::io::AsyncReadExt;
use v1::{ProtocolBodyV1, ProtocolBodysV1, ProtocolHeadV1};

pub const SUPPORT_PROTOCOLS: [u8; 1] = [1];

/// 固定的6字节协议头
pub const PROTOCOL_HEAD_LEN: usize = 10;
pub const PROTOCOL_MAX_MSG_NUM: u8 = 15;
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
*           4bit: message number. (max is [`PROTOCOL_MAX_MSG_NUM`])
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
pub struct Head([u8; PROTOCOL_HEAD_LEN]);

impl Deref for Head {
    type Target = [u8; PROTOCOL_HEAD_LEN];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<[u8; PROTOCOL_HEAD_LEN]> for Head {
    fn from(value: [u8; PROTOCOL_HEAD_LEN]) -> Self {
        Head(value)
    }
}

impl Default for Head {
    fn default() -> Self {
        Self([0_u8; PROTOCOL_HEAD_LEN])
    }
}

impl Head {
    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Head(self.0)
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.as_slice().to_vec()
    }

    fn set_head_flag(&mut self, index: usize, pos: u8, on: bool) {
        if index >= self.0.len() || pos > 7 {
            return;
        }
        let mut flag = self.0[index];
        if on {
            (&mut flag).set_1(pos);
        } else {
            (&mut flag).set_0(pos);
        }
        self.0[index] = flag;
    }

    pub fn set_flag_resq(&mut self, resp: bool) -> &mut Self {
        self.set_head_flag(1, 7, resp);
        self
    }

    pub fn action(&self) -> u8 {
        self.0[0]
    }

    pub fn set_action(&mut self, action: u8) -> &mut Self {
        self.0[0] = action;
        self
    }

    pub fn is_req(&self) -> bool {
        self.0[1].is_0(7)
    }

    pub fn topic_ephemeral(&self) -> bool {
        self.0[1].is_1(6)
    }

    pub fn channel_ephemeral(&self) -> bool {
        self.0[1].is_1(5)
    }

    pub fn prohibit_instant(&self) -> bool {
        self.0[1].is_1(4)
    }

    pub fn set_prohibit_instant(&mut self, prohibit: bool) -> &mut Self {
        self.set_head_flag(1, 4, prohibit);
        self
    }

    pub fn prohibit_defer(&self) -> bool {
        self.0[1].is_1(3)
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
        self.0[1].is_1(4)
    }

    pub fn reject(&self) -> bool {
        self.0[1].is_1(3)
    }

    pub fn topic_len(&self) -> u8 {
        self.0[4]
    }

    pub fn channel_len(&self) -> u8 {
        self.0[5]
    }

    pub fn token_len(&self) -> u8 {
        self.0[6]
    }

    pub fn reject_code(&self) -> u8 {
        self.0[7]
    }

    pub fn set_reject_code(&mut self, code: u8) -> &mut Self {
        let mut flag = self.0[1];
        if code == 0 {
            (&mut flag).set_0(3);
        } else {
            (&mut flag).set_1(3);
        }
        self.0[1] = flag;
        self.0[7] = code;

        self
    }

    pub fn defer_format_len(&self) -> u8 {
        self.0[8]
    }

    pub fn set_defer_format_len(&mut self, len: u8) -> &mut Self {
        self.0[8] = len;
        self
    }

    pub fn version(&self) -> u8 {
        (self.0[3] & 0b11110000) >> 4
    }

    pub fn set_version(&mut self, v: u8) -> Result<()> {
        if v > 0b00001111 || v == 0 {
            return Err(anyhow!("illigal protocol version number"));
        }
        self.0[3] |= v << 4;

        Ok(())
    }

    pub fn msg_num(&self) -> u8 {
        self.0[3] & 0b00001111
    }

    pub fn set_msg_num(&mut self, num: u8) -> Result<()> {
        if num > 0b00001111 {
            return Err(anyhow!("num exceed maxnuim message number"));
        }
        self.0[3] |= num;

        Ok(())
    }

    pub fn set_topic_ephemeral(&mut self, ephemeral: bool) -> &mut Self {
        let mut flag = self.0[1];
        if ephemeral {
            (&mut flag).set_1(6);
        } else {
            (&mut flag).set_0(6);
        }
        self.0[1] = flag;
        self
    }

    pub fn set_channel_ephemeral(&mut self, ephemeral: bool) -> &mut Self {
        let mut flag = self.0[1];
        if ephemeral {
            (&mut flag).set_1(5);
        } else {
            (&mut flag).set_0(5);
        }
        self.0[1] = flag;
        self
    }

    pub fn set_topic_len(&mut self, len: u8) -> &mut Self {
        self.0[4] = len;
        self
    }

    pub fn set_channel_len(&mut self, len: u8) -> &mut Self {
        self.0[5] = len;
        self
    }

    pub fn set_token_len(&mut self, len: u8) -> &mut Self {
        self.0[6] = len;
        self
    }
}

pub enum ProtocolHead {
    V1(ProtocolHeadV1),
}

impl Default for ProtocolHead {
    fn default() -> Self {
        Self::V1(ProtocolHeadV1::new())
    }
}

pub enum ProtocolBodys {
    V1(ProtocolBodysV1),
}

impl Default for ProtocolBodys {
    fn default() -> Self {
        Self::V1(ProtocolBodysV1::new())
    }
}

pub enum ProtocolBody {
    V1(ProtocolBodyV1),
}
impl Default for ProtocolBody {
    fn default() -> Self {
        Self::V1(ProtocolBodyV1::new())
    }
}

/// [`parse_head_from_reader`] read the protocol head from reader.
pub async fn parse_head_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
) -> Result<ProtocolHead> {
    // let mut ph: ProtocolHead = ProtocolHead::new();
    let mut buf = BytesMut::new();

    // debug!(addr = "{self.addr:?}", "read protocol head");
    // parse head
    buf.resize(PROTOCOL_HEAD_LEN, 0);
    reader.read_exact(&mut buf).await?;
    let _head: [u8; PROTOCOL_HEAD_LEN] = buf
        .to_vec()
        .try_into()
        .expect("convert BytesMut to array failed");
    let head = Head::from(_head);
    let version = head.version();
    match version {
        1 => {
            let mut v1 = ProtocolHeadV1::new_with_head(head);
            v1.read_parse(reader).await?;
            Ok(ProtocolHead::V1(v1))
        }
        _ => Err(anyhow!("not support protocol version")),
    }
}

/// [`parse_body_from_reader`] read the protocol bodys from reader.
pub async fn parse_body_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
    head: &ProtocolHead,
) -> Result<ProtocolBodys> {
    match head {
        ProtocolHead::V1(head_v1) => {
            let mut bodys = ProtocolBodysV1::new();
            bodys.read_parse(reader, head_v1.msg_num()).await?;
            Ok(ProtocolBodys::V1(bodys))
        }
    }
}
