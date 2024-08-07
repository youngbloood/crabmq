use super::MessageOperation;
use crate::{
    consts::{ACTION_MSG, PROPTOCOL_V1, X25},
    error::{ProtError, E_BAD_CRC},
    protocol::{
        v1::{dispatch_message::DispatchMessage, V1},
        Head, Protocol, HEAD_LENGTH,
    },
};
use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::Local;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    pin::Pin,
};
use tokio::io::AsyncReadExt;

/**
 * [`MessageV1`] is a unit that store into Storage Media.
 * Encoding:
 *         2 bytes: head.
 *         1 byte: topic length.
 *         1 byte: channel length.
 *         n bytes: topic name.
 *         n bytes: channel name.
 *         n bytes: BinMessage.
 */
#[derive(Clone, Debug)]
pub struct MessageV1 {
    head: Head,
    topic: String,
    channel: String,
    msg: MessageUserV1,
}

impl Default for MessageV1 {
    fn default() -> Self {
        let mut head = Head::default();
        head.set_version(PROPTOCOL_V1).set_action(ACTION_MSG);
        Self {
            head: Head::default(),
            topic: "default".to_string(),
            channel: "default".to_string(),
            msg: Default::default(),
        }
    }
}

impl Deref for MessageV1 {
    type Target = MessageUserV1;

    fn deref(&self) -> &Self::Target {
        &self.msg
    }
}

impl DerefMut for MessageV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.msg
    }
}

impl MessageOperation for MessageV1 {
    fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend([self.topic.len() as u8]);
        res.extend([self.channel.len() as u8]);
        res.extend(self.topic.as_bytes());
        res.extend(self.channel.as_bytes());
        res.extend(self.msg.as_bytes());

        res.to_vec()
    }

    fn convert_to_protocol(self) -> Protocol {
        let version = self.head.get_version();
        match version {
            PROPTOCOL_V1 => {
                let mut dispatch_msg = DispatchMessage::default();
                dispatch_msg.set_msg(self.msg);
                let mut v1 = V1::default();
                v1.set_msg(dispatch_msg);
                Protocol::V1(v1)
            }
            _ => unreachable!(),
        }
    }

    fn get_topic(&self) -> &str {
        &self.topic
    }

    fn get_channel(&self) -> &str {
        &self.channel
    }

    fn get_id(&self) -> &str {
        self.msg.get_id()
    }

    fn defer_time(&self) -> u64 {
        self.msg.get_defer_time()
    }

    fn is_notready(&self) -> bool {
        self.msg.is_notready()
    }

    fn is_ack(&self) -> bool {
        self.msg.is_ack()
    }

    fn is_persist(&self) -> bool {
        self.msg.is_persist()
    }

    fn is_deleted(&self) -> bool {
        self.msg.is_deleted()
    }

    fn is_consumed(&self) -> bool {
        self.msg.is_consumed()
    }
}

impl MessageV1 {
    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) -> &mut Self {
        self.topic = topic.to_string();
        self
    }

    pub fn get_channel(&self) -> &str {
        &self.channel
    }

    pub fn set_channel(&mut self, channel: &str) -> &mut Self {
        self.channel = channel.to_string();
        self
    }

    pub fn get_message(&self) -> MessageUserV1 {
        self.msg.clone()
    }

    pub fn set_message(&mut self, msg: MessageUserV1) -> &mut Self {
        self.msg = msg;
        self
    }

    pub async fn parse_from_reader(
        reader: &mut Pin<&mut impl AsyncReadExt>,
        head: Head,
    ) -> Result<Self> {
        let mut msgv1 = MessageV1::default();
        msgv1.set_head(head);

        let mut buf = BytesMut::new();
        // parse head
        buf.resize(HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        msgv1.set_head(Head::with(
            buf.to_vec().try_into().expect("conver to head failed"),
        ));

        // parse topic length
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        let topic_len = { *buf.first().unwrap() };

        // parse channel length
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        let channel_len = { *buf.first().unwrap() };

        // parse topic
        if topic_len != 0 {
            buf.resize(topic_len as _, 0);
            reader.read_exact(&mut buf).await?;
            msgv1.set_topic(&String::from_utf8(buf.to_vec())?);
        }

        // parse channel
        if channel_len != 0 {
            buf.resize(channel_len as _, 0);
            reader.read_exact(&mut buf).await?;
            msgv1.set_channel(&String::from_utf8(buf.to_vec())?);
        }

        msgv1.set_message(
            MessageUserV1::parse_from(reader, 1)
                .await?
                .first()
                .unwrap()
                .to_owned(),
        );

        Ok(msgv1)
    }
}

const MESSAGE_USER_V1_HEAD_LENGTH: usize = 12;
/**
### FIXED HEAD LENGTH([[`PROTOCOL_HEAD_LEN`] bytes), Every body has the same structure:
* head: 10 bytes:
* 1st byte: flag:
*           1bit: has crc.
*           1bit: is ack: mark the message weather need ack.
*           1bit: is persist immediately: mark this message need persist to [`Storage`].
*           1bit: is defer: mark this message is a defer message.
*           1bit: defer type: 0: offset expired timme; 1: concrete expired time.
*           1bit: is notready: mark this message if notready. false mean the message can't be consumed, true mean the message can be consumed.(must with MSG_ID)
*           1bit: is consumed: denote the message is consumed.
*           1bit: is deleted: denote the message is deleted.
* 2nd byte: ID-LENGTH
* 3-10th bytes: BODY-LENGTH(8 bytes)
* 11-12th byte: reserve byte
*
* optional:
*           2 bytes: crc.
*           8 bytes: defer time.
*           id value(length determined by ID-LENGTH)
*           body value(length determined by BODY-LENGTH)
*/

#[derive(Default, Clone)]
pub struct MessageUserV1Head([u8; MESSAGE_USER_V1_HEAD_LENGTH]);

impl Debug for MessageUserV1Head {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageUserV1Head")
            .field("has-crc", &self.has_crc_flag())
            .field("is-ack", &self.is_ack())
            .field("is-persist", &self.is_persist())
            .field("is-defer", &self.is_defer())
            .field("is-defer-concrete", &self.is_defer_concrete())
            .field("is-notready", &self.is_notready())
            .field("is-comsumed", &self.is_notready())
            .field("id-len", &self.id_len())
            .field("body-len", &self.body_len())
            .finish()
    }
}

impl MessageUserV1Head {
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

    fn with(head: [u8; MESSAGE_USER_V1_HEAD_LENGTH]) -> Self {
        MessageUserV1Head(head)
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }
    // ack flag
    pub fn is_ack(&self) -> bool {
        self.0[0].is_1(6)
    }

    /// set the ack flag value.
    pub fn set_ack(&mut self, ack: bool) -> &mut Self {
        self.set_head_flag(0, 6, ack);
        self
    }

    /// persist flag.
    pub fn is_persist(&self) -> bool {
        self.0[0].is_1(5)
    }

    /// set the persist flag value.
    pub fn set_persist(&mut self, persist: bool) -> &mut Self {
        self.set_head_flag(0, 5, persist);
        self
    }

    /// defer flag.
    pub fn is_defer(&self) -> bool {
        self.0[0].is_1(4)
    }

    /// set the defer flag value.
    fn set_defer(&mut self, defer: bool) -> &mut Self {
        self.set_head_flag(0, 4, defer);
        self
    }

    /// defer_concrete flag.
    pub fn is_defer_concrete(&self) -> bool {
        self.0[0].is_1(3)
    }

    /// set the defer_concrete flag value.
    fn set_defer_concrete(&mut self, concrete: bool) -> &mut Self {
        self.set_head_flag(0, 3, concrete);
        self
    }

    /// notready flag
    pub fn is_notready(&self) -> bool {
        self.0[0].is_1(2)
    }

    /// set the notready flag value.
    pub fn set_notready(&mut self, notready: bool) -> &mut Self {
        self.set_head_flag(0, 2, notready);
        self
    }

    /// consumed flag
    pub fn is_consumed(&self) -> bool {
        self.0[0].is_1(1)
    }

    /// set the consumed flag value.
    fn set_consumed(&mut self, consumed: bool) -> &mut Self {
        self.set_head_flag(0, 1, consumed);
        self
    }

    /// deleted flag
    pub fn is_deleted(&self) -> bool {
        self.0[0].is_1(0)
    }

    /// set the deleted flag value.
    fn set_deleted(&mut self, deleted: bool) -> &mut Self {
        self.set_head_flag(0, 0, deleted);
        self
    }

    pub fn id_len(&self) -> u8 {
        self.0[1]
    }

    fn set_id_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }

    pub fn body_len(&self) -> u64 {
        u64::from_be_bytes(
            self.0[2..MESSAGE_USER_V1_HEAD_LENGTH - 2]
                .to_vec()
                .try_into()
                .expect("get body length failed"),
        )
    }

    fn set_body_len(&mut self, l: u64) -> &mut Self {
        // set the body length in head
        let body_len = l.to_be_bytes();
        let mut pos = 2;
        for v in body_len {
            self.0[pos] = v;
            pos += 1;
        }
        self
    }
}

/// The unit that store into storage
#[derive(Default, Clone, Debug)]
pub struct MessageUserV1 {
    head: MessageUserV1Head,
    sid: i64, // session_id: generate by ProtocolBodys

    crc: u16,
    defer_time: u64, // 8 bytes
    id: String,
    body: Bytes,
}

impl Deref for MessageUserV1 {
    type Target = MessageUserV1Head;

    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

/// user can custom the message head
impl DerefMut for MessageUserV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.head
    }
}

impl MessageUserV1 {
    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut msg = self.clone();
        let dst_crc = msg.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }

        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.0.to_vec());
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.is_defer() {
            res.extend(self.defer_time.to_be_bytes());
        }
        if self.id_len() != 0 {
            res.extend(self.id.as_bytes());
        }
        if self.body_len() != 0 {
            res.extend(self.body.to_vec());
        }

        res
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.head.set_crc_flag(false);
        self.crc = X25.checksum(&self.as_bytes());
        self.head.set_crc_flag(true);
        self
    }

    pub fn get_defer_time(&self) -> u64 {
        self.defer_time
    }

    pub fn set_defer_time(&mut self, defer_time: u64) -> &mut Self {
        self.defer_time = defer_time;
        self.head.set_defer(true);
        self.head.set_defer_concrete(true);
        self
    }

    pub fn set_defer_time_offset(&mut self, offset: u64) -> &mut Self {
        self.defer_time = Local::now().timestamp() as u64 + offset;
        self.head.set_defer(true);
        self
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn set_id(&mut self, id: &str) -> Result<()> {
        if id.len() >= u8::MAX as usize {
            return Err(anyhow!("id excess the max length"));
        }
        self.id = id.to_string();
        self.head.set_id_len(self.id.len() as u8);
        Ok(())
    }

    pub fn get_body(&self) -> Bytes {
        self.body.clone()
    }

    pub fn set_body(&mut self, body: Bytes) -> &mut Self {
        self.head.set_body_len(body.len() as u64);
        self.body = body;
        self
    }

    pub fn convert_to_messagev1(self, topic_name: &str, channel_name: &str) -> MessageV1 {
        let mut head = Head::default();
        head.set_version(PROPTOCOL_V1).set_action(ACTION_MSG);
        MessageV1 {
            head,
            topic: topic_name.to_string(),
            channel: channel_name.to_string(),
            msg: self,
        }
    }

    pub async fn parse_from(
        reader: &mut Pin<&mut impl AsyncReadExt>,
        mut num: u8,
    ) -> Result<Vec<Self>> {
        let mut res = vec![];
        while num != 0 {
            num -= 1;
            let mut msg = MessageUserV1::default();
            msg.parse_reader(reader).await?;
            res.push(msg);
        }
        Ok(res)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse head
        buf.resize(MESSAGE_USER_V1_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.head = MessageUserV1Head::with(
            buf.to_vec()
                .try_into()
                .expect("convert to bin message-user-v1-head failed"),
        );

        // parse crc
        if self.has_crc_flag() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            self.crc = u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc failed"));
        }

        // parse defer time
        if self.is_defer() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            let defer_time = u64::from_be_bytes(
                buf.to_vec()
                    .try_into()
                    .expect("convert to defer time failed"),
            );
            if self.is_defer_concrete() {
                self.defer_time = defer_time;
            } else {
                let dt = Local::now();
                self.defer_time = dt.timestamp() as u64 + defer_time;
            }
        }

        // parse id
        let id_len = self.id_len();
        if id_len != 0 {
            buf.resize(id_len as usize, 0);
            reader.read_exact(&mut buf).await?;
            self.id = String::from_utf8(buf.to_vec()).expect("illigal id value");
        }

        // parse body
        let body_len = self.body_len();
        if body_len != 0 {
            buf.resize(body_len as usize, 0);
            reader.read_exact(&mut buf).await?;
            self.body = Bytes::copy_from_slice(buf.as_ref());
        }

        Ok(())
    }
}
