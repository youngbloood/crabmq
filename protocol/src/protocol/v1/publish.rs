use super::new_v1_head;
use super::reply::Reply;
use super::reply::ReplyBuilder;
use super::BuilderV1;
use super::Head;
use super::ProtError;
use super::E_BAD_CRC;
use super::V1;
use super::X25;
use crate::consts::ACTION_PUBLISH;
use crate::consts::CRC_LENGTH;
use crate::message::v1::MessageUserV1;
use crate::message::v1::MessageV1;
use crate::message::Message;
use crate::protocol::Builder;
use crate::protocol::Protocol;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation, BitOperation as _};
use std::fmt::Debug;
use std::{ops::Deref, pin::Pin};
use tokio::io::AsyncReadExt;

const PUBLISH_HEAD_LENGTH: usize = 6;

/**
 * CONST [`PUBLISH_HEAD_LENGTH`] length head info:
 * 1st bytes:
 *          1 bit: is heartbeat
 *          1 bit: has crc
 *          1 bit: is ephemeral
 *          1 bit: *reserve bit*
 *          4 bits: msg number
 * 2-4 bytes: reserve bytes
 * 5th byte: topic length
 * 6th byte: token length
 *
 * EXTEND according the head:
 *          [`CRC_LENGTH`] bytes: crc value
 *          n bytes: topic name
 *          n bytes: token value
 */
#[derive(Default, Clone)]
pub struct PublishHead([u8; PUBLISH_HEAD_LENGTH]);

impl Debug for PublishHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishHead")
            .field("is-heartbeat", &self.is_heartbeat())
            .field("has-crc", &self.has_crc_flag())
            .field("is-ephemeral", &self.is_ephemeral())
            .field("msg-num", &self.msg_num())
            .field("topic-len", &self.get_topic_len())
            .field("token-len", &self.get_token_len())
            .finish()
    }
}

impl PublishHead {
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

    pub fn with(head: [u8; PUBLISH_HEAD_LENGTH]) -> Self {
        PublishHead(head)
    }

    pub fn is_heartbeat(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_heartbeat(&mut self, hb: bool) -> &mut Self {
        self.set_head_flag(0, 7, hb);
        self
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(6)
    }

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 6, has);
        self
    }

    pub fn is_ephemeral(&self) -> bool {
        self.0[0].is_1(5)
    }

    fn set_ephemeral(&mut self, epehemral: bool) -> &mut Self {
        self.set_head_flag(0, 5, epehemral);
        self
    }

    pub fn msg_num(&self) -> u8 {
        self.0[0] & 0b00001111
    }

    fn set_msg_num(&mut self, num: u8) -> Result<()> {
        if num > 0b00001111 {
            return Err(anyhow!("num exceed maxnuim message number"));
        }
        let src_num = self.0[0];
        self.0[0] = (src_num >> 4 << 4) | num;

        Ok(())
    }

    pub fn get_topic_len(&self) -> u8 {
        self.0[4]
    }

    fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[4] = l;
        self
    }

    pub fn get_token_len(&self) -> u8 {
        self.0[5]
    }

    fn set_token_len(&mut self, l: u8) -> &mut Self {
        self.0[5] = l;
        self
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

#[derive(Clone, Debug)]
pub struct Publish {
    head: Head,
    pub_head: PublishHead,

    crc: u16,
    topic: String,
    token: String,

    msgs: Vec<MessageUserV1>,
}

// impl Debug for Publish {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("PublishHead")
//             .field("head", &self.head)
//             .field("pub_head", &self.pub_head)
//             .field("crc", &self.crc)
//             .field("topic", &self.topic)
//             .field("token", &self.token)
//             .field("msgs", &self.msgs)
//             .finish()
//     }
// }

impl Default for Publish {
    fn default() -> Self {
        let topic_name = "default";
        let mut pub_head = PublishHead::default();
        pub_head.set_topic_len(topic_name.len() as _);
        Self {
            head: new_v1_head(ACTION_PUBLISH),
            pub_head,
            crc: 0,
            topic: topic_name.to_string(),
            token: Default::default(),
            msgs: Default::default(),
        }
    }
}

impl Deref for Publish {
    type Target = PublishHead;

    fn deref(&self) -> &Self::Target {
        &self.pub_head
    }
}

impl Builder for Publish {
    fn build(self) -> Protocol {
        let mut v1 = V1::default();
        v1.set_publish(self);
        Protocol::V1(v1)
    }
}

impl BuilderV1 for Publish {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        v1.set_publish(self);
        v1
    }
}

impl ReplyBuilder for Publish {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_PUBLISH)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_PUBLISH, err_code)
    }
}

impl Publish {
    pub fn split_message(&self) -> Vec<Message> {
        let mut res = Vec::with_capacity(self.msgs.len());
        for v in &self.msgs {
            let mut msgv1 = MessageV1::default();
            msgv1
                .set_head(self.head.clone())
                .set_topic(&self.topic)
                .set_message(v.clone());
            res.push(Message::V1(msgv1));
        }
        res
    }

    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();

        let mut publish = self.clone();
        let dst_crc = publish.calc_crc().get_crc();
        if dst_crc != src_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }
        Ok(())
    }

    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn get_pub_head(&self) -> PublishHead {
        self.pub_head.clone()
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.pub_head.set_crc_flag(false);
        let bts = self.as_bytes();
        self.crc = X25.checksum(&bts);
        self.pub_head.set_crc_flag(true);

        self
    }

    pub fn set_ephemeral(&mut self, e: bool) -> &mut Self {
        self.pub_head.set_ephemeral(e);
        self
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) -> Result<()> {
        if topic.len() > u8::MAX as usize {
            return Err(anyhow!("topic length excess the u8::MAX"));
        }
        self.topic = topic.to_string();
        self.pub_head.set_topic_len(topic.len() as _);
        Ok(())
    }

    pub fn get_token(&self) -> &str {
        &self.token
    }

    pub fn set_token(&mut self, token: &str) -> Result<()> {
        if token.len() > u8::MAX as usize {
            return Err(anyhow!("topic length excess the u8::MAX"));
        }
        self.token = token.to_string();
        self.pub_head.set_token_len(token.len() as _);
        Ok(())
    }

    pub fn get_msgs(&self) -> &Vec<MessageUserV1> {
        &self.msgs
    }

    pub fn set_msgs(&mut self, msgs: Vec<MessageUserV1>) -> Result<()> {
        self.pub_head.set_msg_num(msgs.len() as u8)?;
        self.msgs = msgs;
        Ok(())
    }

    pub fn push_msg(&mut self, msg: MessageUserV1) -> Result<()> {
        if self.msg_num() >= 16 {
            return Err(anyhow!(
                "can't push any message cause the max message number is 16"
            ));
        }
        self.msgs.push(msg);
        let old_num = self.msg_num();
        let _ = self.pub_head.set_msg_num(old_num + 1);
        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.pub_head.as_bytes());
        if self.has_crc_flag() {
            res.extend(self.get_crc().to_be_bytes());
        }
        if self.get_topic_len() != 0 {
            res.extend(self.topic.as_bytes());
        }
        if self.get_token_len() != 0 {
            res.extend(self.token.as_bytes());
        }
        for msg in &self.msgs {
            res.extend(msg.as_bytes());
        }
        res
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut publish = Publish::default();
        publish.parse_reader(reader).await?;
        Ok(publish)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        println!("parse publish");
        // parse pub-head
        buf.resize(PUBLISH_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.pub_head = PublishHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to publish head failed"),
        );

        // parse crc
        if self.has_crc_flag() {
            buf.resize(CRC_LENGTH, 0);
            reader.read_exact(&mut buf).await?;
            self.crc =
                u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc vec failed"));
        }

        // parse topic
        if self.get_topic_len() != 0 {
            buf.resize(self.get_topic_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.topic = String::from_utf8(buf.to_vec())?;
        }

        // parse token
        if self.get_token_len() != 0 {
            buf.resize(self.get_token_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.token = String::from_utf8(buf.to_vec())?;
        }

        // parse bin message
        self.set_msgs(MessageUserV1::parse_from(reader, self.msg_num()).await?)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
