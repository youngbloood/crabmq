use std::{ops::Deref, pin::Pin};

use super::common_reply::Reply;
use super::common_reply::ReplyBuilder;
use super::BuilderV1;
use super::V1;
use super::{bin_message::BinMessage, Head};
use crate::message::v1::MessageV1;
use crate::message::Message;
use crate::protocol::v1::ACTION_PUBLISH;
use crate::protocol::Builder;
use crate::protocol::Protocol;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation, BitOperation as _};
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
 *          2 bytes: crc value
 *          n bytes: topic name
 *          n bytes: token value
 */
#[derive(Default, Clone, Debug)]
pub struct PublishHead([u8; PUBLISH_HEAD_LENGTH]);

impl PublishHead {
    fn set_flag(&mut self, index: usize, pos: u8, on: bool) {
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
        self.set_flag(0, 7, hb);
        self
    }

    pub fn has_crc(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_crc(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 6, has);
        self
    }

    pub fn is_ephemeral(&self) -> bool {
        self.0[0].is_1(5)
    }

    pub fn set_ephemeral(&mut self, epehemral: bool) -> &mut Self {
        self.set_flag(0, 5, epehemral);
        self
    }

    pub fn msg_num(&self) -> u8 {
        self.0[0] & 0b00001111
    }

    pub fn set_msg_num(&mut self, num: u8) -> Result<()> {
        if num > 0b00001111 {
            return Err(anyhow!("num exceed maxnuim message number"));
        }
        self.0[0] |= num;

        Ok(())
    }

    pub fn get_topic_len(&self) -> u8 {
        self.0[4]
    }

    pub fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[4] = l;
        self
    }

    pub fn get_token_len(&self) -> u8 {
        self.0[5]
    }

    pub fn set_token_len(&mut self, l: u8) -> &mut Self {
        self.0[5] = l;
        self
    }
}

#[derive(Default, Clone, Debug)]
pub struct Publish {
    pub head: Head,
    pub_head: PublishHead,

    crc: u16,
    topic: String,
    token: String,

    msgs: Vec<BinMessage>,
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
        v1.set_head(self.head.clone()).set_publish(self);
        Protocol::V1(v1)
    }
}

impl BuilderV1 for Publish {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_publish(self);
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

    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_pub_head(&self) -> PublishHead {
        self.pub_head.clone()
    }

    pub fn set_pub_head(&mut self, head: PublishHead) -> &mut Self {
        self.pub_head = head;
        self
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn set_crc(&mut self, crc: u16) -> &mut Self {
        self.crc = crc;
        self
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) -> &mut Self {
        self.topic = topic.to_string();
        self
    }

    pub fn get_token(&self) -> &str {
        &self.token
    }

    pub fn set_token(&mut self, token: &str) -> &mut Self {
        self.token = token.to_string();
        self
    }

    pub fn get_msgs(&self) -> &str {
        &self.token
    }

    pub fn set_msgs(&mut self, msgs: Vec<BinMessage>) -> &mut Self {
        self.msgs = msgs;
        self
    }

    pub fn push_msg(&mut self, msg: BinMessage) -> &mut Self {
        self.msgs.push(msg);
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut buf = BytesMut::new();
        buf.resize(PUBLISH_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;

        let pub_head = PublishHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to publish head failed"),
        );

        let mut publish = Publish::default();
        publish.set_head(head).set_pub_head(pub_head.clone());

        // parse crc
        if pub_head.has_crc() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            publish.set_crc(u16::from_be_bytes(
                buf.to_vec().try_into().expect("convert to crc vec failed"),
            ));
        }

        // parse topic
        if pub_head.get_topic_len() != 0 {
            buf.resize(pub_head.get_topic_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            publish.set_topic(&String::from_utf8(buf.to_vec())?);
        }

        // parse token
        if pub_head.get_token_len() != 0 {
            buf.resize(pub_head.get_token_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            publish.set_token(&String::from_utf8(buf.to_vec())?);
        }

        // parse bin message
        publish.set_msgs(BinMessage::parse_from(reader, pub_head.msg_num()).await?);

        Ok(publish)
    }
}
