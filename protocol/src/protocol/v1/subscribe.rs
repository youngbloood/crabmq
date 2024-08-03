use std::{ops::Deref, pin::Pin};

use crate::protocol::{Builder, Protocol};

use super::{
    common_reply::{Reply, ReplyBuilder},
    BuilderV1, Head, ACTION_SUBSCRIBE, V1,
};
use anyhow::Result;
use bytes::BytesMut;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use tokio::io::AsyncReadExt;

const SUBSCRIBE_HEAD_LENGTH: usize = 6;

/**
 * CONST [`PUBLISH_HEAD_LENGTH`] length head info:
 * 1st bytes:
 *          1 bit: is heartbeat
 *          1 bit: has crc
 *          1 bit: is ephemeral
 *          1 bit: set [`ready_number`]
 *          4 bits: msg number
 * 2-3 bytes: reserve bytes
 * 4th byte: topic length
 * 5th byte: channel length
 * 6th byte: token length
 *
 * EXTEND according the head:
 *          2 bytes: crc value
 *          2 bytes: u16: ready number
 *          n bytes: topic name
 *          n bytes: channel name
 *          n bytes: token value
 */
#[derive(Default, Clone, Debug)]
pub struct SubscribeHead([u8; SUBSCRIBE_HEAD_LENGTH]);

impl SubscribeHead {
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

    pub fn with(head: [u8; SUBSCRIBE_HEAD_LENGTH]) -> Self {
        SubscribeHead(head)
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

    pub fn has_ready_number(&self) -> bool {
        self.0[0].is_1(4)
    }

    pub fn set_ready_number(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 4, has);
        self
    }

    pub fn get_topic_len(&self) -> u8 {
        self.0[3]
    }

    pub fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[3] = l;
        self
    }

    pub fn get_channel_len(&self) -> u8 {
        self.0[4]
    }

    pub fn set_channel_len(&mut self, l: u8) -> &mut Self {
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

#[derive(Clone, Debug)]
pub struct Subscribe {
    head: Head,
    sub_head: SubscribeHead,

    crc: u16,
    ready_number: u16,
    topic: String,
    channel: String,
    token: String,
}

impl Default for Subscribe {
    fn default() -> Self {
        let mut sub_head = SubscribeHead::default();
        let topic = "default".to_string();
        let channel = "default".to_string();
        sub_head
            .set_token_len(topic.len() as _)
            .set_channel_len(channel.len() as _)
            .set_ready_number(true);

        Self {
            head: Default::default(),
            sub_head: Default::default(),
            crc: Default::default(),
            ready_number: 1,
            topic,
            channel,
            token: Default::default(),
        }
    }
}

impl Deref for Subscribe {
    type Target = SubscribeHead;

    fn deref(&self) -> &Self::Target {
        &self.sub_head
    }
}

impl Builder for Subscribe {
    fn build(self) -> Protocol {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_subscribe(self);
        Protocol::V1(v1)
    }
}

impl BuilderV1 for Subscribe {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_subscribe(self);
        v1
    }
}

impl ReplyBuilder for Subscribe {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_SUBSCRIBE)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_SUBSCRIBE, err_code)
    }
}

impl Subscribe {
    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_sub_head(&self) -> SubscribeHead {
        self.sub_head.clone()
    }

    pub fn set_sub_head(&mut self, head: SubscribeHead) -> &mut Self {
        self.sub_head = head;
        self
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn set_crc(&mut self, crc: u16) -> &mut Self {
        self.crc = crc;
        self
    }

    pub fn get_ready_number(&self) -> u16 {
        self.ready_number
    }

    pub fn set_ready_number(&mut self, rn: u16) -> &mut Self {
        self.ready_number = rn;
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

    pub fn get_token(&self) -> &str {
        &self.token
    }

    pub fn set_token(&mut self, token: &str) -> &mut Self {
        self.token = token.to_string();
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut buf = BytesMut::new();
        buf.resize(SUBSCRIBE_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;

        let sub_head = SubscribeHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to publish head failed"),
        );

        let mut subscribe = Subscribe::default();
        subscribe.set_head(head).set_sub_head(sub_head.clone());

        // parse crc
        if sub_head.has_crc() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            subscribe.set_crc(u16::from_be_bytes(
                buf.to_vec().try_into().expect("convert to crc vec failed"),
            ));
        }

        // parse ready-number
        if sub_head.has_ready_number() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            subscribe.set_ready_number(u16::from_be_bytes(
                buf.to_vec()
                    .try_into()
                    .expect("convert to ready-number vec failed"),
            ));
        }

        // parse topic
        if sub_head.get_topic_len() != 0 {
            buf.resize(sub_head.get_topic_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            subscribe.set_topic(&String::from_utf8(buf.to_vec())?);
        }

        // parse channel
        if sub_head.get_channel_len() != 0 {
            buf.resize(sub_head.get_channel_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            subscribe.set_channel(&String::from_utf8(buf.to_vec())?);
        }

        // parse token
        if sub_head.get_token_len() != 0 {
            buf.resize(sub_head.get_token_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            subscribe.set_token(&String::from_utf8(buf.to_vec())?);
        }

        Ok(subscribe)
    }
}
