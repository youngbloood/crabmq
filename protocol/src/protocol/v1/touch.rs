use super::{
    new_v1_head,
    reply::{Reply, ReplyBuilder},
    BuilderV1, E_BAD_CRC, E_TOPIC_PROHIBIT_TYPE, V1, X25,
};
use crate::{
    consts::ACTION_TOUCH,
    protocol::{Builder, Head, Protocol},
};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::{fmt::Debug, ops::Deref, pin::Pin};
use tokio::io::AsyncReadExt;
use tracing::debug;

pub const TOUCH_HEAD_LENGTH: usize = 8;
/**
* ### FIXED HEAD LENGTH([[`PROTOCOL_HEAD_LEN`] bytes):
* HEAD: 10 bytes:
* 1nd byte: global flags:
*           1bit: topic is ephemeral: true mean the topic is ephemeral, it will be delete then there is no publishers.
*           1bit: channel is ephemeral: true mean the channel is ephemeral, it will be delete then there is no subcribers.
*           1bit: is heartbeat.
*           1bit: prohibit send instant message (default is 0).
*           1bit: prohibit send defer message (default is 0).
*           1bit: has crc.
*           2bit: reserve bit.
* 2rd byte: topic flags
*           1bit: set [`max_msg_num_per_file`].
*           1bit: set [`max_size_per_file`].
*           1bit: set [`compress_type`].
*           1bit: set [`subscribe_type`].
*           1bit: set [`record_num_per_file`].
*           1bit: set [`record_size_per_file`].
*           1bit: set [`fd_cache_size`].
* 3th byte: topic length.
* 4th byte: channel length.
* 5th byte: token length.
* 6th byte: custom defer message store format length.
* 7-8th byte: reserve bytes.
*
* optional:
*           2 bytes crc values
*           8 bytes [`max_msg_num_per_file`]
*           8 bytes [`max_size_per_file`]
*           1 byte [`compress_type`]
*           1 byte [`subscribe_type`], front 4 bits is subscribe-type-in-channel, back 4 bits is subscribe-type-in-client
*           8 bytes [`record_num_per_file`]
*           8 bytes [`record_size_per_file`]
*           8 bytes [`fd_cache_size`]
*           topic value.
*           channel value.
*           token value.
*           custom defer message store format.
*/
#[derive(Default, Clone)]
pub struct TouchHead([u8; TOUCH_HEAD_LENGTH]);

impl Debug for TouchHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TouchHead")
            .field("topic-is-ephemeral", &self.topic_is_ephemeral())
            .field("channel-is-ephemeral", &self.channel_is_ephemeral())
            .field("is-heartbeat", &self.is_heartbeat())
            .field("prohibit-instant", &self.prohibit_instant())
            .field("prohibit-defer", &self.prohibit_defer())
            .field("has-crc", &self.has_crc_flag())
            .field("has-max-msg-num-per-file", &self.has_max_msg_num_per_file())
            .field("has-max-size-per-file", &self.has_max_size_per_file())
            .field("has-compress-type", &self.has_compress_type())
            .field("has-subscribe-type", &self.has_subscribe_type())
            .field("has-record-num-per-file", &self.has_record_num_per_file())
            .field("has-record-size-per-file", &self.has_record_size_per_file())
            .field("has-fd-cache-size", &self.has_fd_cache_size())
            .field("topic-len", &self.get_token_len())
            .field("channel-len", &self.get_channel_len())
            .field("token-len", &self.get_token_len())
            .field("defer-msg-format-len", &self.defer_format_len())
            .finish()
    }
}

impl TouchHead {
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

    pub fn with(head: [u8; TOUCH_HEAD_LENGTH]) -> Self {
        TouchHead(head)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    // 1st byte ======================================== START
    pub fn topic_is_ephemeral(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_topic_is_ephemeral(&mut self, e: bool) -> &mut Self {
        self.set_head_flag(0, 7, e);
        self
    }

    pub fn channel_is_ephemeral(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_channel_is_ephemerall(&mut self, e: bool) -> &mut Self {
        self.set_head_flag(0, 6, e);
        self
    }

    pub fn is_heartbeat(&self) -> bool {
        self.0[0].is_1(5)
    }

    pub fn set_heartbeat(&mut self, hb: bool) -> &mut Self {
        self.set_head_flag(0, 5, hb);
        self
    }

    pub fn prohibit_instant(&self) -> bool {
        self.0[0].is_1(4)
    }

    pub fn set_prohibit_instant(&mut self, p: bool) -> &mut Self {
        self.set_head_flag(0, 4, p);
        self
    }

    pub fn prohibit_defer(&self) -> bool {
        self.0[0].is_1(3)
    }

    pub fn set_prohibit_defer(&mut self, p: bool) -> &mut Self {
        self.set_head_flag(0, 3, p);
        self
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(2)
    }

    pub fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 2, has);
        self
    }
    // 1st byte ======================================== END

    // 2nd byte ======================================== START
    pub fn has_max_msg_num_per_file(&self) -> bool {
        self.0[1].is_1(7)
    }

    pub fn set_max_msg_num_per_file(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 7, has);
        self
    }

    pub fn has_max_size_per_file(&self) -> bool {
        self.0[1].is_1(6)
    }

    pub fn set_max_size_per_file(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 6, has);
        self
    }

    pub fn has_compress_type(&self) -> bool {
        self.0[1].is_1(5)
    }

    pub fn set_compress_type(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 5, has);
        self
    }

    pub fn has_subscribe_type(&self) -> bool {
        self.0[1].is_1(4)
    }

    pub fn set_subscribe_type(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 4, has);
        self
    }

    pub fn has_record_num_per_file(&self) -> bool {
        self.0[1].is_1(3)
    }

    pub fn set_record_num_per_file(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 3, has);
        self
    }

    pub fn has_record_size_per_file(&self) -> bool {
        self.0[1].is_1(2)
    }

    pub fn set_record_size_per_file(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 2, has);
        self
    }

    pub fn has_fd_cache_size(&self) -> bool {
        self.0[1].is_1(1)
    }

    pub fn set_fd_cache_size(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(1, 1, has);
        self
    }
    // 2nd byte ======================================== END

    // 3rd byte ======================================== START
    pub fn topic_len(&self) -> u8 {
        self.0[2]
    }

    pub fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[2] = l;
        self
    }
    // 3rd byte ======================================== END

    // 4th byte ======================================== START
    pub fn get_channel_len(&self) -> u8 {
        self.0[3]
    }

    pub fn set_channel_len(&mut self, l: u8) -> &mut Self {
        self.0[3] = l;
        self
    }
    // 4th byte ======================================== END

    // 5th byte ======================================== START
    pub fn get_token_len(&self) -> u8 {
        self.0[4]
    }
    pub fn set_token_len(&mut self, l: u8) -> &mut Self {
        self.0[4] = l;
        self
    }
    // 5th byte ======================================== END

    // 6th byte ======================================== START
    pub fn defer_format_len(&self) -> u8 {
        self.0[5]
    }

    pub fn set_defer_format_len(&mut self, l: u8) -> &mut Self {
        self.0[5] = l;
        self
    }
    // 6th byte ======================================== END
}

#[derive(Debug, Clone)]
pub struct Touch {
    head: Head,
    touch_head: TouchHead,

    topic: String,
    channel: String,
    token: String,

    // optional
    crc: u16,
    max_msg_num_per_file: u64,
    max_size_per_file: u64,
    compress_type: u8,
    subscribe_type: u8,
    record_num_per_file: u64,
    record_size_per_file: u64,
    fd_cache_size: u64,
    defer_msg_format: String,
}

impl Default for Touch {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_TOUCH),
            touch_head: TouchHead::default(),
            topic: String::new(),
            channel: String::new(),
            token: String::new(),
            crc: 0,
            max_msg_num_per_file: 0,
            max_size_per_file: 0,
            compress_type: 0,
            subscribe_type: 0,
            record_num_per_file: 0,
            record_size_per_file: 0,
            fd_cache_size: 0,
            defer_msg_format: String::new(),
        }
    }
}

impl Deref for Touch {
    type Target = TouchHead;

    fn deref(&self) -> &Self::Target {
        &self.touch_head
    }
}

impl Builder for Touch {
    fn build(self) -> Protocol {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_touch(self);
        Protocol::V1(v1)
    }
}

impl BuilderV1 for Touch {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_touch(self);
        v1
    }
}

impl ReplyBuilder for Touch {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_TOUCH)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_TOUCH, err_code)
    }
}

impl Touch {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.touch_head.as_bytes());
        if self.get_token_len() != 0 {
            res.extend(self.topic.as_bytes());
        }
        if self.get_channel_len() != 0 {
            res.extend(self.channel.as_bytes());
        }
        if self.get_token_len() != 0 {
            res.extend(self.token.as_bytes());
        }
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.has_max_msg_num_per_file() {
            res.extend(self.max_msg_num_per_file.to_be_bytes());
        }
        if self.has_max_size_per_file() {
            res.extend(self.max_size_per_file.to_be_bytes());
        }
        if self.has_compress_type() {
            res.push(self.compress_type);
        }
        if self.has_subscribe_type() {
            res.push(self.subscribe_type);
        }
        if self.has_record_num_per_file() {
            res.extend(self.record_num_per_file.to_be_bytes());
        }
        if self.has_record_size_per_file() {
            res.extend(self.record_size_per_file.to_be_bytes());
        }
        if self.has_fd_cache_size() {
            res.extend(self.fd_cache_size.to_be_bytes());
        }
        if self.defer_format_len() != 0 {
            res.extend(self.defer_msg_format.as_bytes());
        }
        res
    }

    pub fn validate(&self) -> Option<Protocol> {
        if !self.has_crc_flag() {
            return None;
        }
        let src_crc = self.get_crc();
        let mut touch = self.clone();
        let dst_crc = touch.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Some(self.build_reply_err(E_BAD_CRC).build());
        }
        if self.prohibit_defer() && self.prohibit_instant() {
            return Some(self.build_reply_err(E_TOPIC_PROHIBIT_TYPE).build());
        }

        None
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.touch_head.set_crc_flag(false);
        let bts = self.as_bytes();
        self.crc = X25.checksum(&bts);
        self.touch_head.set_crc_flag(true);

        self
    }

    pub fn set_defer_format(&mut self, fmt: &str) -> Result<()> {
        if fmt.len() > u8::MAX as usize {
            return Err(anyhow!("exceed the max the u8"));
        }
        self.touch_head.set_defer_format_len(fmt.len() as u8);
        self.defer_msg_format = fmt.to_string();

        Ok(())
    }

    pub fn topic(&self) -> &str {
        let tp = self.topic.as_str();
        if tp.is_empty() {
            return "default";
        }
        tp
    }

    pub fn set_topic(&mut self, topic: &str) -> Result<()> {
        if topic.len() > u8::MAX as usize {
            return Err(anyhow!("topic len exceed max length 256"));
        }
        self.topic = topic.to_string();
        self.touch_head.set_topic_len(topic.len() as u8);
        Ok(())
    }

    pub fn channel(&self) -> &str {
        let chan = self.channel.as_str();
        if chan.is_empty() {
            return "default";
        }
        chan
    }

    pub fn set_channel(&mut self, channel: &str) -> Result<()> {
        if channel.len() > u8::MAX as usize {
            return Err(anyhow!("channel len exceed max length 256"));
        }
        self.channel = channel.to_string();
        self.touch_head.set_channel_len(channel.len() as u8);
        Ok(())
    }

    pub fn set_token(&mut self, token: &str) -> Result<()> {
        if token.len() > u8::MAX as usize {
            return Err(anyhow!("token len exceed max length 256"));
        }
        self.token = token.to_string();
        self.touch_head.set_token_len(token.len() as u8);
        Ok(())
    }

    pub fn get_max_msg_num_per_file(&self) -> u64 {
        self.max_msg_num_per_file
    }

    pub fn set_max_msg_num_per_file(&mut self, num: u64) {
        self.max_msg_num_per_file = num;
        self.touch_head.set_max_msg_num_per_file(true);
    }

    pub fn get_max_size_per_file(&self) -> u64 {
        self.max_size_per_file
    }

    pub fn set_max_size_per_file(&mut self, num: u64) {
        self.max_size_per_file = num;
        self.touch_head.set_max_size_per_file(true);
    }

    pub fn get_compress_type(&self) -> u8 {
        self.compress_type
    }

    pub fn set_compress_type(&mut self, t: u8) {
        self.compress_type = t;
        self.touch_head.set_compress_type(true);
    }

    pub fn get_subscribe_type(&self) -> u8 {
        self.subscribe_type
    }

    pub fn set_subscribe_type(&mut self, t: u8) {
        self.subscribe_type = t;
        self.touch_head.set_subscribe_type(true);
    }

    pub fn get_record_num_per_file(&self) -> u64 {
        self.record_num_per_file
    }

    pub fn set_record_num_per_file(&mut self, num: u64) {
        self.record_num_per_file = num;
        self.touch_head.set_record_num_per_file(true);
    }

    pub fn get_record_size_per_file(&self) -> u64 {
        self.record_size_per_file
    }

    pub fn set_record_size_per_file(&mut self, num: u64) {
        self.record_size_per_file = num;
        self.touch_head.set_record_size_per_file(true);
    }

    pub fn get_fd_cache_size(&self) -> u64 {
        self.fd_cache_size
    }

    pub fn set_fd_cache_size(&mut self, num: u64) {
        self.fd_cache_size = num;
        self.touch_head.set_fd_cache_size(true);
    }

    pub fn get_defer_msg_format(&self) -> &str {
        self.defer_msg_format.as_str()
    }

    pub fn set_defer_msg_format(&mut self, fmt: &str) -> Result<()> {
        if fmt.len() > u8::MAX as usize {
            return Err(anyhow!("too long defer format"));
        }
        self.defer_msg_format = fmt.to_string();
        self.touch_head.set_defer_format_len(fmt.len() as u8);
        Ok(())
    }

    /// [`parse_from`] read the protocol head from bts.
    pub async fn parse_from(fd: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut touch = Self::default();
        // touch.set_head(head);
        touch.read_parse(fd).await?;
        Ok(touch)
    }

    /// [`read_parse`] read the protocol head from reader.
    pub async fn read_parse(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse touch head
        buf.resize(TOUCH_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.touch_head = TouchHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to touch head failed"),
        );

        // parse crc
        if self.has_crc_flag() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            self.crc =
                u16::from_be_bytes(buf.to_vec().try_into().expect("convert to 2 bytes failed"));
        }

        // parse [`max_msg_num_per_file`]
        if self.has_max_msg_num_per_file() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.max_msg_num_per_file =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to 8 bytes failed"));
        }

        // parse [`max_size_per_file`]
        if self.has_max_size_per_file() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.max_size_per_file =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to 8 bytes failed"));
        }

        // parse compress_type
        if self.has_compress_type() {
            buf.resize(1, 0);
            reader.read_exact(&mut buf).await?;
            self.compress_type =
                u8::from_be_bytes(buf.to_vec().try_into().expect("convert to 1 bytes failed"))
        }

        // parse subscribe_type
        if self.has_subscribe_type() {
            buf.resize(1, 0);
            reader.read_exact(&mut buf).await?;
            self.subscribe_type =
                u8::from_be_bytes(buf.to_vec().try_into().expect("convert to 1 bytes failed"))
        }

        // parse [`record_num_per_file`]
        if self.has_record_num_per_file() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.record_num_per_file =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to 8 bytes failed"));
        }

        // parse [`record_size_per_file`]
        if self.has_record_size_per_file() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.record_size_per_file =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to 8 bytes failed"));
        }
        // parse [`fd_cache_size`]
        if self.has_fd_cache_size() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.fd_cache_size =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to 8 bytes failed"));
        }

        // parse topic name
        buf.resize(self.topic_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let topic: String = String::from_utf8(buf.to_vec()).expect("illigal topic name");
        self.set_topic(topic.as_str())?;

        debug!(addr = "{self.addr:?}", "parse channel name");

        // parse channel name
        buf.resize(self.get_channel_len() as usize, 0);
        reader.read_exact(&mut buf).await?;
        let channel = String::from_utf8(buf.to_vec()).expect("illigal channel name");
        self.set_channel(channel.as_str())?;

        debug!(addr = "{self.addr:?}", "parse token");
        // parse token
        buf.resize(self.get_token_len() as usize, 0);
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
}
