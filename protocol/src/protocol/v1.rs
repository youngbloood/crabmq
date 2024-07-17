use std::ops::DerefMut;

use super::{ProtError, *};
use crate::{Head, PROTOCOL_HEAD_LEN, SUPPORT_PROTOCOLS};
use anyhow::{anyhow, Error, Result};
use bytes::BytesMut;
use tracing::debug;

#[derive(Default)]
pub struct ProtocolHeadV1 {
    head: Head,
    topic: String,
    channel: String,
    token: String,
    // reject_code: u8,
    pub defer_msg_format: String,
}

impl Debug for ProtocolHeadV1 {
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
            .field("defer_msg_format", &self.defer_msg_format)
            .finish()
    }
}

impl Deref for ProtocolHeadV1 {
    type Target = Head;

    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

impl DerefMut for ProtocolHeadV1 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.head
    }
}

impl ProtocolHeadV1 {
    pub fn new() -> Self {
        ProtocolHeadV1 {
            head: Head::default(),
            topic: String::new(),
            channel: String::new(),
            token: String::new(),
            defer_msg_format: String::new(),
        }
    }

    pub fn new_with_head(head: Head) -> Self {
        ProtocolHeadV1 {
            head,
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
    pub async fn parse_from(fd: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut head = ProtocolHeadV1::new();
        head.read_parse(fd).await?;
        Ok(head)
    }

    /// [`read_parse`] read the protocol head from reader.
    pub async fn read_parse(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();
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
        ph.set_head(self.head.clone());
        let _ = ph.set_topic(self.topic.as_str());
        let _ = ph.set_channel(self.channel.as_str());
        let _ = ph.set_token(self.token.as_str());
        let _ = ph.set_defer_format(self.defer_msg_format.as_str());

        ph
    }

    pub fn validate(&self) -> Result<()> {
        let v = self.version();
        if !SUPPORT_PROTOCOLS.contains(&v) {
            return Err(Error::from(ProtError::new(ERR_NOT_SUPPORT_VERSION)));
        }
        if self.is_req() {
            return self.validate_req();
        }
        self.validate_resq()
    }

    /// [`validate_req`] validate the head is valid in protocol.
    fn validate_req(&self) -> Result<()> {
        if !self.is_req() {
            return Err(Error::from(ProtError::new(ERR_ZERO)));
        }
        if self.reject() || self.reject_code() != 0 {
            return Err(Error::from(ProtError::new(ERR_SHOULD_NOT_REJECT_CODE)));
        }
        match self.action() {
            ACTION_PUB | ACTION_FIN | ACTION_RDY | ACTION_REQ => {
                if self.msg_num() == 0 {
                    return Err(Error::from(ProtError::new(RR_NEED_MSG)));
                }
                if self.msg_num() > PROTOCOL_MAX_MSG_NUM {
                    return Err(Error::from(ProtError::new(ERR_EXCEED_MAX_NUM)));
                }
            }

            ACTION_NOP => {}

            _ => {
                if self.msg_num() != 0 {
                    return Err(Error::from(ProtError::new(ERR_SHOULD_NOT_MSG)));
                }
            }
        }
        Ok(())
    }

    /// [`validate_resq`] validate the head is valid in protocol.
    fn validate_resq(&self) -> Result<()> {
        if self.is_req() {
            return Err(Error::from(ProtError::new(ERR_ZERO)));
        }
        if self.msg_num() > PROTOCOL_MAX_MSG_NUM {
            return Err(Error::from(ProtError::new(ERR_EXCEED_MAX_NUM)));
        }
        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result = vec![];
        result.extend(self.head.bytes());
        result.extend(self.topic.as_bytes());
        result.extend(self.channel.as_bytes());
        result.extend(self.token.as_bytes());
        result.extend(self.defer_msg_format.as_bytes());

        result
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn set_defer_format(&mut self, fmt: &str) -> Result<()> {
        if fmt.len() > u8::MAX as usize {
            return Err(anyhow!("exceed the max the u8"));
        }
        self.head.set_defer_format_len(fmt.len() as u8);
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
        self.head.set_topic_len(topic.len() as u8);
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
        self.head.set_channel_len(channel.len() as u8);
        Ok(())
    }

    pub fn set_token(&mut self, token: &str) -> Result<()> {
        if token.len() > u8::MAX as usize {
            return Err(anyhow!("token len exceed max length 256"));
        }
        self.token = token.to_string();
        self.head.set_token_len(token.len() as u8);
        Ok(())
    }
}

/**
* ProtocolBodys:
*/
#[derive(Debug, Default)]
pub struct ProtocolBodysV1 {
    sid: i64, // 标识一批次的message
    pub list: Vec<ProtocolBodyV1>,
}

impl ProtocolBodysV1 {
    pub fn new() -> Self {
        ProtocolBodysV1 {
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
    pub async fn read_parse(
        &mut self,
        reader: &mut Pin<&mut impl AsyncReadExt>,
        mut msg_num: u8,
    ) -> Result<()> {
        // let mut pbs = ProtocolBodys::new();
        while msg_num != 0 {
            msg_num -= 1;

            let mut pb = ProtocolBodyV1::new();
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

    pub fn push(&mut self, pb: ProtocolBodyV1) -> &mut Self {
        self.list.push(pb);
        self
    }

    pub fn pop(&mut self) -> Option<ProtocolBodyV1> {
        self.list.pop()
    }

    pub fn validate(&self, max_msg_num: u64, max_msg_len: u64) -> Result<()> {
        if self.list.len() > max_msg_num as usize {
            return Err(Error::from(ProtError::new(ERR_EXCEED_MAX_NUM)));
        }
        let iter = self.list.iter();
        for pb in iter {
            pb.validate(max_msg_len)?;
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
pub struct ProtocolBodyV1 {
    head: [u8; PROTOCOL_BODY_HEAD_LEN],
    sid: i64,        // session_id: generate by ProtocolBodys
    defer_time: u64, // 8 bytes
    pub id: String,
    // id是否来源于外部
    id_source_external: bool,
    body: Bytes,
}

impl Debug for ProtocolBodyV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolBody")
            .field("is_update", &self.is_update())
            .field("is_ack", &self.is_ack())
            .field("is_persist", &self.is_persist())
            .field("is_defer", &self.is_defer())
            .field("is_defer_concrete", &self.is_defer_concrete())
            .field("is_delete", &self.is_delete())
            .field("is_notready", &self.is_notready())
            .field("is_consume", &self.is_consume())
            .field("defer_time", &self.defer_time)
            .field("id", &self.id)
            .field("body", &self.body)
            .finish()
    }
}

impl ProtocolBodyV1 {
    pub fn new() -> Self {
        ProtocolBodyV1 {
            head: [0_u8; PROTOCOL_BODY_HEAD_LEN],
            sid: 0,
            defer_time: 0,
            id: String::new(),
            id_source_external: false,
            body: Bytes::new(),
        }
    }

    pub fn validate(&self, max_msg_len: u64) -> Result<()> {
        if self.body_len() > max_msg_len {
            return Err(Error::from(ProtError::new(ERR_EXCEED_MAX_LEN)));
        }
        if self.is_update() && self.id.is_empty() {
            return Err(Error::from(ProtError::new(ERR_SHOULD_HAS_ID)));
        }

        Ok(())
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
            self.id_source_external = true;
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
