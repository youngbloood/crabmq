use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use chrono::Local;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::{ops::Deref, pin::Pin};
use tokio::io::AsyncReadExt;

const BIN_MESSAGE_HEAD: usize = 12;
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

#[derive(Default, Clone, Debug)]
pub struct BinMessageHead([u8; BIN_MESSAGE_HEAD]);

impl BinMessageHead {
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

    pub fn with(head: [u8; BIN_MESSAGE_HEAD]) -> Self {
        BinMessageHead(head)
    }

    // updated flag.
    pub fn is_update(&self) -> bool {
        self.0[0].is_1(7)
    }

    /// set the update flag value.
    pub fn set_update(&mut self, update: bool) -> &mut Self {
        self.set_flag(0, 7, update);
        self
    }

    // ack flag
    pub fn is_ack(&self) -> bool {
        self.0[0].is_1(6)
    }

    /// set the ack flag value.
    pub fn set_ack(&mut self, ack: bool) -> &mut Self {
        self.set_flag(0, 6, ack);
        self
    }

    /// persist flag.
    pub fn is_persist(&self) -> bool {
        self.0[0].is_1(5)
    }

    /// set the persist flag value.
    pub fn set_persist(&mut self, persist: bool) -> &mut Self {
        self.set_flag(0, 5, persist);
        self
    }

    /// defer flag.
    pub fn is_defer(&self) -> bool {
        self.0[0].is_1(4)
    }

    /// set the defer flag value.
    pub fn set_defer(&mut self, defer: bool) -> &mut Self {
        self.set_flag(0, 4, defer);
        self
    }

    /// defer_concrete flag.
    pub fn is_defer_concrete(&self) -> bool {
        self.0[0].is_1(3)
    }

    /// set the defer_concrete flag value.
    pub fn set_defer_concrete(&mut self, concrete: bool) -> &mut Self {
        self.set_flag(0, 3, concrete);
        self
    }

    /// delete flag.
    pub fn is_delete(&self) -> bool {
        self.0[0].is_1(2)
    }

    /// set the delete flag value.
    pub fn set_delete(&mut self, delete: bool) -> &mut Self {
        self.set_flag(0, 2, delete);
        self
    }

    /// notready flag
    pub fn is_notready(&self) -> bool {
        self.0[0].is_1(1)
    }

    /// set the notready flag value.
    pub fn set_notready(&mut self, notready: bool) -> &mut Self {
        self.set_flag(0, 1, notready);
        self
    }

    /// consume flag.
    pub fn is_consume(&self) -> bool {
        self.0[0].is_1(0)
    }

    /// set the consume flag value.
    pub fn set_consume(&mut self, consume: bool) -> &mut Self {
        self.set_flag(0, 0, consume);
        self
    }

    pub fn id_len(&self) -> u8 {
        self.0[1]
    }

    pub fn set_id_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }

    pub fn body_len(&self) -> u64 {
        u64::from_be_bytes(
            self.0[2..BIN_MESSAGE_HEAD - 2]
                .to_vec()
                .try_into()
                .expect("get body length failed"),
        )
    }

    pub fn set_body_len(&mut self, l: u64) -> &mut Self {
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

#[derive(Default, Clone, Debug)]
pub struct BinMessage {
    head: BinMessageHead,
    sid: i64,        // session_id: generate by ProtocolBodys
    defer_time: u64, // 8 bytes
    id: String,
    body: Bytes,
}

impl Deref for BinMessage {
    type Target = BinMessageHead;

    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

impl BinMessage {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.0.to_vec());
        if self.head.is_defer() {
            res.extend(self.defer_time.to_be_bytes());
        }
        res.extend(self.id.as_bytes());
        res.extend(self.body.to_vec());

        res
    }

    pub fn get_head(&self) -> &BinMessageHead {
        &self.head
    }

    pub fn set_head(&mut self, head: BinMessageHead) -> &mut Self {
        self.head = head;
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

    pub async fn parse_from(
        reader: &mut Pin<&mut impl AsyncReadExt>,
        mut num: u8,
    ) -> Result<Vec<Self>> {
        let mut buf = BytesMut::new();

        let mut res = vec![];
        while num != 0 {
            num -= 1;
            buf.resize(BIN_MESSAGE_HEAD, 0);
            reader.read_exact(&mut buf).await?;

            let head = BinMessageHead::with(
                buf.to_vec()
                    .try_into()
                    .expect("convert to bin message head failed"),
            );

            let mut bin_msg = BinMessage::default();
            bin_msg.set_head(head.clone());

            // parse defer time
            if head.is_defer() {
                buf.resize(8, 0);
                reader.read_exact(&mut buf).await?;
                let defer_time = u64::from_be_bytes(
                    buf.to_vec()
                        .try_into()
                        .expect("convert to defer time failed"),
                );
                if head.is_defer_concrete() {
                    bin_msg.set_defer_time(defer_time);
                } else {
                    let dt = Local::now();
                    bin_msg.set_defer_time(dt.timestamp() as u64 + defer_time);
                }
            }

            // parse id
            let id_len = head.id_len();
            if id_len != 0 {
                buf.resize(id_len as usize, 0);
                reader.read_exact(&mut buf).await?;
                let _ = bin_msg.set_id(&String::from_utf8(buf.to_vec()).expect("illigal id value"));
            }

            // parse body
            let body_len = head.body_len();
            if body_len != 0 {
                buf.resize(body_len as usize, 0);
                reader.read_exact(&mut buf).await?;
                bin_msg.set_body(Bytes::copy_from_slice(buf.as_ref()));
            }

            res.push(bin_msg);
        }

        Ok(res)
    }
}
