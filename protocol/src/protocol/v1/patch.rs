use super::{
    new_v1_head,
    reply::{Reply, ReplyBuilder},
    BuilderV1, ProtError, ACTION_PATCH, CRC_LENGTH, E_BAD_CRC, V1, X25,
};
use crate::protocol::{Builder, Head, Protocol};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    pin::Pin,
};
use tokio::io::AsyncReadExt;

const UPDATE_HEAD_LENGTH: usize = 4;
/**
### FIXED HEAD LENGTH([[`PROTOCOL_HEAD_LEN`] bytes), Every body has the same structure:
* head: 10 bytes:
* 1st byte: flag:
*           1bit: has crc.
*           1bit: is heartbeat.
*
*       The follow flags[`delete`], [`fin`], [`notready`] always used by publisher.
*           1bit: is update-delete: only update [`delete`] flag.
*           1bit: is delete: mark this message is delete, then will not be consumed if it not consume.(must with MSG_ID)（优先级高于is notready)
*           1bit: is update-notready: only update [`notready`] flag.
*           1bit: is notready: mark this message if notready. false mean the message can't be consumed, true mean the message can be consumed.(must with MSG_ID)
*           1bit: is update-fin: only update [`fin`] flag.
*           1bit: is fin: mark the message has been consumed, then the message will not re-transport to client. #NNR
*
* 2nd byte:
*       The follow 2bits flags always used by subscribers.
*           1bit: rrt: Reset Re-transport Timeout. #NNR
*       The follow 1bit flag is can be used by publishers/subscribers.
*           1bit: close: is close the client. If this flag is true, and the update flag is true. Then will do the update and close the client connection. #NNR

* 3rd byte: topic length.
* 4th byte: ID-LENGTH
*
* #NNR denote the message Not-Need-Reply.
* optional:
*           [`CRC_LENGTH`] bytes: crc.
*           id value(length determined by ID-LENGTH)
*/

#[derive(Default, Clone)]
pub struct PatchHead([u8; UPDATE_HEAD_LENGTH]);

impl Debug for PatchHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatchHead")
            .field("has-crc", &self.has_crc_flag())
            .field("is-heartbeat", &self.is_heartbeat())
            .field("is-update-delete", &self.is_update_delete())
            .field("is-delete", &self.is_delete())
            .field("is-update-notready", &self.is_update_delete())
            .field("is-notready", &self.is_notready())
            .field("is-fin", &self.is_fin())
            .field("is-close", &self.is_fin())
            .field("topic-len", &self.get_id_len())
            .field("id-len", &self.get_id_len())
            .finish()
    }
}

impl PatchHead {
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

    fn with(head: [u8; UPDATE_HEAD_LENGTH]) -> Self {
        PatchHead(head)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }

    pub fn is_heartbeat(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_heartbeat(&mut self, hb: bool) -> &mut Self {
        self.set_head_flag(0, 6, hb);
        self
    }

    // updated flag.
    pub fn is_update_delete(&self) -> bool {
        self.0[0].is_1(5)
    }

    /// set the update flag value.
    fn set_update_delete(&mut self, update: bool) -> &mut Self {
        self.set_head_flag(0, 5, update);
        self
    }

    /// delete flag.
    pub fn is_delete(&self) -> bool {
        self.0[0].is_1(4)
    }

    /// set the delete flag value.
    pub fn set_delete(&mut self, delete: bool) -> &mut Self {
        self.set_head_flag(0, 4, delete);
        self.set_update_delete(true);
        self
    }

    // updated flag.
    pub fn is_update_notready(&self) -> bool {
        self.0[0].is_1(3)
    }

    /// set the update flag value.
    fn set_update_notready(&mut self, update: bool) -> &mut Self {
        self.set_head_flag(0, 3, update);
        self
    }

    /// notready flag
    pub fn is_notready(&self) -> bool {
        self.0[0].is_1(2)
    }

    /// set the notready flag value.
    pub fn set_notready(&mut self, notready: bool) -> &mut Self {
        self.set_head_flag(0, 2, notready);
        self.set_update_notready(true);
        self
    }

    // updated flag.
    pub fn is_update_fin(&self) -> bool {
        self.0[0].is_1(1)
    }

    /// set the update flag value.
    fn set_update_fin(&mut self, update: bool) -> &mut Self {
        self.set_head_flag(0, 1, update);
        self
    }

    /// fin flag.
    pub fn is_fin(&self) -> bool {
        self.0[0].is_1(0)
    }

    /// set the fin flag value.
    pub fn set_fin(&mut self, fin: bool) -> &mut Self {
        self.set_head_flag(0, 0, fin);
        self.set_update_fin(true);
        self
    }

    /// rrt: Reset Re-transport Timeout flag.
    pub fn is_rrt(&self) -> bool {
        self.0[1].is_1(7)
    }

    /// set the rrt flag value.
    pub fn set_rrt(&mut self, rrt: bool) -> &mut Self {
        self.set_head_flag(1, 7, rrt);
        self
    }

    /// fin flag.
    pub fn is_close(&self) -> bool {
        self.0[1].is_1(6)
    }

    /// set the close flag value.
    pub fn set_close(&mut self, close: bool) -> &mut Self {
        self.set_head_flag(1, 6, close);
        self
    }

    pub fn get_topic_len(&self) -> u8 {
        self.0[2]
    }

    fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[2] = l;
        self
    }

    pub fn get_id_len(&self) -> u8 {
        self.0[3]
    }

    fn set_id_len(&mut self, l: u8) -> &mut Self {
        self.0[3] = l;
        self
    }

    /// nnr: No-Need-Reply flag
    pub fn is_nnr(&self) -> bool {
        self.is_fin() || self.is_rrt() || self.is_close()
    }
}

#[derive(Clone, Debug)]
pub struct Patch {
    head: Head,
    update_head: PatchHead,

    crc: u16,
    topic: String,
    id: String,
}

impl Default for Patch {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_PATCH),
            update_head: PatchHead::default(),
            crc: Default::default(),
            topic: String::new(),
            id: Default::default(),
        }
    }
}

impl Deref for Patch {
    type Target = PatchHead;

    fn deref(&self) -> &Self::Target {
        &self.update_head
    }
}

impl DerefMut for Patch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.update_head
    }
}

impl ReplyBuilder for Patch {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_PATCH)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_PATCH, err_code)
    }
}

impl BuilderV1 for Patch {
    fn buildv1(self) -> super::V1 {
        let mut v1 = V1::default();
        v1.set_patch(self);
        v1
    }
}

impl Builder for Patch {
    fn build(self) -> Protocol {
        Protocol::V1(Self::buildv1(self))
    }
}

impl Patch {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.update_head.as_bytes());
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.get_topic_len() != 0 {
            res.extend(self.topic.as_bytes());
        }
        if self.get_id_len() != 0 {
            res.extend(self.id.as_bytes());
        }
        res
    }

    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut update = self.clone();
        let dst_crc = update.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }
        Ok(())
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.set_crc_flag(false);
        self.crc = X25.checksum(&self.as_bytes());
        self.set_crc_flag(true);
        self
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn set_id(&mut self, id: &str) -> Result<()> {
        if id.len() > u8::MAX as usize {
            return Err(anyhow!("id length excess the max u8"));
        }
        self.set_id_len(id.len() as _);
        self.id = id.to_string();
        Ok(())
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) -> Result<()> {
        if topic.len() > u8::MAX as usize {
            return Err(anyhow!("topic length excess the max u8"));
        }
        self.set_topic_len(topic.len() as _);
        self.topic = topic.to_string();
        Ok(())
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut fin = Patch::default();
        fin.parse_reader(reader).await?;
        Ok(fin)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse head
        buf.resize(UPDATE_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.update_head =
            PatchHead::with(buf.to_vec().try_into().expect("convert to fin-head failed"));

        // parse crc
        if self.has_crc_flag() {
            buf.resize(CRC_LENGTH, 0);
            reader.read_exact(&mut buf).await?;
            self.crc =
                u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc vec failed"));
        }

        // parse id
        if self.get_id_len() != 0 {
            buf.resize(self.get_id_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.id = String::from_utf8(buf.to_vec()).expect("convert to id failed");
        }

        Ok(())
    }
}
