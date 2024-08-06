use super::auth::AuthType;
use super::new_v1_head;
use super::reply::Reply;
use super::reply::ReplyBuilder;
use super::BuilderV1;
use super::Head;
use super::ProtError;
use super::E_BAD_CRC;
use super::V1;
use super::X25;
use crate::consts::ACTION_IDENTITY;
use crate::protocol::Builder;
use crate::protocol::Protocol;
use anyhow::Result;
use bytes::BytesMut;
use rsbit::{BitFlagOperation, BitOperation as _};
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

const IDENTITY_HEAD_LENGTH: usize = 4;

/**
 * CONST [`IDENTITY_HEAD_LENGTH`] length head info:
 * 1st bytes:
 *          1 bit: has crc
 *          7 bits: *reserve bit*
 * 2-4 bytes: reserve bytes
 *
 * EXTEND according the head:
 *          2 bytes: crc value
 */
#[derive(Default, Clone)]
pub struct IdentityHead([u8; IDENTITY_HEAD_LENGTH]);

impl Debug for IdentityHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityHead")
            .field("has-crc", &self.has_crc_flag())
            .finish()
    }
}

impl IdentityHead {
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

    pub fn with(head: [u8; IDENTITY_HEAD_LENGTH]) -> Self {
        IdentityHead(head)
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

#[derive(Clone, Debug)]
pub struct Identity {
    head: Head,
    identity_head: IdentityHead,
    crc: u16,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_IDENTITY),
            identity_head: Default::default(),
            crc: Default::default(),
        }
    }
}

impl Deref for Identity {
    type Target = IdentityHead;

    fn deref(&self) -> &Self::Target {
        &self.identity_head
    }
}

impl BuilderV1 for Identity {
    fn buildv1(self) -> super::V1 {
        let mut v1 = V1::default();
        v1.set_identity(self);
        v1
    }
}

impl ReplyBuilder for Identity {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_IDENTITY)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_IDENTITY, err_code)
    }
}

impl Identity {
    pub fn validate(&self) -> Option<Protocol> {
        if !self.identity_head.has_crc_flag() {
            return None;
        }
        let src_crc = self.get_crc();
        let mut identity = self.clone();
        let dst_crc = identity.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Some(self.build_reply_err(E_BAD_CRC).build());
        }
        None
    }

    pub fn set_identity_head(&mut self, head: IdentityHead) -> &mut Self {
        self.identity_head = head;
        self
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.identity_head.set_crc_flag(false);
        let bts = self.as_bytes();
        self.crc = X25.checksum(&bts);
        self
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.identity_head.as_bytes());

        if self.identity_head.has_crc_flag() {
            res.extend(self.get_crc().to_be_bytes())
        }
        res
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut identity = Identity::default();
        identity.parse_reader(reader).await?;
        Ok(identity)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.resize(IDENTITY_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;

        let identity_head = IdentityHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to publish head failed"),
        );
        self.identity_head = identity_head;

        // parse crc
        if self.has_crc_flag() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            self.crc =
                u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc vec failed"));
        }

        Ok(())
    }
}

// ==================================  IDENTITY REPLY ============================

pub const IDENTITY_REPLY_HEAD_LENGTH: usize = 2;

/**
 * 1st byte:
 *         1 bit: has crc.
 *         1 bit: has support_auth_type.
 *         1 bit: has max_support_protocol_version.
 * 2nd byte: salt length.
 */
#[derive(Default, Clone)]
pub struct IdentityReplyHead([u8; IDENTITY_REPLY_HEAD_LENGTH]);

impl Debug for IdentityReplyHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityReplyHead")
            .field("has-crc", &self.has_crc_flag())
            .field("has-support-auth-type", &self.has_support_auth_type())
            .field(
                "has-max-support-protocol-versioin",
                &self.has_max_support_protocol_version(),
            )
            .field("salt-len", &self.get_salt_len())
            .finish()
    }
}

impl IdentityReplyHead {
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

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }

    pub fn has_support_auth_type(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_support_auth_type(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 6, has);
        self
    }

    pub fn has_max_support_protocol_version(&self) -> bool {
        self.0[0].is_1(5)
    }

    pub fn set_max_support_protocol_version(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 5, has);
        self
    }

    pub fn get_salt_len(&self) -> u8 {
        self.0[1]
    }

    pub fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }
}

#[derive(Default, Clone)]
pub struct IdentityReply {
    head: Head,
    reply_head: IdentityReplyHead,

    crc: u16,
    support_auth_type: AuthType,
    max_support_protocol_version: u8,
    salt: String,
}

impl Deref for IdentityReply {
    type Target = IdentityReplyHead;

    fn deref(&self) -> &Self::Target {
        &self.reply_head
    }
}

impl IdentityReply {
    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut identity_reply = self.clone();
        let dst_crc = identity_reply.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }
        Ok(())
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.reply_head.as_bytes());
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.has_support_auth_type() {
            res.extend(self.support_auth_type.as_bytes());
        }
        if self.has_max_support_protocol_version() {
            res.push(self.max_support_protocol_version);
        }
        if self.get_salt_len() != 0 {
            res.extend(self.salt.as_bytes());
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

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.reply_head.set_crc_flag(false);
        let bts = self.as_bytes();
        self.crc = X25.checksum(&bts);
        self.reply_head.set_crc_flag(true);

        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut ir = IdentityReply::default();
        ir.set_head(head);
        ir.parse_reader(reader).await?;
        Ok(ir)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse crc
        if self.has_crc_flag() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            self.crc = u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc failed"));
        }

        // parse support auth type
        if self.has_support_auth_type() {
            buf.resize(1, 0);
            reader.read_exact(&mut buf).await?;
            self.support_auth_type = AuthType::with(*buf.first().unwrap());
        }

        // parse max support protocol version
        if self.has_max_support_protocol_version() {
            buf.resize(1, 0);
            reader.read_exact(&mut buf).await?;
            self.max_support_protocol_version = *buf.first().unwrap();
        }

        // parse salt
        if self.get_salt_len() != 0 {
            buf.resize(self.get_salt_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.salt = String::from_utf8(buf.to_vec())?;
        }

        Ok(())
    }
}
