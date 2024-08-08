use super::auth::AuthType;
use super::auth::AUTH_TYPE_LENGTH;
use super::new_v1_head;
use super::reply::Reply;
use super::reply::ReplyBuilder;
use super::BuilderV1;
use super::Head;
use super::V1;
use crate::consts::*;
use crate::error::*;
use crate::protocol::Builder;
use crate::protocol::Protocol;
use anyhow::{anyhow, Result};
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

    fn with(head: [u8; IDENTITY_HEAD_LENGTH]) -> Self {
        IdentityHead(head)
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }

    fn as_bytes(&self) -> Vec<u8> {
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
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.identity_head.as_bytes());

        if self.identity_head.has_crc_flag() {
            res.extend(self.get_crc().to_be_bytes())
        }
        res
    }

    pub fn validate(&self) -> Result<()> {
        if !self.identity_head.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut identity = self.clone();
        let dst_crc = identity.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }
        Ok(())
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.identity_head.set_crc_flag(false);
        let bts = self.as_bytes();
        self.crc = X25.checksum(&bts);
        self.identity_head.set_crc_flag(true);
        self
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
            buf.resize(CRC_LENGTH, 0);
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
    fn with(head: [u8; IDENTITY_REPLY_HEAD_LENGTH]) -> Self {
        Self(head)
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

    fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 7, has);
        self
    }

    pub fn has_support_auth_type(&self) -> bool {
        self.0[0].is_1(6)
    }

    fn set_support_auth_type(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 6, has);
        self
    }

    pub fn has_max_support_protocol_version(&self) -> bool {
        self.0[0].is_1(5)
    }

    fn set_max_support_protocol_version(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 5, has);
        self
    }

    pub fn get_salt_len(&self) -> u8 {
        self.0[1]
    }

    fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }
}

#[derive(Clone, Debug)]
pub struct IdentityReply {
    head: Head,
    reply_head: IdentityReplyHead,

    crc: u16,
    support_auth_type: AuthType,
    max_support_protocol_version: u8,
    salt: String,
}

impl Default for IdentityReply {
    fn default() -> Self {
        let mut reply_head = IdentityReplyHead::default();
        reply_head.set_max_support_protocol_version(true);
        Self {
            head: new_v1_head(ACTION_REPLY),
            reply_head,
            crc: 0,
            support_auth_type: AuthType::default(),
            max_support_protocol_version: PROPTOCOL_V1,
            salt: String::new(),
        }
    }
}

impl Deref for IdentityReply {
    type Target = IdentityReplyHead;

    fn deref(&self) -> &Self::Target {
        &self.reply_head
    }
}

impl BuilderV1 for IdentityReply {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        let reply = self.build_reply_ok();
        v1.set_reply(reply);
        v1
    }
}

impl Builder for IdentityReply {
    fn build(self) -> Protocol {
        Protocol::V1(self.buildv1())
    }
}

impl ReplyBuilder for IdentityReply {
    fn build_reply_ok(&self) -> Reply {
        let mut reply = Reply::with_ok(ACTION_IDENTITY);
        let _ = reply.set_identity_reply(self.clone());
        reply
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_IDENTITY, err_code)
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
        // ignore the head in reply.
        // res.extend(self.head.as_bytes());
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

    pub fn get_authtype(&self) -> AuthType {
        self.support_auth_type.clone()
    }

    pub fn set_authtype(&mut self, auth_type: AuthType) -> &mut Self {
        self.support_auth_type = auth_type;
        self.reply_head.set_support_auth_type(true);
        self
    }

    pub fn get_max_protocol_version(&self) -> u8 {
        self.max_support_protocol_version
    }

    pub fn set_max_protocol_version(&mut self, v: u8) -> &mut Self {
        self.max_support_protocol_version = v;
        self.reply_head.set_max_support_protocol_version(true);
        self
    }

    pub fn get_salt(&self) -> &str {
        &self.salt
    }

    pub fn set_salt(&mut self, salt: &str) -> Result<()> {
        if salt.len() > u8::MAX as usize {
            return Err(anyhow!("salt length excess the max u8"));
        }
        self.reply_head.set_salt_len(salt.len() as _);
        self.salt = salt.to_string();
        Ok(())
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut ir = IdentityReply::default();
        ir.parse_reader(reader).await?;
        Ok(ir)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse identity reply head
        buf.resize(IDENTITY_REPLY_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.reply_head = IdentityReplyHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to identity-reply-head failed"),
        );

        // parse crc
        if self.has_crc_flag() {
            buf.resize(CRC_LENGTH, 0);
            reader.read_exact(&mut buf).await?;
            self.crc = u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc failed"));
        }

        // parse support auth type
        if self.has_support_auth_type() {
            buf.resize(AUTH_TYPE_LENGTH, 0);
            reader.read_exact(&mut buf).await?;
            self.support_auth_type = AuthType::with(
                buf.to_vec()
                    .try_into()
                    .expect("convert to auth-type failed"),
            );
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
