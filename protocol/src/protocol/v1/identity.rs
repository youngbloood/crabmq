use super::auth::AuthType;
use super::new_v1_head;
use super::BuilderV1;
use super::Head;
use super::V1;
use crate::consts::ACTION_IDENTITY;
use crate::consts::PROPTOCOL_V1;
use anyhow::Result;
use bytes::BytesMut;
use rsbit::{BitFlagOperation, BitOperation as _};
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

const IDENTITY_HEAD_LENGTH: usize = 4;

/**
 * CONST [`IDENTITY_HEAD_LENGTH`] length head info:
 * 1st bytes:
 *          1 bit: has crc
 *          3 bit: *reserve bit*
 *          4 bits: msg number
 * 2-4 bytes: reserve bytes
 *
 * EXTEND according the head:
 *          2 bytes: crc value
 */
#[derive(Default, Clone, Debug)]
pub struct IdentityHead([u8; IDENTITY_HEAD_LENGTH]);

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

    pub fn has_crc(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_crc(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 6, has);
        self
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

impl BuilderV1 for Identity {
    fn buildv1(self) -> super::V1 {
        let mut v1 = V1::default();
        v1.set_identity(self);
        v1
    }
}

impl Identity {
    pub fn set_identity_head(&mut self, head: IdentityHead) -> &mut Self {
        self.identity_head = head;
        self
    }

    pub fn set_crc(&mut self, crc: u16) -> &mut Self {
        self.crc = crc;
        self
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut buf = BytesMut::new();
        buf.resize(IDENTITY_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;

        let identity_head = IdentityHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to publish head failed"),
        );

        let mut identity = Identity::default();
        identity
            .set_head(head)
            .set_identity_head(identity_head.clone());

        // parse crc
        if identity_head.has_crc() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            identity.set_crc(u16::from_be_bytes(
                buf.to_vec().try_into().expect("convert to crc vec failed"),
            ));
        }

        Ok(identity)
    }
}

// ==================================  IDENTITY REPLY ============================

pub const IDENTITY_REPLY_HEAD_LENGTH: usize = 2;

/**
 * 1st byte:
 *         1 bit: has support_auth_type.
 *         1 bit: has max_support_protocol_version.
 * 2nd byte: salt length.
 */
#[derive(Default)]
pub struct IdentityReplyHead([u8; IDENTITY_REPLY_HEAD_LENGTH]);

impl IdentityReplyHead {
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

    pub fn has_support_auth_type(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_support_auth_type(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 7, has);
        self
    }

    pub fn has_max_support_protocol_version(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_max_support_protocol_version(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 6, has);
        self
    }

    pub fn salt_len(&self) -> u8 {
        self.0[1]
    }

    pub fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }
}

#[derive(Default)]
pub struct IdentityReply {
    head: Head,
    reply_head: IdentityReplyHead,
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
    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
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
        if self.salt_len() != 0 {
            buf.resize(self.salt_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.salt = String::from_utf8(buf.to_vec())?;
        }

        Ok(())
    }
}
