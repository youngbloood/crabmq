use super::{new_v1_head, ProtError, ACTION_AUTH_REPLY, E_BAD_CRC, X25};
use crate::consts::ACTION_AUTH;
use crate::protocol::Head;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

pub const AUTH_HEAD_LENGTH: usize = 6;

#[derive(Default, Clone)]
pub struct AuthType(u8);

impl Debug for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthType").finish()
    }
}

impl AuthType {
    pub fn with(a: u8) -> Self {
        AuthType(a)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        vec![self.0]
    }
}

/**
 * 1st byte:
 *         1 bit: has crc.
 *         7 bit: *reserve bits*
 * 2nd byte: AuthType
 * 3rd byte: username length.
 * 4-5 bytes: u16: password length.
 * 6th byte: salt length.
 */
#[derive(Default, Clone)]
pub struct AuthHead([u8; AUTH_HEAD_LENGTH]);

impl Debug for AuthHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthHead")
            .field("has-crc", &self.has_crc_flag())
            .field("auth-type", &self.get_authtype())
            .field("username-len", &self.get_username_len())
            .field("password-len", &self.get_password_len())
            .field("salt-len", &self.get_salt_len())
            .finish()
    }
}

impl AuthHead {
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

    pub fn get_authtype(&self) -> AuthType {
        AuthType::with(self.0[1])
    }

    pub fn get_username_len(&self) -> u8 {
        self.0[2]
    }

    pub fn set_username_len(&mut self, l: u8) -> &mut Self {
        self.0[2] = l;
        self
    }

    pub fn get_password_len(&self) -> u16 {
        u16::from_be_bytes(self.0[3..4].try_into().expect("convert to u16 failed"))
    }

    pub fn set_password_len(&mut self, l: u16) -> &mut Self {
        let bts = l.to_be_bytes();
        self.0[3] = bts[0];
        self.0[4] = bts[1];
        self
    }

    pub fn get_salt_len(&self) -> u8 {
        self.0[5]
    }

    pub fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.0[5] = l;
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut auth_head = Self::default();
        auth_head.parse_reader(reader).await?;
        Ok(auth_head)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.resize(AUTH_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.0 = buf
            .to_vec()
            .try_into()
            .expect("convert to auth-head failed");

        Ok(())
    }
}

pub struct Auth {
    head: Head,
    auth_head: AuthHead,

    username: String,
    password: String,
    salt: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_AUTH),
            auth_head: AuthHead::default(),
            username: String::new(),
            password: String::new(),
            salt: String::new(),
        }
    }
}

impl Deref for Auth {
    type Target = AuthHead;

    fn deref(&self) -> &Self::Target {
        &self.auth_head
    }
}

impl Auth {
    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_username(&self) -> &str {
        &self.username
    }

    pub fn set_username(&mut self, username: &str) -> &mut Self {
        self.username = username.to_string();
        self
    }

    pub fn get_password(&self) -> &str {
        &self.password
    }

    pub fn set_password(&mut self, password: &str) -> &mut Self {
        self.password = password.to_string();
        self
    }

    pub fn get_salt(&self) -> &str {
        &self.salt
    }

    pub fn set_salt(&mut self, salt: &str) -> &mut Self {
        self.salt = salt.to_string();
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut auth = Auth::default();
        auth.set_head(head);
        auth.parse_reader(reader).await?;
        Ok(auth)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        self.auth_head = AuthHead::parse_from(reader).await?;
        let mut buf = BytesMut::new();
        // parse username
        if self.get_username_len() != 0 {
            buf.resize(self.get_username_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.username = String::from_utf8(buf.to_vec())?;
        }

        // parse password
        if self.get_password_len() != 0 {
            buf.resize(self.get_password_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.password = String::from_utf8(buf.to_vec())?;
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

const AUTH_REPLY_HEAD_LENGTH: usize = 2;
/**
 * 1 byte:
 *       1 bit: has crc.
 *       1 bit: has timeout.
 * 2 byte: token length.  
 */
#[derive(Default, Clone)]
pub struct AuthReplyHead([u8; AUTH_REPLY_HEAD_LENGTH]);

impl Debug for AuthReplyHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthReplyHead")
            .field("has-crc", &self.has_crc_flag())
            .field("has-timeout", &self.has_timeout_flag())
            .field("token-len", &self.get_token_len())
            .finish()
    }
}

impl AuthReplyHead {
    pub fn with(head: [u8; AUTH_REPLY_HEAD_LENGTH]) -> Self {
        AuthReplyHead(head)
    }

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

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn has_crc_flag(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 7, has);
        self
    }

    pub fn has_timeout_flag(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_timeout_flag(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 6, has);
        self
    }

    pub fn get_token_len(&self) -> u8 {
        self.0[1]
    }

    pub fn set_token_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }
}

#[derive(Clone, Debug)]
pub struct AuthReply {
    head: Head,
    reply_head: AuthReplyHead,

    crc: u16,
    timeout: u64,
    token: String,
}

impl Default for AuthReply {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_AUTH_REPLY),
            reply_head: Default::default(),
            token: Default::default(),
            timeout: Default::default(),
            crc: 0,
        }
    }
}

impl Deref for AuthReply {
    type Target = AuthReplyHead;

    fn deref(&self) -> &Self::Target {
        &self.reply_head
    }
}

impl AuthReply {
    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut auth_reply = self.clone();
        let dst_crc = auth_reply.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }

        Ok(())
    }

    pub fn set_timeout(&mut self, timeout: u64) -> &mut Self {
        self.timeout = timeout;
        self.reply_head.set_timeout_flag(true);
        self
    }

    pub fn set_token(&mut self, token: &str) -> Result<()> {
        if token.len() > u8::MAX as usize {
            return Err(anyhow!("token length excess the max of u8"));
        }
        self.reply_head.set_token_len(token.len() as u8);
        self.token = token.to_string();

        Ok(())
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

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.reply_head.as_bytes());
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.has_timeout_flag() {
            res.extend(self.timeout.to_be_bytes());
        }
        if self.get_token_len() != 0 {
            res.extend(self.token.as_bytes());
        }
        res
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut reply = Self::default();
        reply.parse_reader(reader).await?;
        Ok(reply)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse reply-head
        buf.resize(AUTH_REPLY_HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        self.reply_head = AuthReplyHead::with(
            buf.to_vec()
                .try_into()
                .expect("convert to auth-reply-head failed"),
        );

        // parse crc
        if self.has_crc_flag() {
            buf.resize(2, 0);
            reader.read_exact(&mut buf).await?;
            self.crc = u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc failed"))
        }

        // parse timeout
        if self.has_timeout_flag() {
            buf.resize(8, 0);
            reader.read_exact(&mut buf).await?;
            self.timeout =
                u64::from_be_bytes(buf.to_vec().try_into().expect("convert to timeout failed"))
        }

        // parse token
        if self.get_token_len() != 0 {
            buf.resize(self.get_token_len() as _, 0);
            reader.read_exact(&mut buf).await?;
            self.token = String::from_utf8(buf.to_vec())?;
        }

        Ok(())
    }
}
