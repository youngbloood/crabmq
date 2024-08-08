use super::reply::{Reply, ReplyBuilder};
use super::{new_v1_head, BuilderV1, ProtError, ACTION_REPLY, CRC_LENGTH, E_BAD_CRC, V1, X25};
use crate::consts::ACTION_AUTH;
use crate::protocol::{Builder, Head, Protocol};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use rsbit::{BitFlagOperation as _, BitOperation as _};
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

pub const AUTH_TYPE_LENGTH: usize = 2;
pub const AUTH_HEAD_LENGTH: usize = 5 + AUTH_TYPE_LENGTH;

/**
 * 1st byte: series: hash type.
 * 2nd byte: branch: the branch of this hash type.
 *
 * 常见列表参考: [wiki](https://zh.wikipedia.org/wiki/%E6%95%A3%E5%88%97%E5%87%BD%E6%95%B8)
 * SUPPORT LIST: 碰撞(collision)
 * MD: 1-x
 *      MD5: 1-2
 * SHA: 2-x
 *      SHA-0: 2-0
 *      SHA-1: 2-1
 *      SHA-256: 2-2
 *      SHA-512: 2-3
 */
#[derive(Default, Clone)]
pub struct AuthType([u8; AUTH_TYPE_LENGTH]);

impl Debug for AuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthType").finish()
    }
}

impl AuthType {
    fn series(&self) -> u8 {
        self.0[0]
    }

    fn set_series(&mut self, s: u8) -> &mut Self {
        self.0[0] = s;
        self
    }

    fn branch(&self) -> u8 {
        self.0[1]
    }

    fn set_branch(&mut self, b: u8) -> &mut Self {
        self.0[1] = b;
        self
    }

    pub fn with(auth: [u8; AUTH_TYPE_LENGTH]) -> Self {
        AuthType(auth)
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn is_no_need_auth(&self) -> bool {
        self.series() == 0 && self.branch() == 0
    }
    //  ================= MD5 Series =================
    pub fn is_md5(&self) -> bool {
        self.series() == 1 && self.branch() == 2
    }

    pub fn set_md5(&mut self) -> &mut Self {
        self.set_series(1).set_branch(2);
        self
    }
    //  ================= MD5 Series =================

    //  ================= SHA Series =================
    pub fn is_sha0(&self) -> bool {
        self.series() == 2 && self.branch() == 0
    }

    pub fn set_sha0(&mut self) -> &mut Self {
        self.set_series(2).set_branch(0);
        self
    }

    pub fn is_sha1(&self) -> bool {
        self.series() == 2 && self.branch() == 1
    }

    pub fn set_sha1(&mut self) -> &mut Self {
        self.set_series(2).set_branch(1);
        self
    }

    pub fn is_sha256(&self) -> bool {
        self.series() == 2 && self.branch() == 2
    }

    pub fn set_sha256(&mut self) -> &mut Self {
        self.set_series(2).set_branch(2);
        self
    }

    pub fn is_sha512(&self) -> bool {
        self.series() == 2 && self.branch() == 3
    }

    pub fn set_sha512(&mut self) -> &mut Self {
        self.set_series(2).set_branch(3);
        self
    }
    //  ================= SHA Series =================
}

/**
 * 1st byte:
 *         1 bit: has crc.
 *         7 bit: *reserve bits*
 * 2nd byte: username length.
 * 3-4 bytes: u16: password length.
 * 5th byte: salt length.
 * [`AUTH_TYPE_LENGTH`] bytes: AuthType
 *
 * optional:
 *        [`CRC_LENGTH`] bytes: crc value.
 *        n bytes: username value.
 *        n bytes: password value.
 *        n bytes: salt value.
 */
#[derive(Default, Clone)]
pub struct AuthHead([u8; AUTH_HEAD_LENGTH]);

impl Debug for AuthHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthHead")
            .field("has-crc", &self.has_crc_flag())
            .field("username-len", &self.get_username_len())
            .field("password-len", &self.get_password_len())
            .field("salt-len", &self.get_salt_len())
            .field("auth-type", &self.get_authtype())
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

    pub fn get_username_len(&self) -> u8 {
        self.0[1]
    }

    pub fn set_username_len(&mut self, l: u8) -> &mut Self {
        self.0[1] = l;
        self
    }

    pub fn get_password_len(&self) -> u16 {
        u16::from_be_bytes(self.0[2..4].try_into().expect("convert to u16 failed"))
    }

    pub fn set_password_len(&mut self, l: u16) -> &mut Self {
        let bts = l.to_be_bytes();
        self.0[2] = bts[0];
        self.0[3] = bts[1];
        self
    }

    pub fn get_salt_len(&self) -> u8 {
        self.0[4]
    }

    pub fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.0[4] = l;
        self
    }

    pub fn get_authtype(&self) -> AuthType {
        AuthType::with(self.0[4..].try_into().expect("convert to auth-type failed"))
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

#[derive(Clone, Debug)]
pub struct Auth {
    head: Head,
    auth_head: AuthHead,

    crc: u16,
    username: String,
    password: String,
    salt: String,
}

impl Default for Auth {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_AUTH),
            auth_head: AuthHead::default(),
            crc: 0,
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

impl ReplyBuilder for Auth {
    fn build_reply_ok(&self) -> Reply {
        Reply::with_ok(ACTION_AUTH)
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_AUTH, err_code)
    }
}

impl BuilderV1 for Auth {
    fn buildv1(self) -> super::V1 {
        let mut v1 = V1::default();
        v1.set_auth(self);
        v1
    }
}

impl Builder for Auth {
    fn build(self) -> Protocol {
        Protocol::V1(Self::buildv1(self))
    }
}

impl Auth {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.auth_head.as_bytes());
        if self.has_crc_flag() {
            res.extend(self.crc.to_be_bytes());
        }
        if self.get_username_len() != 0 {
            res.extend(self.username.as_bytes());
        }
        if self.get_password_len() != 0 {
            res.extend(self.password.as_bytes());
        }
        if self.get_salt_len() != 0 {
            res.extend(self.salt.as_bytes())
        }
        res
    }

    pub fn validate(&self) -> Result<()> {
        if !self.has_crc_flag() {
            return Ok(());
        }
        let src_crc = self.get_crc();
        let mut auth = self.clone();
        let dst_crc = auth.calc_crc().get_crc();
        if src_crc != dst_crc {
            return Err(ProtError::new(E_BAD_CRC).into());
        }
        Ok(())
    }

    pub fn get_crc(&self) -> u16 {
        self.crc
    }

    pub fn calc_crc(&mut self) -> &mut Self {
        self.auth_head.set_crc_flag(false);
        self.crc = X25.checksum(&self.as_bytes());
        self.auth_head.set_crc_flag(true);
        self
    }

    pub fn get_username(&self) -> &str {
        &self.username
    }

    pub fn set_username(&mut self, username: &str) -> Result<()> {
        if username.len() >= u8::MAX as usize {
            return Err(anyhow!("username length excess the max u8"));
        }
        self.auth_head.set_username_len(username.len() as _);
        self.username = username.to_string();
        Ok(())
    }

    pub fn get_password(&self) -> &str {
        &self.password
    }

    pub fn set_password(&mut self, password: &str) -> Result<()> {
        if password.len() >= u16::MAX as usize {
            return Err(anyhow!("password length excess the max u16"));
        }
        self.auth_head.set_password_len(password.len() as _);
        self.password = password.to_string();
        Ok(())
    }

    pub fn get_salt(&self) -> &str {
        &self.salt
    }

    pub fn set_salt(&mut self, salt: &str) -> Result<()> {
        if salt.len() >= u8::MAX as usize {
            return Err(anyhow!("salt length excess the max u8"));
        }
        self.auth_head.set_salt_len(salt.len() as _);
        self.salt = salt.to_string();
        Ok(())
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut auth = Auth::default();
        auth.parse_reader(reader).await?;
        Ok(auth)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        self.auth_head = AuthHead::parse_from(reader).await?;
        let mut buf = BytesMut::new();

        // parse crc
        if self.has_crc_flag() {
            buf.resize(CRC_LENGTH, 0);
            reader.read_exact(&mut buf).await?;
            self.crc = u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc failed"))
        }

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
    fn with(head: [u8; AUTH_REPLY_HEAD_LENGTH]) -> Self {
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

    fn set_crc_flag(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 7, has);
        self
    }

    pub fn has_timeout_flag(&self) -> bool {
        self.0[0].is_1(6)
    }

    fn set_timeout_flag(&mut self, has: bool) -> &mut Self {
        self.set_flag(0, 6, has);
        self
    }

    pub fn get_token_len(&self) -> u8 {
        self.0[1]
    }

    fn set_token_len(&mut self, l: u8) -> &mut Self {
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
            head: new_v1_head(ACTION_REPLY),
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

impl ReplyBuilder for AuthReply {
    fn build_reply_ok(&self) -> Reply {
        let mut reply = Reply::with_ok(ACTION_AUTH);
        let _ = reply.set_auth_reply(self.clone());
        reply
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        Reply::with_action_err(ACTION_AUTH, err_code)
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

    pub fn get_head(&self) -> Head {
        self.head.clone()
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
        // ignore the head in reply
        // res.extend(self.head.as_bytes());
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
            buf.resize(CRC_LENGTH, 0);
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
