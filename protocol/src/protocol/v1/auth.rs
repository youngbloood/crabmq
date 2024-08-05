use crate::consts::ACTION_AUTH;
use crate::protocol::Head;
use anyhow::Result;
use bytes::BytesMut;
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

use super::{new_v1_head, PROPTOCOL_V1};

pub const AUTH_HEAD_LENGTH: usize = 5;

#[derive(Default)]
pub struct AuthType(u8);

impl AuthType {
    pub fn with(a: u8) -> Self {
        AuthType(a)
    }
}

/**
 * 1st byte: AuthType
 * 2nd byte: username length.
 * 3-4 bytes: u16: password length.
 * 5th byte: salt length.
 */
#[derive(Default)]
pub struct AuthHead {
    auth_type: AuthType,
    username_len: u8,
    password_len: u16,
    salt_len: u8,
}

impl Deref for AuthHead {
    type Target = AuthType;

    fn deref(&self) -> &Self::Target {
        &self.auth_type
    }
}

impl AuthHead {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.push(self.auth_type.0);
        res.extend(self.username_len.to_be_bytes());
        res.push(self.salt_len);
        res
    }

    pub fn get_username_len(&self) -> u8 {
        self.username_len
    }

    pub fn set_username_len(&mut self, l: u8) -> &mut Self {
        self.username_len = l;
        self
    }

    pub fn get_password_len(&self) -> u16 {
        self.password_len
    }

    pub fn set_password_len(&mut self, l: u16) -> &mut Self {
        self.password_len = l;
        self
    }

    pub fn get_salt_len(&self) -> u8 {
        self.salt_len
    }

    pub fn set_salt_len(&mut self, l: u8) -> &mut Self {
        self.salt_len = l;
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

        self.auth_type = AuthType::with(buf[0]);
        self.username_len = buf[1];
        self.password_len = u16::from_be_bytes(
            buf[2..4]
                .to_vec()
                .try_into()
                .expect("conver to password len failed"),
        );
        self.salt_len = buf[4];
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
