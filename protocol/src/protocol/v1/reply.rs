use super::auth::AuthReply;
use super::identity::IdentityReply;
use super::{new_v1_head, BuilderV1, ACTION_AUTH, ACTION_IDENTITY, OK, V1};
use crate::consts::ACTION_REPLY;
use crate::protocol::Protocol;
use crate::protocol::{Builder, Head};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use std::ops::Deref;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

#[enum_dispatch]
pub trait ReplyBuilder {
    fn build_reply_ok(&self) -> Reply;
    fn build_reply_err(&self, err_code: u8) -> Reply;
}

#[derive(Debug, Clone)]
pub struct Reply {
    head: Head,
    /// action type: 表示返回的操作类型
    action_type: u8,
    /// err_code: 表示返回的错误类型，为0表示正确
    err_code: u8,

    /// action_type = [`ACTION_IDENTITY`] 时，且 err_code = 0 时有值
    identity_reply: Option<IdentityReply>,

    /// action_type = [`ACTION_AUTH`] 时，且 err_code = 0 时有值
    auth_reply: Option<AuthReply>,
}

impl Default for Reply {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_REPLY),
            action_type: Default::default(),
            err_code: Default::default(),
            identity_reply: None,
            auth_reply: None,
        }
    }
}

impl Deref for Reply {
    type Target = Head;

    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

impl BuilderV1 for Reply {
    fn buildv1(self) -> V1 {
        let mut v1 = V1::default();
        v1.set_head(self.head.clone()).set_reply(self);
        v1
    }
}

impl Builder for Reply {
    fn build(self) -> Protocol {
        Protocol::V1(self.buildv1())
    }
}

impl Reply {
    pub fn with_ok(resp_type: u8) -> Self {
        let mut reply = Self::default();
        reply.set_action_type(resp_type);
        reply
    }

    pub fn with_action_err(resp_type: u8, err_code: u8) -> Self {
        let mut reply = Self::default();
        reply.set_action_type(resp_type).set_err_code(err_code);
        reply
    }

    // pub fn set_head(&mut self, head: Head) -> &mut Self {
    //     self.head = head;
    //     self
    // }

    pub fn get_action_type(&self) -> u8 {
        self.action_type
    }

    pub fn set_action_type(&mut self, action: u8) -> &mut Self {
        self.action_type = action;
        self
    }

    pub fn get_err_code(&self) -> u8 {
        self.err_code
    }

    pub fn set_err_code(&mut self, code: u8) -> &mut Self {
        self.err_code = code;
        self
    }

    pub fn get_identity_reply(&self) -> Option<IdentityReply> {
        self.identity_reply.clone()
    }

    pub fn set_identity_reply(&mut self, r: IdentityReply) -> Result<()> {
        if self.action_type != ACTION_IDENTITY || !self.is_ok() {
            return Err(anyhow!("can't set the identity_reply"));
        }
        self.identity_reply = Some(r);
        Ok(())
    }

    pub fn get_auth_reply(&self) -> Option<AuthReply> {
        self.auth_reply.clone()
    }

    pub fn set_auth_reply(&mut self, a: AuthReply) -> Result<()> {
        if self.action_type != ACTION_AUTH || !self.is_ok() {
            return Err(anyhow!("can't set the auth_reply"));
        }
        self.auth_reply = Some(a);
        Ok(())
    }

    pub fn is_ok(&self) -> bool {
        self.err_code == OK
    }

    pub fn is_err(&self) -> bool {
        self.err_code != OK
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.push(self.action_type);
        res.push(self.err_code);

        if self.get_action_type() == ACTION_IDENTITY
            && self.is_ok()
            && self.identity_reply.is_some()
        {
            res.extend(self.identity_reply.as_ref().unwrap().as_bytes());
        }

        if self.get_action_type() == ACTION_AUTH && self.is_ok() && self.auth_reply.is_some() {
            res.extend(self.auth_reply.as_ref().unwrap().as_bytes());
        }

        res
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut reply = Reply::default();
        reply.parse_reader(reader).await?;
        Ok(reply)
    }

    pub async fn parse_reader(&mut self, reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<()> {
        let mut buf = BytesMut::new();

        // parse action
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        self.set_action_type(*buf.first().unwrap());

        // parse err_code
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        self.set_err_code(*buf.first().unwrap());

        if self.get_action_type() == ACTION_IDENTITY && self.is_ok() {
            let identity_reply = IdentityReply::parse_from(reader).await?;
            let _ = self.set_identity_reply(identity_reply);
        }

        if self.get_action_type() == ACTION_AUTH && self.is_ok() {
            let auth_reply = AuthReply::parse_from(reader).await?;
            let _ = self.set_auth_reply(auth_reply);
        }

        Ok(())
    }
}
