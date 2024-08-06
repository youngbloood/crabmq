use super::{new_v1_head, BuilderV1, V1};
use crate::consts::ACTION_REPLY;
use crate::protocol::Protocol;
use crate::protocol::{Builder, Head};
use anyhow::Result;
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
}

impl Default for Reply {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_REPLY),
            action_type: Default::default(),
            err_code: Default::default(),
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

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn set_action_type(&mut self, action: u8) -> &mut Self {
        self.action_type = action;
        self
    }

    pub fn set_err_code(&mut self, code: u8) -> &mut Self {
        self.err_code = code;
        self
    }

    pub fn is_ok(&self) -> bool {
        self.err_code == 0
    }

    pub fn is_err(&self) -> bool {
        self.err_code != 0
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.push(self.action_type);
        res.push(self.err_code);

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

        Ok(())
    }
}
