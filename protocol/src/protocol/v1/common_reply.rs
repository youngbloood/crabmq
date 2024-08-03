use super::ACTION_COMMON_REPLY;
use crate::protocol::Head;
use anyhow::Result;
use bytes::BytesMut;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use tokio::io::AsyncReadExt;

pub trait ReplyBuilder {
    fn build_reply_ok(&self) -> Reply;
    fn build_reply_err(&self, err_code: u8) -> Reply;
}

#[derive(Debug)]
pub struct Reply {
    head: Head,
    /// action type: 表示返回的操作类型
    action_type: u8,
    /// err_code: 表示返回的错误类型，为0表示正确
    err_code: u8,
}

impl Default for Reply {
    fn default() -> Self {
        let mut head = Head::default();
        head.set_action(ACTION_COMMON_REPLY);
        Self {
            head,
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

impl DerefMut for Reply {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.head
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

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<Self> {
        let mut reply = Reply::default();
        reply.set_head(head);
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
