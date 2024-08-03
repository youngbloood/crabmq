pub mod auth;
pub mod bin_message;
pub mod common_reply;
pub mod identity;
pub mod publish;
pub mod subscribe;
pub mod touch;

use super::{Builder, Head, Protocol, ProtolOperation};
use crate::message::Message;
use anyhow::{anyhow, Result};
use identity::Identity;
use publish::Publish;
use std::pin::Pin;
use subscribe::Subscribe;
use tokio::io::AsyncReadExt;
use touch::Touch;

// ========= PROTOCOL ACTION =========
// even number denote the Client -> Server
// odd number denote the Server -> Client
const ACTION_IDENTITY: u8 = 0;
const ACTION_IDENTITY_REPLY: u8 = 1;

const ACTION_AUTH: u8 = 2;
const ACTION_AUTH_REPLY: u8 = 3;

/// reply is [`ACTION_COMMON_REPLY`]
const ACTION_TOUCH: u8 = 4;

/// reply is [`ACTION_COMMON_REPLY`]
const ACTION_PUBLISH: u8 = 6;

/// reply is [`ACTION_COMMON_REPLY`]
const ACTION_SUBSCRIBE: u8 = 8;

/// subs重置超时时间
///
/// no reply
const ACTION_RESET: u8 = 11;

/// pubs更新msg的元信息
///
/// reply is [`ACTION_COMMON_REPLY`]
const ACTION_UPDATE: u8 = 12;

/// 客户端主动关闭链接
///
/// no reply
const ACTION_CLOSE: u8 = 14;

/// subs端标记一个消息被完整处理
///
/// no reply
const ACTION_FIN: u8 = 16;

// server给subs发送消息
const ACTION_MSG: u8 = 17;

// Server 通用响应
const ACTION_COMMON_REPLY: u8 = 19;
// ========= PROTOCOL ACTION =========

pub trait BuilderV1 {
    fn buildv1(self) -> V1;
}

#[derive(Default, Clone, Debug)]
pub struct V1 {
    pub head: Head,
    identity: Option<Identity>,
    publish: Option<Publish>,
    subscribe: Option<Subscribe>,
    touch: Option<Touch>,
}

impl ProtolOperation for V1 {
    fn get_version(&self) -> u8 {
        self.head.get_version()
    }

    fn get_action(&self) -> u8 {
        self.head.get_action()
    }

    fn convert_to_message(&self) -> Result<Message> {
        todo!();
    }
}

impl Builder for V1 {
    fn build(self) -> Protocol {
        Protocol::V1(self)
    }
}

impl V1 {
    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_identity(&self) -> Option<Identity> {
        self.identity.clone()
    }

    pub fn set_identity(&mut self, i: Identity) -> &mut Self {
        self.identity = Some(i);
        self
    }

    pub fn get_publish(&self) -> Option<Publish> {
        self.publish.clone()
    }

    pub fn set_publish(&mut self, p: Publish) -> &mut Self {
        self.publish = Some(p);
        self
    }

    pub fn get_subscribe(&self) -> Option<Subscribe> {
        self.subscribe.clone()
    }

    pub fn set_subscribe(&mut self, s: Subscribe) -> &mut Self {
        self.subscribe = Some(s);
        self
    }

    pub fn get_touch(&self) -> Option<Touch> {
        self.touch.clone()
    }

    pub fn set_touch(&mut self, t: Touch) -> &mut Self {
        self.touch = Some(t);
        self
    }

    pub fn validate_for_server(&self) -> Result<()> {
        if self.get_action() % 2 != 0 {
            return Err(anyhow!("illigal action"));
        }
        Ok(())
    }

    pub fn validate_for_client(&self) -> Result<()> {
        if self.get_action() % 2 == 0 {
            return Err(anyhow!("illigal action"));
        }
        Ok(())
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<V1> {
        match head.get_action() {
            ACTION_IDENTITY => Ok(Identity::parse_from(reader, head).await?.buildv1()),
            ACTION_TOUCH => Ok(Touch::parse_from(reader, head).await?.buildv1()),
            ACTION_PUBLISH => Ok(Publish::parse_from(reader, head).await?.buildv1()),
            ACTION_SUBSCRIBE => Ok(Subscribe::parse_from(reader, head).await?.buildv1()),

            _ => unreachable!(),
        }
    }
}
