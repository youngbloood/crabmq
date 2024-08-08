pub mod auth;
pub mod dispatch_message;
pub mod identity;
pub mod patch;
pub mod publish;
pub mod reply;
pub mod subscribe;
pub mod touch;

use super::{Builder, Head, Protocol, ProtocolOperation};
use crate::{consts::*, error::*, message::Message};
use anyhow::{anyhow, Result};
use auth::Auth;
use dispatch_message::DispatchMessage;
use identity::Identity;
use patch::Patch;
use publish::Publish;
use reply::{Reply, ReplyBuilder};
use std::pin::Pin;
use subscribe::Subscribe;
use tokio::io::AsyncReadExt;
use touch::Touch;

pub trait BuilderV1 {
    fn buildv1(self) -> V1;
}

#[derive(Clone, Debug)]
pub struct V1 {
    head: Head,
    identity: Option<Identity>,
    auth: Option<Auth>,
    publish: Option<Publish>,
    subscribe: Option<Subscribe>,
    touch: Option<Touch>,
    msg: Option<DispatchMessage>,
    patch: Option<Patch>,

    reply: Option<Reply>,
}

impl Default for V1 {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_IDENTITY),
            identity: Default::default(),
            auth: Default::default(),
            publish: Default::default(),
            subscribe: Default::default(),
            touch: Default::default(),
            msg: Default::default(),
            patch: Default::default(),
            reply: Default::default(),
        }
    }
}

impl ReplyBuilder for V1 {
    fn build_reply_ok(&self) -> Reply {
        match self.head.get_action() {
            ACTION_IDENTITY => self.identity.as_ref().unwrap().build_reply_ok(),
            ACTION_AUTH => self.auth.as_ref().unwrap().build_reply_ok(),
            ACTION_TOUCH => self.touch.as_ref().unwrap().build_reply_ok(),
            ACTION_PUBLISH => self.publish.as_ref().unwrap().build_reply_ok(),
            ACTION_SUBSCRIBE => self.subscribe.as_ref().unwrap().build_reply_ok(),
            ACTION_PATCH => self.patch.as_ref().unwrap().build_reply_ok(),
            _ => unreachable!(),
        }
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        match self.head.get_action() {
            ACTION_IDENTITY => self.identity.as_ref().unwrap().build_reply_err(err_code),
            ACTION_AUTH => self.auth.as_ref().unwrap().build_reply_err(err_code),
            ACTION_TOUCH => self.touch.as_ref().unwrap().build_reply_err(err_code),
            ACTION_PUBLISH => self.publish.as_ref().unwrap().build_reply_err(err_code),
            ACTION_SUBSCRIBE => self.subscribe.as_ref().unwrap().build_reply_err(err_code),
            ACTION_PATCH => self.patch.as_ref().unwrap().build_reply_err(err_code),
            _ => Reply::default(),
        }
    }
}

impl ProtocolOperation for V1 {
    fn get_version(&self) -> u8 {
        self.head.get_version()
    }

    fn get_action(&self) -> u8 {
        self.head.get_action()
    }

    fn convert_to_message(&self) -> Result<Vec<Message>> {
        match self.head.get_action() {
            ACTION_PUBLISH => {
                if let Some(p) = self.publish.as_ref() {
                    return Ok(p.split_message());
                }
                Err(anyhow!("not found publish"))
            }
            _ => Err(anyhow!("not found publish")),
        }
    }

    fn as_bytes(&self) -> Vec<u8> {
        match self.head.get_action() {
            ACTION_IDENTITY => self.identity.as_ref().unwrap().as_bytes(),
            ACTION_AUTH => self.auth.as_ref().unwrap().as_bytes(),
            ACTION_TOUCH => self.touch.as_ref().unwrap().as_bytes(),
            ACTION_PUBLISH => self.publish.as_ref().unwrap().as_bytes(),
            ACTION_SUBSCRIBE => self.subscribe.as_ref().unwrap().as_bytes(),
            ACTION_MSG => self.msg.as_ref().unwrap().as_bytes(),
            ACTION_PATCH => self.patch.as_ref().unwrap().as_bytes(),
            ACTION_REPLY => self.reply.as_ref().unwrap().as_bytes(),
            _ => unreachable!(),
        }
    }

    fn validate_for_server(&self) -> Result<()> {
        if self.get_action() % 2 != 0 {
            return Err(ProtError::new(E_ACTION_NOT_SUPPORT).into());
        }

        match self.get_action() {
            ACTION_IDENTITY => self.identity.as_ref().unwrap().validate(),
            ACTION_AUTH => self.auth.as_ref().unwrap().validate(),
            ACTION_TOUCH => self.touch.as_ref().unwrap().validate(),
            ACTION_PUBLISH => self.publish.as_ref().unwrap().validate(),
            ACTION_SUBSCRIBE => self.subscribe.as_ref().unwrap().validate(),
            ACTION_PATCH => self.patch.as_ref().unwrap().validate(),
            _ => Err(ProtError::new(E_ACTION_NOT_SUPPORT).into()),
        }
    }

    fn validate_for_client(&self) -> Result<()> {
        if self.get_action() % 2 == 0 {
            return Err(ProtError::new(E_ACTION_NOT_SUPPORT).into());
        }

        match self.get_action() {
            ACTION_MSG => self.msg.as_ref().unwrap().validate(),
            ACTION_REPLY => self.reply.as_ref().unwrap().validate(),
            _ => Err(ProtError::new(E_ACTION_NOT_SUPPORT).into()),
        }
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
        self.head.set_action(ACTION_IDENTITY);
        self
    }

    pub fn get_auth(&self) -> Option<Auth> {
        self.auth.clone()
    }

    pub fn set_auth(&mut self, a: Auth) -> &mut Self {
        self.auth = Some(a);
        self.head.set_action(ACTION_AUTH);
        self
    }

    pub fn get_publish(&self) -> Option<Publish> {
        self.publish.clone()
    }

    pub fn set_publish(&mut self, p: Publish) -> &mut Self {
        self.publish = Some(p);
        self.head.set_action(ACTION_PUBLISH);
        self
    }

    pub fn get_subscribe(&self) -> Option<Subscribe> {
        self.subscribe.clone()
    }

    pub fn set_subscribe(&mut self, s: Subscribe) -> &mut Self {
        self.subscribe = Some(s);
        self.head.set_action(ACTION_SUBSCRIBE);
        self
    }

    pub fn get_touch(&self) -> Option<Touch> {
        self.touch.clone()
    }

    pub fn set_touch(&mut self, t: Touch) -> &mut Self {
        self.touch = Some(t);
        self.head.set_action(ACTION_TOUCH);
        self
    }

    pub fn get_msg(&self) -> Option<DispatchMessage> {
        self.msg.clone()
    }

    pub fn set_msg(&mut self, m: DispatchMessage) -> &mut Self {
        self.msg = Some(m);
        self.head.set_action(ACTION_MSG);
        self
    }

    pub fn get_patch(&self) -> Option<Patch> {
        self.patch.clone()
    }

    pub fn set_patch(&mut self, p: Patch) -> &mut Self {
        self.patch = Some(p);
        self.head.set_action(ACTION_PATCH);
        self
    }

    pub fn get_reply(&self) -> Option<Reply> {
        self.reply.clone()
    }

    pub fn set_reply(&mut self, r: Reply) -> &mut Self {
        self.reply = Some(r);
        self.head.set_action(ACTION_REPLY);
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>, head: Head) -> Result<V1> {
        match head.get_action() {
            ACTION_IDENTITY => Ok(Identity::parse_from(reader).await?.buildv1()),
            ACTION_AUTH => Ok(Auth::parse_from(reader).await?.buildv1()),
            ACTION_TOUCH => Ok(Touch::parse_from(reader).await?.buildv1()),
            ACTION_PUBLISH => Ok(Publish::parse_from(reader).await?.buildv1()),
            ACTION_SUBSCRIBE => Ok(Subscribe::parse_from(reader).await?.buildv1()),
            ACTION_MSG => Ok(DispatchMessage::parse_from(reader).await?.buildv1()),
            ACTION_PATCH => Ok(Patch::parse_from(reader).await?.buildv1()),
            ACTION_REPLY => Ok(Reply::parse_from(reader).await?.buildv1()),
            _ => unreachable!(),
        }
    }
}

pub fn new_v1_head(action: u8) -> Head {
    let mut head = Head::default();
    head.set_action(action).set_version(PROPTOCOL_V1);
    head
}
