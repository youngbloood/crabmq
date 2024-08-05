pub mod auth;
pub mod identity;
pub mod publish;
pub mod reply;
pub mod subscribe;
pub mod touch;

use super::{Builder, Head, Protocol, ProtocolOperation};
use crate::{consts::*, message::Message};
use anyhow::{anyhow, Result};
use identity::Identity;
use publish::Publish;
use reply::{Reply, ReplyBuilder};
use std::pin::Pin;
use subscribe::Subscribe;
use tokio::io::AsyncReadExt;
use touch::Touch;

pub trait BuilderV1 {
    fn buildv1(self) -> V1;
}

#[derive(Default, Clone, Debug)]
pub struct V1 {
    head: Head,
    identity: Option<Identity>,
    publish: Option<Publish>,
    subscribe: Option<Subscribe>,
    touch: Option<Touch>,
    reply: Option<Reply>,
}

impl ReplyBuilder for V1 {
    fn build_reply_ok(&self) -> Reply {
        match self.head.get_action() {
            ACTION_PUBLISH => self.publish.as_ref().unwrap().build_reply_ok(),
            _ => Reply::default(),
        }
    }

    fn build_reply_err(&self, err_code: u8) -> Reply {
        match self.head.get_action() {
            ACTION_PUBLISH => self.publish.as_ref().unwrap().build_reply_err(err_code),
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
            ACTION_PUBLISH => self.publish.as_ref().unwrap().as_bytes(),
            ACTION_REPLY => self.reply.as_ref().unwrap().as_bytes(),
            _ => unimplemented!(),
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

    pub fn get_reply(&self) -> Option<Reply> {
        self.reply.clone()
    }

    pub fn set_reply(&mut self, r: Reply) -> &mut Self {
        self.reply = Some(r);
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
            ACTION_REPLY => Ok(Reply::parse_from(reader, head).await?.buildv1()),
            // TODO:
            ACTION_MSG => unimplemented!(),
            ACTION_RESET => unimplemented!(),
            ACTION_UPDATE => unimplemented!(),
            ACTION_CLOSE => unimplemented!(),
            ACTION_FIN => unimplemented!(),
            _ => unreachable!(),
        }
    }
}

pub fn new_v1_head(action: u8) -> Head {
    let mut head = Head::default();
    head.set_action(action).set_version(PROPTOCOL_V1);
    head
}
