use super::{new_v1_head, BuilderV1, ACTION_MSG, V1};
use crate::{
    message::v1::MessageUserV1,
    protocol::{Builder, Head, Protocol},
};
use anyhow::{anyhow, Result};
use std::pin::Pin;
use tokio::io::AsyncReadExt;

/**
 * [`DispatchMessage`] is a message that dispatch to subscribers.
 */
#[derive(Clone, Debug)]
pub struct DispatchMessage {
    head: Head,
    msg: MessageUserV1,
}

impl Default for DispatchMessage {
    fn default() -> Self {
        Self {
            head: new_v1_head(ACTION_MSG),
            msg: Default::default(),
        }
    }
}

impl BuilderV1 for DispatchMessage {
    fn buildv1(self) -> super::V1 {
        let mut v1 = V1::default();
        v1.set_msg(self);
        v1
    }
}

impl Builder for DispatchMessage {
    fn build(self) -> Protocol {
        Protocol::V1(Self::buildv1(self))
    }
}

impl DispatchMessage {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend(self.msg.as_bytes());
        res
    }

    pub fn validate(&self) -> Result<()> {
        self.msg.validate()?;
        Ok(())
    }
    pub fn set_msg(&mut self, msg: MessageUserV1) -> &mut Self {
        self.msg = msg;
        self
    }

    pub async fn parse_from(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut msg_userv1 = MessageUserV1::parse_from(reader, 1).await?;
        if msg_userv1.len() != 1 {
            return Err(anyhow!("the message length not 1"));
        }
        let mut msg = Self::default();
        msg.set_msg(msg_userv1.pop().unwrap());
        Ok(msg)
    }
}
