use super::{new_v1_head, ACTION_MSG};
use crate::{message::v1::MessageUserV1, protocol::Head};
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

impl DispatchMessage {
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
