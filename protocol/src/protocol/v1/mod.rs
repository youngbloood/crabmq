pub mod bin_message;
pub mod identity;
pub mod publish;
pub mod subscribe;
pub mod touch;

use crate::message::Message;

use super::{Head, ProtolOperation};
use anyhow::Result;
use publish::Publish;
use std::pin::Pin;
use subscribe::Subscribe;
use tokio::io::AsyncReadExt;
use touch::Touch;

pub const PROPTOCOL_V1: u8 = 1;

#[derive(Default, Clone, Debug)]
pub struct V1 {
    pub head: Head,
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

impl V1 {
    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
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
}

pub async fn parse_protocolv1_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
    head: Head,
) -> Result<V1> {
    match head.get_action() {
        1 => {
            let mut v1 = V1::default();
            v1.set_head(head.clone())
                .set_publish(Publish::parse_from(reader, head).await?);
            Ok(v1)
        }

        2 => {
            let mut v1 = V1::default();
            v1.set_head(head.clone())
                .set_subscribe(Subscribe::parse_from(reader, head).await?);
            Ok(v1)
        }

        _ => unreachable!(),
    }
}
