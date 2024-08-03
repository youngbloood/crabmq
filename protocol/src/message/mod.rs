pub mod v1;

use crate::protocol::{v1::touch::Touch as TouchV1, Protocol};
use anyhow::Result;
use enum_dispatch::enum_dispatch;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

#[enum_dispatch]
pub trait MessageOperation {
    fn as_bytes(&self) -> Vec<u8>;
    fn convert_to_protocol(&self) -> Protocol;
    fn get_topic(&self) -> &str;
    fn get_channel(&self) -> &str;
    fn get_id(&self) -> &str;
    fn defer_time(&self) -> u64;
    fn is_update(&self) -> bool;
    fn is_deleted(&self) -> bool;
    fn is_consumed(&self) -> bool;
    fn is_notready(&self) -> bool;
}

#[derive(Clone, Debug)]
#[enum_dispatch(MessageOperation)]
pub enum Message {
    V1(v1::MessageV1),
}
impl Default for Message {
    fn default() -> Self {
        Message::V1(v1::MessageV1::default())
    }
}

impl Message {
    pub async fn parse_from_reader(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        todo!()
    }

    pub async fn parse_from_vec(_: &[u8]) -> Result<Self> {
        todo!()
    }
}

pub enum TopicCustom {
    V1(TouchV1),
}
