use super::MessageOperation;
use crate::protocol::{v1::bin_message::BinMessage, Head, Protocol};
use anyhow::Result;

#[derive(Clone, Debug)]
pub struct MessageV1 {
    head: Head,
    topic: String,
    channel: String,
    msg: Vec<BinMessage>,
}

impl Default for MessageV1 {
    fn default() -> Self {
        Self {
            head: Head::default(),
            topic: "default".to_string(),
            channel: "default".to_string(),
            msg: Default::default(),
        }
    }
}

impl MessageOperation for MessageV1 {
    fn as_bytes(&self) -> Vec<u8> {
        todo!()
    }
    fn convert_to_protocol(&self) -> Protocol {
        todo!()
    }
    fn get_topic(&self) -> &str {
        todo!()
    }
    fn get_channel(&self) -> &str {
        todo!()
    }
    fn get_id(&self) -> &str {
        todo!()
    }
    fn defer_time(&self) -> u64 {
        todo!()
    }
    fn is_update(&self) -> bool {
        todo!()
    }
    fn is_deleted(&self) -> bool {
        todo!()
    }
    fn is_consumed(&self) -> bool {
        todo!()
    }
    fn is_notready(&self) -> bool {
        todo!()
    }
}

impl MessageV1 {
    pub async fn parse_from(_: &[u8]) -> Result<Self> {
        todo!()
    }
}
