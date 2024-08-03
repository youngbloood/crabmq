use super::MessageOperation;
use crate::protocol::{v1::bin_message::BinMessage, Head, Protocol, HEAD_LENGTH};
use anyhow::Result;
use bytes::BytesMut;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, BufReader};

/**
 * Encoding:
 *         2 bytes: head.
 *         1 byte: topic length.
 *         1 byte: channel length.
 *         n bytes: topic name.
 *         n bytes: channel name.
 *         n bytes: BinMessage.
 */
#[derive(Clone, Debug)]
pub struct MessageV1 {
    head: Head,
    topic: String,
    channel: String,
    msg: BinMessage,
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
        let mut res = vec![];
        res.extend(self.head.as_bytes());
        res.extend([self.topic.len() as u8]);
        res.extend([self.channel.len() as u8]);
        res.extend(self.topic.as_bytes());
        res.extend(self.channel.as_bytes());
        res.extend(self.msg.as_bytes());

        res.to_vec()
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
    pub fn get_head(&self) -> Head {
        self.head.clone()
    }

    pub fn set_head(&mut self, head: Head) -> &mut Self {
        self.head = head;
        self
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: &str) -> &mut Self {
        self.topic = topic.to_string();
        self
    }

    pub fn get_channel(&self) -> &str {
        &self.channel
    }

    pub fn set_channel(&mut self, channel: &str) -> &mut Self {
        self.channel = channel.to_string();
        self
    }

    pub fn get_message(&self) -> BinMessage {
        self.msg.clone()
    }

    pub fn set_message(&mut self, msg: BinMessage) -> &mut Self {
        self.msg = msg;
        self
    }

    pub async fn parse_from_vec(bts: &[u8]) -> Result<Self> {
        let mut buf = BufReader::new(bts);
        let mut reader = Pin::new(&mut buf);
        let msgv1 = MessageV1::parse_from_reader(&mut reader).await?;
        Ok(msgv1)
    }

    pub async fn parse_from_reader(reader: &mut Pin<&mut impl AsyncReadExt>) -> Result<Self> {
        let mut buf = BytesMut::new();

        // parse head
        buf.resize(HEAD_LENGTH, 0);
        reader.read_exact(&mut buf).await?;
        let mut msgv1 = MessageV1::default();
        msgv1.set_head(Head::with(
            buf.to_vec().try_into().expect("conver to head failed"),
        ));

        // parse topic length
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        let topic_len = { *buf.first().unwrap() };

        // parse channel length
        buf.resize(1, 0);
        reader.read_exact(&mut buf).await?;
        let channel_len = { *buf.first().unwrap() };

        // parse topic
        if topic_len != 0 {
            buf.resize(topic_len as _, 0);
            reader.read_exact(&mut buf).await?;
            msgv1.set_topic(&String::from_utf8(buf.to_vec())?);
        }

        // parse channel
        if channel_len != 0 {
            buf.resize(channel_len as _, 0);
            reader.read_exact(&mut buf).await?;
            msgv1.set_channel(&String::from_utf8(buf.to_vec())?);
        }

        msgv1.set_message(
            BinMessage::parse_from(reader, 1)
                .await?
                .first()
                .unwrap()
                .to_owned(),
        );

        Ok(msgv1)
    }
}
