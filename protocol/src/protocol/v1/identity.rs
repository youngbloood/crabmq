use std::pin::Pin;

use anyhow::Result;
use bytes::BytesMut;
use rsbit::{BitFlagOperation, BitOperation as _};
use tokio::io::AsyncReadExt;

use super::Head;

const IDENTITY_HEAD_LENGTH: usize = 6;

/**
 * CONST [`IDENTITY_HEAD_LENGTH`] length head info:
 * 1st bytes:
 *          1 bit: is heartbeat
 *          1 bit: has crc
 *          1 bit: is reject
 *          1 bit: *reserve bit*
 *          4 bits: msg number
 * 2-4 bytes: reserve bytes
 * 5th byte: topic length
 * 6th byte: token length
 *
 * EXTEND according the head:
 *          2 bytes: crc value
 *          n bytes: topic name
 *          n bytes: token value
 */
#[derive(Default, Clone)]
struct IdentityHead([u8; IDENTITY_HEAD_LENGTH]);

impl IdentityHead {
    fn set_head_flag(&mut self, index: usize, pos: u8, on: bool) {
        if index >= self.0.len() || pos > 7 {
            return;
        }
        let mut flag = self.0[index];
        if on {
            (&mut flag).set_1(pos);
        } else {
            (&mut flag).set_0(pos);
        }
        self.0[index] = flag;
    }

    pub fn with(head: [u8; IDENTITY_HEAD_LENGTH]) -> Self {
        IdentityHead(head)
    }

    pub fn is_heartbeat(&self) -> bool {
        self.0[0].is_1(7)
    }

    pub fn set_heartbeat(&mut self, hb: bool) -> &mut Self {
        self.set_head_flag(0, 7, hb);
        self
    }

    pub fn has_crc(&self) -> bool {
        self.0[0].is_1(6)
    }

    pub fn set_crc(&mut self, has: bool) -> &mut Self {
        self.set_head_flag(0, 6, has);
        self
    }

    pub fn get_topic_len(&self) -> u8 {
        self.0[4]
    }

    pub fn set_topic_len(&mut self, l: u8) -> &mut Self {
        self.0[4] = l;
        self
    }

    pub fn get_token_len(&self) -> u8 {
        self.0[5]
    }

    pub fn set_token_len(&mut self, l: u8) -> &mut Self {
        self.0[5] = l;
        self
    }
}

#[derive(Default, Clone)]
pub struct Publish {
    head: Head,
    pub_head: IdentityHead,

    crc: u16,
    topic: String,
    token: String,
}

pub async fn parse_protocolv1_publish_from_reader(
    reader: &mut Pin<&mut impl AsyncReadExt>,
    head: Head,
) -> Result<Publish> {
    let mut buf = BytesMut::new();
    buf.resize(IDENTITY_HEAD_LENGTH, 0);
    reader.read_exact(&mut buf).await?;

    let pub_head = IdentityHead::with(
        buf.to_vec()
            .try_into()
            .expect("convert to publish head failed"),
    );

    let mut publish = Publish::default();
    publish.head = head;

    // parse crc
    if pub_head.has_crc() {
        buf.resize(2, 0);
        reader.read_exact(&mut buf).await?;
        publish.crc =
            u16::from_be_bytes(buf.to_vec().try_into().expect("convert to crc vec failed"));
    }

    // parse topic
    if pub_head.get_topic_len() != 0 {
        buf.resize(pub_head.get_topic_len() as _, 0);
        reader.read_exact(&mut buf).await?;
        publish.topic = String::from_utf8(buf.to_vec())?;
    }

    // parse token
    if pub_head.get_token_len() != 0 {
        buf.resize(pub_head.get_token_len() as _, 0);
        reader.read_exact(&mut buf).await?;
        publish.token = String::from_utf8(buf.to_vec())?;
    }

    publish.pub_head = pub_head;

    Ok(publish)
}
