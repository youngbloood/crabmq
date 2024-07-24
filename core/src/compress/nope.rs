use super::Compress;
use anyhow::Result;
use protocol::message::Message;

pub struct Nope;

#[async_trait::async_trait]
impl Compress for Nope {
    async fn compress(&self, msg: Message) -> Result<Vec<u8>> {
        Ok(msg.as_bytes())
    }

    async fn decompress(&self, bts: &[u8]) -> Result<Message> {
        let msg = Message::parse_from(bts).await?;
        Ok(msg)
    }
}
