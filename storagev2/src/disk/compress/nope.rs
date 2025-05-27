use super::Compress;
use anyhow::Result;
use bytes::Bytes;

pub struct Nope;

#[async_trait::async_trait]
impl Compress for Nope {
    async fn compress(&self, msg: Bytes) -> Result<Bytes> {
        Ok(msg)
    }

    async fn decompress(&self, bts: &[u8]) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(bts))
    }
}
