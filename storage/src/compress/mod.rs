mod nope;

use anyhow::Result;
use nope::Nope;
use protocol::message::Message;

use std::ops::Deref;

pub const COMPRESS_TYPE_NONE: &str = "no";

/// [`Compress`] is compress the message and uncompress message.
///
/// In order to use less space in disk.
#[async_trait::async_trait]
pub trait Compress: Send + Sync {
    /// compress the message and return the compressed bytes.
    async fn compress(&self, _: Message) -> Result<Vec<u8>>;

    /// decompress the message from the bytes.
    async fn decompress(&self, _: &[u8]) -> Result<Message>;
}

pub struct CompressWrapper {
    inner: Box<dyn Compress>,
}

impl CompressWrapper {
    pub fn new(cache: Box<dyn Compress>) -> Self {
        CompressWrapper { inner: cache }
    }

    pub fn with_type(t: &str) -> Self {
        match t {
            COMPRESS_TYPE_NONE => CompressWrapper {
                inner: Box::new(Nope) as Box<dyn Compress>,
            },
            _ => CompressWrapper {
                inner: Box::new(Nope) as Box<dyn Compress>,
            },
        }
    }
}

impl Deref for CompressWrapper {
    type Target = Box<dyn Compress>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
