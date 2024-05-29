use anyhow::Result;
use std::ops::Deref;

use crate::message::Message;

/// [`Compress`] is compress the message and uncompress message.
///
/// In order to use less space in disk.
pub trait Compress {
    /// compress the message and return the compressed bytes.
    fn compress(&mut self, _: Message) -> Result<Vec<u8>>;

    /// uncompress the message from the bytes.
    fn uncompress(&mut self, _: &[u8]) -> Result<Message>;
}

pub struct CompressGuard {
    cache: Box<dyn Compress>,
}

impl CompressGuard {
    pub fn new(cache: Box<dyn Compress>) -> Self {
        CompressGuard { cache }
    }
}

impl Deref for CompressGuard {
    type Target = Box<dyn Compress>;

    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}
