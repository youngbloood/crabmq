pub mod aggregation;
pub mod err;
pub mod v1;

use anyhow::Result;
use lazy_static::lazy_static;
use std::{any::Any, collections::HashMap, fmt::Debug};

// Include generated Protobuf code
pub mod pbv1 {
    include!(concat!(env!("OUT_DIR"), "/crabmq.protocol.v1.rs"));
}

pub use pbv1::*;

pub const VERSION1: u8 = 1;

pub trait Encoder {
    fn encode(&self) -> Result<Vec<u8>>;
}

pub trait Decoder {
    fn decode(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl<T> Encoder for T
where
    T: prost::Message,
{
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.encode_to_vec())
    }
}

impl<T> Decoder for T
where
    T: prost::Message + Default,
{
    fn decode(data: &[u8]) -> Result<Self> {
        Ok(T::decode(data)?)
    }
}

pub trait EnDecoder: Encoder + Decoder + Send + Sync + Debug {
    fn index(&self) -> u16;
    fn as_any(&self) -> &dyn Any;
}

// ============================================================================
// Message Decoder Registry (消息解码器注册表)
// Key: (version, index)，按协议版本 + 消息类型索引分派；各版本通过 v1::decoders() 等注入
// ============================================================================

pub type MessageDecoder = fn(&[u8]) -> Result<Box<dyn EnDecoder>>;

lazy_static! {
    static ref DECODER_REGISTRY: HashMap<(u8, u16), MessageDecoder> = {
        let mut m = HashMap::new();
        m.extend(v1::decoders());
        m
    };
}

/// Decode a message by version and index.
///
/// Looks up the decoder from the registry with `(version, index)` and decodes the message.
///
/// # Arguments
/// * `version` - Protocol version (e.g. `VERSION1`)
/// * `index` - Message type index
/// * `data` - Encoded message body
///
/// # Returns
/// A boxed `EnDecoder` trait object, or an error if version/index is unsupported or decode fails.
pub fn decode_message(version: u8, index: u16, data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let key = (version, index);
    let decoder = DECODER_REGISTRY.get(&key).ok_or_else(|| {
        anyhow::anyhow!(
            "Unsupported protocol version or unknown message type: version={}, index={}",
            version,
            index
        )
    })?;
    decoder(data)
}
