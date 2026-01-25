use crate::{Decoder, EnDecoder, Encoder};
use anyhow::Result;
use std::{any::Any, collections::HashMap};

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub enum ErrorCode {
    // raft id conflict
    RaftIDConflict = 1,
    // broker id conflict
    BrokerIDConflict = 2,
}

// ErrorResponse contains all error code and message and metadata
#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub message: String,
    pub meta: Option<HashMap<String, String>>,
}

impl Encoder for ErrorResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ErrorResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ErrorResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ErrorResponse {
    fn index(&self) -> u8 {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
