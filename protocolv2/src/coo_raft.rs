use crate::{
    COO_RAFT_CONF_CHANGE_REQUEST_INDEX, COO_RAFT_GET_META_REQUEST_INDEX,
    COO_RAFT_GET_META_RESPONSE_INDEX, COO_RAFT_ORIGIN_MESSAGE_INDEX, Decoder, EnDecoder, Encoder,
};
use anyhow::Result;
use std::{any::Any, collections::HashMap};

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct CooRaftGetMetaRequest {
    pub id: u64,
    pub raft_addr: String,
    pub meta: HashMap<String, String>,
}

impl Encoder for CooRaftGetMetaRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooRaftGetMetaRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooRaftGetMetaRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooRaftGetMetaRequest {
    fn index(&self) -> u8 {
        COO_RAFT_GET_META_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct CooRaftGetMetaResponse {
    pub id: u64,
    pub raft_addr: String,
    pub meta: HashMap<String, String>,
}

impl Encoder for CooRaftGetMetaResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooRaftGetMetaResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooRaftGetMetaResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooRaftGetMetaResponse {
    fn index(&self) -> u8 {
        COO_RAFT_GET_META_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// 支持 ConfChange 和 ConfChangeV2
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct CooRaftConfChangeRequest {
    pub version: u8,
    pub message: Vec<u8>,
}

impl Encoder for CooRaftConfChangeRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooRaftConfChangeRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooRaftConfChangeRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooRaftConfChangeRequest {
    fn index(&self) -> u8 {
        COO_RAFT_CONF_CHANGE_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// raft 原生信息，对应 raft::eraftpb::Message
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct CooRaftOriginMessage {
    pub message: Vec<u8>,
}

impl Encoder for CooRaftOriginMessage {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooRaftOriginMessage {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooRaftOriginMessage, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooRaftOriginMessage {
    fn index(&self) -> u8 {
        COO_RAFT_ORIGIN_MESSAGE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// 向 raft 集群中提案的信息
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct CooRaftProposeMessage {
    pub index: u8,
    pub message: Vec<u8>,
}

impl Encoder for CooRaftProposeMessage {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooRaftProposeMessage {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooRaftProposeMessage, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooRaftProposeMessage {
    fn index(&self) -> u8 {
        COO_RAFT_ORIGIN_MESSAGE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
