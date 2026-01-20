use std::any::Any;

use crate::{
    BROKER_COO_HEARTBEAT_REQUEST_INDEX, BROKER_COO_HEARTBEAT_RESPONSE_INDEX, Decoder, EnDecoder,
    Encoder, common::Topics,
};
use anyhow::Result;

// Broker COO 心跳请求
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct BrokerCooHeartbeatRequest {
    pub broker_id: u64,
    pub broker_addr: String,
    pub version: String,

    // 该 broker 网络速率
    pub netrate: u32,
    // 该 broker 的 cpu 占用率
    pub cpurate: u32,
    // 该 broker 的内存占用率
    pub memrate: u32,
    // 该 broker 的磁盘占用率
    pub diskrate: u32,

    // 该 broker 的订阅连接数
    pub sub_count: u32,
    // 该 broker 的发布连接数
    pub pub_count: u32,
    // 该 broker 的时间戳
    pub timestamp: u64,
}

impl Encoder for BrokerCooHeartbeatRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for BrokerCooHeartbeatRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (BrokerCooHeartbeatRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for BrokerCooHeartbeatRequest {
    fn index(&self) -> u8 {
        BROKER_COO_HEARTBEAT_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Broker COO 心跳响应
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct BrokerCooHeartbeatResponse {
    pub code: u16,
    pub message: String,
    pub topics: Topics,
}

impl Encoder for BrokerCooHeartbeatResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for BrokerCooHeartbeatResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (BrokerCooHeartbeatResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for BrokerCooHeartbeatResponse {
    fn index(&self) -> u8 {
        BROKER_COO_HEARTBEAT_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
