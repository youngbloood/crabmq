use anyhow::Result;
use lazy_static::lazy_static;
use prost::Message;
use std::{any::Any, collections::HashMap, fmt::Debug};

pub mod aggregation;
pub mod broker_coo;
pub mod client_broker;
pub mod client_coo;
pub mod common;
pub mod coo_raft;
pub mod err;

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
// 消息类型索引 (Message Type Indexes)
// ============================================================================
// 命名规则：
// 1. coo <-> coo 的交互消息为 [1,2]x
// 2. broker <-> coo 的交互消息为 [3,4]x。%2==1，表示 broker->coo 的消息； %2==0，表示 coo->broker 的消息
// 3. broker <-> broker 的交互消息为 [5,6]x
// 4. client <-> broker 的交互消息为 [7,8]x。%2==1，表示 client->broker 的消息； %2==0，表示 broker->client 的消息
// 5. client <-> coo的交互消息为 [9,10]x

pub const COO_RAFT_GET_META_REQUEST_INDEX: u16 = 1;
pub const COO_RAFT_GET_META_RESPONSE_INDEX: u16 = 2;
pub const COO_RAFT_CONF_CHANGE_REQUEST_INDEX: u16 = 3;
pub const COO_RAFT_ORIGIN_MESSAGE_INDEX: u16 = 4;
pub const COO_RAFT_PROPOSE_MESSAGE_INDEX: u16 = 5;
pub const BROKER_COO_HEARTBEAT_REQUEST_INDEX: u16 = 30;
pub const BROKER_COO_HEARTBEAT_RESPONSE_INDEX: u16 = 31;

pub const CLIENT_COO_AUTH_REQUEST_INDEX: u16 = 90;
pub const CLIENT_COO_AUTH_RESPONSE_INDEX: u16 = 91;
pub const CLIENT_COO_HEARTBEAT_REQUEST_INDEX: u16 = 92;
pub const CLIENT_COO_HEARTBEAT_RESPONSE_INDEX: u16 = 93;
pub const CLIENT_COO_NEW_TOPIC_REQUEST_INDEX: u16 = 94;
pub const CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX: u16 = 95;
pub const CLIENT_COO_ADD_PARTITION_REQUEST_INDEX: u16 = 96;
pub const CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX: u16 = 97;
pub const CLIENT_COO_SUB_REQUEST_INDEX: u16 = 98;
pub const CLIENT_COO_SUB_RESPONSE_INDEX: u16 = 99;

pub const TOPICS_INDEX: u16 = 100;

// ============================================================================
// Message Decoder Registry (消息解码器注册表)
// ============================================================================

type MessageDecoder = fn(&[u8]) -> Result<Box<dyn EnDecoder>>;

lazy_static! {
    static ref DECODER_REGISTRY: HashMap<u16, MessageDecoder> = {
        let mut m = HashMap::new();

        // Broker <-> COO messages
        m.insert(BROKER_COO_HEARTBEAT_REQUEST_INDEX, broker_coo_heartbeat_request_decoder as MessageDecoder);
        m.insert(BROKER_COO_HEARTBEAT_RESPONSE_INDEX, broker_coo_heartbeat_response_decoder as MessageDecoder);

        // Client <-> COO messages
        m.insert(CLIENT_COO_AUTH_REQUEST_INDEX, client_coo_auth_request_decoder as MessageDecoder);
        m.insert(CLIENT_COO_AUTH_RESPONSE_INDEX, client_coo_auth_response_decoder as MessageDecoder);
        m.insert(CLIENT_COO_HEARTBEAT_REQUEST_INDEX, client_coo_heartbeat_request_decoder as MessageDecoder);
        m.insert(CLIENT_COO_HEARTBEAT_RESPONSE_INDEX, coo_client_heartbeat_response_decoder as MessageDecoder);
        m.insert(CLIENT_COO_NEW_TOPIC_REQUEST_INDEX, client_coo_new_topic_request_decoder as MessageDecoder);
        m.insert(CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX, coo_client_new_topic_response_decoder as MessageDecoder);
        m.insert(CLIENT_COO_ADD_PARTITION_REQUEST_INDEX, client_coo_add_partition_request_decoder as MessageDecoder);
        m.insert(CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX, coo_client_add_partition_response_decoder as MessageDecoder);
        m.insert(CLIENT_COO_SUB_REQUEST_INDEX, client_coo_sub_request_decoder as MessageDecoder);
        m.insert(CLIENT_COO_SUB_RESPONSE_INDEX, coo_client_sub_response_decoder as MessageDecoder);

        // COO Raft messages
        m.insert(COO_RAFT_GET_META_REQUEST_INDEX, coo_raft_get_meta_request_decoder as MessageDecoder);
        m.insert(COO_RAFT_GET_META_RESPONSE_INDEX, coo_raft_get_meta_response_decoder as MessageDecoder);

        // Common messages
        m.insert(TOPICS_INDEX, topics_decoder as MessageDecoder);

        m
    };
}

/// Decode a message by its index
///
/// This function looks up the decoder from the registry and decodes the message.
/// # Arguments
/// * `index` - The message type index
/// * `data` - The encoded message data
///
/// # Returns
/// A boxed `EnDecoder` trait object or an error
pub fn decode_message(version: u8, index: u16, data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    DECODER_REGISTRY
        .get(&index)
        .ok_or_else(|| anyhow::anyhow!("Unknown message type index: {}", index))?(data)
}

// ============================================================================
// Decoder Functions (单个解码器函数)
// ============================================================================

fn broker_coo_heartbeat_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <broker_coo::BrokerCooHeartbeatRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn broker_coo_heartbeat_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <broker_coo::BrokerCooHeartbeatResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_auth_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooAuthRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_auth_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooAuthResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_heartbeat_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooHeartbeatRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_client_heartbeat_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::CooClientHeartbeatResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_new_topic_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooNewTopicRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_client_new_topic_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::CooClientNewTopicResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_add_partition_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooAddPartitionRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_client_add_partition_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::CooClientAddPartitionResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn client_coo_sub_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::ClientCooSubRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_client_sub_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <client_coo::CooClientSubResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_raft_get_meta_request_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <coo_raft::CooRaftGetMetaRequest as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn coo_raft_get_meta_response_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <coo_raft::CooRaftGetMetaResponse as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}

fn topics_decoder(data: &[u8]) -> Result<Box<dyn EnDecoder>> {
    let msg = <common::Topics as Decoder>::decode(data)?;
    Ok(Box::new(msg) as Box<dyn EnDecoder>)
}
