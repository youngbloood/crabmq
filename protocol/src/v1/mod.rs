//! Protocol version 1: message type indexes and decoders for proto/v1.

use std::collections::HashMap;

use crate::{Decoder, EnDecoder, MessageDecoder, VERSION1};
use anyhow::Result;

pub mod broker_coo;
pub mod client_broker;
pub mod client_coo;
pub mod common;
pub mod coo_raft;

// ============================================================================
// 消息类型索引 (Message Type Indexes) - v1
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
// Decoder functions (v1)
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

/// Returns (version, index) -> decoder map for v1. Used by the crate-level registry.
pub fn decoders() -> HashMap<(u8, u16), MessageDecoder> {
    let mut m = HashMap::new();
    let v = VERSION1;

    m.insert(
        (v, BROKER_COO_HEARTBEAT_REQUEST_INDEX),
        broker_coo_heartbeat_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, BROKER_COO_HEARTBEAT_RESPONSE_INDEX),
        broker_coo_heartbeat_response_decoder as MessageDecoder,
    );

    m.insert(
        (v, CLIENT_COO_AUTH_REQUEST_INDEX),
        client_coo_auth_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_AUTH_RESPONSE_INDEX),
        client_coo_auth_response_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_HEARTBEAT_REQUEST_INDEX),
        client_coo_heartbeat_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_HEARTBEAT_RESPONSE_INDEX),
        coo_client_heartbeat_response_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_NEW_TOPIC_REQUEST_INDEX),
        client_coo_new_topic_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX),
        coo_client_new_topic_response_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_ADD_PARTITION_REQUEST_INDEX),
        client_coo_add_partition_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX),
        coo_client_add_partition_response_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_SUB_REQUEST_INDEX),
        client_coo_sub_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, CLIENT_COO_SUB_RESPONSE_INDEX),
        coo_client_sub_response_decoder as MessageDecoder,
    );

    m.insert(
        (v, COO_RAFT_GET_META_REQUEST_INDEX),
        coo_raft_get_meta_request_decoder as MessageDecoder,
    );
    m.insert(
        (v, COO_RAFT_GET_META_RESPONSE_INDEX),
        coo_raft_get_meta_response_decoder as MessageDecoder,
    );

    m.insert((v, TOPICS_INDEX), topics_decoder as MessageDecoder);

    m
}
