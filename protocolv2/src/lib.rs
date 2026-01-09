use anyhow::Result;
pub mod broker_coo;
pub mod client_broker;
pub mod client_coo;
pub mod common;
pub mod coo_raft;

pub use broker_coo::*;
pub use client_broker::*;
pub use client_coo::*;
pub use common::*;
pub use coo_raft::*;

pub trait Encoder {
    fn encode(&self) -> Result<Vec<u8>>;
}

pub trait Decoder {
    fn decode(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

pub trait EnDecoder: Encoder + Decoder + Send + Sync {
    fn index(&self) -> u8;
}

// 命名规则：
// 1. coo <-> coo 的交互消息为 [1,2]x
// 2. broker <-> coo 的交互消息为 [3,4]x。%2==1，表示 broker->coo 的消息； %2==0，表示 coo->broker 的消息
// 3. broker <-> broker 的交互消息为 [5,6]x
// 4. client <-> broker 的交互消息为 [7,8]x。%2==1，表示 client->broker 的消息； %2==0，表示 broker->client 的消息
// 5. client <-> coo的交互消息为 [9,10]x

pub const COO_RAFT_GET_META_REQUEST_INDEX: u8 = 1;
pub const COO_RAFT_GET_META_RESPONSE_INDEX: u8 = 2;
pub const COO_RAFT_CONF_CHANGE_REQUEST_INDEX: u8 = 3;
pub const COO_RAFT_ORIGIN_MESSAGE_INDEX: u8 = 4;
pub const COO_RAFT_PROPOSE_MESSAGE_INDEX: u8 = 5;

pub const BROKER_COO_HEARTBEAT_REQUEST_INDEX: u8 = 30;
pub const BROKER_COO_HEARTBEAT_RESPONSE_INDEX: u8 = 31;

pub const CLIENT_COO_AUTH_REQUEST_INDEX: u8 = 90;
pub const CLIENT_COO_AUTH_RESPONSE_INDEX: u8 = 91;
pub const CLIENT_COO_HEARTBEAT_REQUEST_INDEX: u8 = 92;
pub const CLIENT_COO_HEARTBEAT_RESPONSE_INDEX: u8 = 93;
pub const CLIENT_COO_NEW_TOPIC_REQUEST_INDEX: u8 = 94;
pub const CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX: u8 = 95;
pub const CLIENT_COO_ADD_PARTITION_REQUEST_INDEX: u8 = 96;
pub const CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX: u8 = 97;
pub const CLIENT_COO_SUB_REQUEST_INDEX: u8 = 98;
pub const CLIENT_COO_SUB_RESPONSE_INDEX: u8 = 99;

pub const TOPICS_INDEX: u8 = 100;

// 命名规则：%2==1，表示到 其他->broker 的消息； %2==0，表示 broker->其他 的消息

// static MESSAGE_INDEX: std::sync::OnceLock<HashMap<u16, Box<dyn EnDecoder + Send + Sync>>> =
//     OnceLock::new();

// fn message_index() -> &'static HashMap<u16, Box<dyn EnDecoder + Send + Sync>> {
//     MESSAGE_INDEX.get_or_init(|| {
//         let mut m = HashMap::new();
//         m.insert(
//             BrokerCooHeartbeatRequestIndex,
//             Box::new(broker_coo::BrokerCooHeartbeatRequest::default())
//                 as Box<dyn EnDecoder + Send + Sync + 'static>,
//         );
//         m.insert(
//             BrokerCooHeartbeatResponseIndex,
//             Box::new(broker_coo::BrokerCooHeartbeatResponse::default())
//                 as Box<dyn EnDecoder + Send + Sync + 'static>,
//         );
//         m
//     })
// }
