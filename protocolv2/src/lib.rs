use anyhow::Result;
use std::{any, collections::HashMap, hash::Hash, sync::OnceLock};
pub mod broker_coo;
pub mod client_broker;
pub mod client_coo;
pub mod common;
pub mod coo_raft;

pub trait Encoder {
    fn encode(&self) -> Result<Vec<u8>>;
}

pub trait Decoder {
    fn decode(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

pub trait EnDecoder: Encoder + Decoder + Send + Sync {}

// 命名规则：
// 1. coo <-> coo 的交互消息为 1xxx
// 2. broker <-> coo 的交互消息为 2xxx。%2==1，表示 broker->coo 的消息； %2==0，表示 coo->broker 的消息
// 3. broker <-> broker 的交互消息为 3xxxx
// 4. client <-> broker 的交互消息为 4xxxx。%2==1，表示 client->broker 的消息； %2==0，表示 broker->client 的消息
// 5. cli <-> coo的交互消息为 5xxx
pub const BrokerCooHeartbeatRequestIndex: u16 = 1001;
pub const BrokerCooHeartbeatResponseIndex: u16 = 1002;

// 命名规则：%2==1，表示到 其他->broker 的消息； %2==0，表示 broker->其他 的消息

static MESSAGE_INDEX: std::sync::OnceLock<HashMap<u16, Box<dyn EnDecoder + Send + Sync>>> =
    OnceLock::new();

fn message_index() -> &'static HashMap<u16, Box<dyn EnDecoder + Send + Sync>> {
    MESSAGE_INDEX.get_or_init(|| {
        let mut m = HashMap::new();
        m.insert(
            BrokerCooHeartbeatRequestIndex,
            Box::new(broker_coo::BrokerCooHeartbeatRequest::default())
                as Box<dyn EnDecoder + Send + Sync>,
        );
        m.insert(
            BrokerCooHeartbeatResponseIndex,
            Box::new(broker_coo::BrokerCooHeartbeatResponse::default())
                as Box<dyn EnDecoder + Send + Sync>,
        );
        m
    })
}
