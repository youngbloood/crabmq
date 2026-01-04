use crate::{
    Decoder, EnDecoder, Encoder,
    common::{TopicInfo, Topics},
};
use anyhow::Result;
use rkyv::{Deserialize, Serialize, deserialize, rancor::Error};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct ClientCooAuthRequest {
    pub client_id: String,
    pub secret: String,
}

impl Encoder for ClientCooAuthRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(rkyv::to_bytes::<Error>(self)?.into_vec())
    }
}

impl Decoder for ClientCooAuthRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let archived = rkyv::access::<ClientCooAuthRequest, Error>(data)?;
        let c = deserialize::<ClientCooAuthRequest, Error>(archived)?;
        Ok(c)
    }
}

impl EnDecoder for ClientCooAuthRequest {}

pub struct ClientCooAuthResponse {
    pub code: u16,
    pub message: String,
    pub token: String,
}

impl Encoder for ClientCooAuthResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for ClientCooAuthResponse {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for ClientCooAuthResponse {}

pub struct ClientCooHeartbeatRequest {
    pub client_id: String,
    pub token: String,
}

impl Encoder for ClientCooHeartbeatRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for ClientCooHeartbeatRequest {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for ClientCooHeartbeatRequest {}

// Coo 返回给 Client 的心跳响应
pub struct CooClientHeartbeatResponse {
    pub client_id: String,
    pub seccess: bool,
    // 额外消息索引
    pub ext_msg_index: u32,
    // 额外消息内容
    pub ext_msg: Vec<u8>,
}

impl Encoder for CooClientHeartbeatResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for CooClientHeartbeatResponse {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for CooClientHeartbeatResponse {}

// Coo 返回给 Client 的 Topic 列表
type CooClientTopicList = Topics;

// Client 向 Coo 发起 NewTopic 请求
pub struct ClientCooNewTopicRequest {
    // 请求创建的 topic 名称
    pub topic: String,
    // 创建 topic 时的分区因子，仅有两种类型，数字类型和类似“100n”类型。后者表示当前的broker每个分配100个。
    pub partition_factor: String,
    // 每个分区的副本数
    pub partition_replication_count: u32,
    // 创建的超时时间，单位: s
    pub timeout: u64,
}

impl Encoder for ClientCooNewTopicRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for ClientCooNewTopicRequest {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for ClientCooNewTopicRequest {}

// Coo 响应给 Client : NewTopic的结果
pub struct CooClientNewTopicResponse {
    pub code: u16,
    pub message: String,
    pub topic: Option<TopicInfo>,
}

impl Encoder for CooClientNewTopicResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for CooClientNewTopicResponse {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for CooClientNewTopicResponse {}

pub struct ClientCooAddPartitionRequest {
    // 需要添加分区的 topic 名称
    pub topic: String,
    // 需要添加的分区数量
    pub partition_count: u32,
    // 负载均衡的 key
    pub key: String,
    // 添加分区的超时时间，单位: s
    pub timeout: u64,
}

impl Encoder for ClientCooAddPartitionRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for ClientCooAddPartitionRequest {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for ClientCooAddPartitionRequest {}

pub struct CooClientAddPartitionResponse {
    pub code: u16,
    pub message: String,
    pub topic: Option<TopicInfo>,
}

impl Encoder for CooClientAddPartitionResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for CooClientAddPartitionResponse {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for CooClientAddPartitionResponse {}

// Client 向 Coo: 发起订阅请求
pub struct ClientCooSubRequest {
    pub group_id: u32,
    pub option: Option<ClientCooSubOption>,
}

impl Encoder for ClientCooSubRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for ClientCooSubRequest {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for ClientCooSubRequest {}

pub struct ClientCooSubOption {
    pub sub_topics: Vec<ClientCooSubTopic>,
    pub auto_commit: bool,
    // 设置滑动窗口大小，用于 broker 中单个 consumer 限制 read_ptr 与 commit_ptr 之间的窗口大小，防止consumer过度占用资源
    pub consumer_slide_window_size: u64,
}

pub struct ClientCooSubTopic {
    pub topic: String,
    // 起始消费位移（1表示最新，0表示从头开始消费）
    // 仅每个组中的第一个消费者的 offset 设置有效
    pub head_offset: u8,
}
