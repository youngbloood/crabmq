use std::any::Any;

use crate::{
    CLIENT_COO_ADD_PARTITION_REQUEST_INDEX, CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX,
    CLIENT_COO_AUTH_REQUEST_INDEX, CLIENT_COO_AUTH_RESPONSE_INDEX,
    CLIENT_COO_HEARTBEAT_REQUEST_INDEX, CLIENT_COO_HEARTBEAT_RESPONSE_INDEX,
    CLIENT_COO_NEW_TOPIC_REQUEST_INDEX, CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX,
    CLIENT_COO_SUB_REQUEST_INDEX, CLIENT_COO_SUB_RESPONSE_INDEX, Decoder, EnDecoder, Encoder,
    common::{SegmentOffset, TopicInfo, Topics},
};
use anyhow::Result;

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooAuthRequest {
    pub client_id: String,
    pub secret: String,
}

impl Encoder for ClientCooAuthRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooAuthRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooAuthRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooAuthRequest {
    fn index(&self) -> u8 {
        CLIENT_COO_AUTH_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooAuthResponse {
    pub code: u16,
    pub message: String,
    pub token: String,
}

impl Encoder for ClientCooAuthResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooAuthResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooAuthResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooAuthResponse {
    fn index(&self) -> u8 {
        CLIENT_COO_AUTH_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooHeartbeatRequest {
    pub client_id: String,
    pub token: String,
}

impl Encoder for ClientCooHeartbeatRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooHeartbeatRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooHeartbeatRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooHeartbeatRequest {
    fn index(&self) -> u8 {
        CLIENT_COO_HEARTBEAT_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Coo 返回给 Client 的心跳响应
#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
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
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooClientHeartbeatResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooClientHeartbeatResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooClientHeartbeatResponse {
    fn index(&self) -> u8 {
        CLIENT_COO_HEARTBEAT_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Coo 返回给 Client 的 Topic 列表
type CooClientTopicList = Topics;

// Client 向 Coo 发起 NewTopic 请求
#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooNewTopicRequest {
    // 请求创建的 topic 名称
    pub topic: String,
    // 创建 topic 时的分区因子，仅有两种类型，数字类型和类似“100n”类型。后者表示当前的broker每个分配100个。
    pub partition_factor: String,
    // 每个分区的副本数
    // <0: 不创建分区副本
    // =0: 按coo配置的来创建分区副本数量
    // >0: 创建指定分区副本数量
    pub partition_replication_count: i8,
    // 副本分区是否可读
    pub partition_replication_readble: bool,
    // 每个broker最多可存在多少个分区，超过这个数量后不再分配分区到这个broker上
    pub broker_partition_replication_limit: u32,
    // 创建的超时时间，单位: s
    pub timeout: u64,
}

impl Encoder for ClientCooNewTopicRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooNewTopicRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooNewTopicRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooNewTopicRequest {
    fn index(&self) -> u8 {
        CLIENT_COO_NEW_TOPIC_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Coo 响应给 Client : NewTopic的结果
#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct CooClientNewTopicResponse {
    pub code: u16,
    pub message: String,
    pub topic: Option<TopicInfo>,
}

impl Encoder for CooClientNewTopicResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooClientNewTopicResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooClientNewTopicResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooClientNewTopicResponse {
    fn index(&self) -> u8 {
        CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
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
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooAddPartitionRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooAddPartitionRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooAddPartitionRequest {
    fn index(&self) -> u8 {
        CLIENT_COO_ADD_PARTITION_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct CooClientAddPartitionResponse {
    pub code: u16,
    pub message: String,
    pub topic: Option<TopicInfo>,
}

impl Encoder for CooClientAddPartitionResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooClientAddPartitionResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooClientAddPartitionResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooClientAddPartitionResponse {
    fn index(&self) -> u8 {
        CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Client 向 Coo: 发起订阅请求
#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooSubRequest {
    pub group_id: u32,
    pub option: Option<ClientCooSubOption>,
}

impl Encoder for ClientCooSubRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for ClientCooSubRequest {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (ClientCooSubRequest, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for ClientCooSubRequest {
    fn index(&self) -> u8 {
        CLIENT_COO_SUB_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooSubOption {
    pub sub_topics: Vec<ClientCooSubTopic>,
    pub auto_commit: bool,
    // 设置滑动窗口大小，用于 broker 中单个 consumer 限制 read_ptr 与 commit_ptr 之间的窗口大小，防止consumer过度占用资源
    pub consumer_slide_window_size: u64,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooSubTopic {
    pub topic: String,
    // 起始消费位移（1表示最新，0表示从头开始消费）
    // 仅每个组中的第一个消费者的 offset 设置有效
    pub head_offset: u8,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct ClientCooCommitRequest {
    pub topic: String,
    pub pos: SegmentOffset,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct CooClientCommitResponse {
    pub topic: String,
    pub pos: SegmentOffset,
    pub code: u16,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode, Clone)]
pub struct CooClientSubResponse {
    pub code: u16,
    pub message: String,
    pub topics: Vec<TopicInfo>,
}

impl Encoder for CooClientSubResponse {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for CooClientSubResponse {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (CooClientSubResponse, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for CooClientSubResponse {
    fn index(&self) -> u8 {
        CLIENT_COO_SUB_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
