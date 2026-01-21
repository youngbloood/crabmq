use crate::{Decoder, EnDecoder, Encoder, TOPICS_INDEX};
use anyhow::Result;
use std::{any::Any, collections::HashMap, hash::Hash};

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct Topics {
    pub term: u64,
    pub topics: Vec<TopicInfo>,
}

impl Encoder for Topics {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::encode_to_vec(self, bincode::config::standard())?)
    }
}

impl Decoder for Topics {
    fn decode(data: &[u8]) -> Result<Self> {
        let (obj, _): (Topics, usize) =
            bincode::decode_from_slice(&data[..], bincode::config::standard())?;
        Ok(obj)
    }
}

impl EnDecoder for Topics {
    fn index(&self) -> u8 {
        TOPICS_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct TopicInfo {
    pub term: u64,
    // 主题名称
    pub topic: String,
    // 主题的分区集群信息
    pub partitions_cluster: HashMap<u32, PartitionCluster>,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct PartitionCluster {
    // 集群 ID
    pub cluster_id: u32,
    // Leader 分区 ID
    pub leader_partition_id: u32,
    // Follower 分区 ID 列表
    pub follow_partition_ids: Vec<u32>,
    // 分区信息列表
    pub partitions: HashMap<u32, PartitionInfo>,
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct PartitionInfo {
    // 该分区 ID
    pub id: u32,
    // 该分区所属的集群 ID
    pub cluster_id: u32,
    // 该分区所在的 Broker ID
    pub broker_id: u32,
    // 该分区所在的 Broker 地址
    pub broker_addr: String,
    // 该分区的高水位偏移量
    pub hw: SegmentOffset,
    // 该分区是否为 Leader
    pub is_leader: bool,
    // 该分区的最新偏移量
    pub log_end_offset: SegmentOffset,
    // 该分区是否可读
    pub readble: bool,
}

impl PartitionInfo {
    fn can_write(&self) -> bool {
        self.is_leader
    }

    fn can_read(&self) -> bool {
        self.is_leader || (!self.is_leader && self.readble)
    }
}

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
pub struct SegmentOffset {
    pub segment_id: u32,
    pub offset: u64,
}
