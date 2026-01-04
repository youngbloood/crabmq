use crate::{Decoder, EnDecoder, Encoder};
use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Topics {
    pub term: u64,
    pub topics: Vec<TopicInfo>,
}

impl Encoder for Topics {
    fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }
}

impl Decoder for Topics {
    fn decode(_data: &[u8]) -> Result<Self> {
        todo!()
    }
}

impl EnDecoder for Topics {}

#[derive(Debug, Default)]
pub struct TopicInfo {
    pub term: u64,
    // 主题名称
    pub topic: String,
    // 主题的分区集群信息
    pub partitions_cluster: HashMap<u32, PartitionCluster>,
}

#[derive(Debug, Default)]
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

#[derive(Debug, Default)]
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
}

#[derive(Debug, Default)]
pub struct SegmentOffset {
    pub segment_id: u32,
    pub offset: u64,
}
