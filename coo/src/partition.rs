use crate::{BrokerNode, raftx::PartitionApply};
use anyhow::{Result, anyhow};
use bincode::config;
use dashmap::DashMap;
use grpcx::{
    brokercoosvc::BrokerState,
    commonsvc::TopicPartitionMeta,
    topic_meta::{TopicPartitionDetail, TopicPartitionDetailSnapshot},
};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::{num::ParseIntError, sync::Arc};

const TOPIC_META_PREFIX: &str = "topic_meta/";
const TOPIC_PARTITION_PREFIX: &str = "topic_partition/";
const TOPIC_KEY_PREFIX: &str = "topic_key/";

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("Topic not found")]
    TopicNotFound,
    #[error("No partitions available")]
    NoPartitions,
    #[error("Invalid strategy for key assignment")]
    InvalidStrategy,
}

/// 分区分配策略
#[derive(Debug, Clone)]
pub struct PartitionPolicy {
    pub replication_factor: usize,   // 副本数量
    pub strategy: PartitionStrategy, // 分区策略类型
}

/// 分区策略类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Key 哈希分配（相同 Key 到固定分区）
    HashKey,
    /// 轮询分配（无 Key 时均匀分配）
    RoundRobin,
    /// 基于 Broker 负载的分配
    LoadAware,
}

impl Default for PartitionPolicy {
    fn default() -> Self {
        Self {
            replication_factor: 2,
            strategy: PartitionStrategy::RoundRobin,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicMeta {
    pub version: u64, // 版本号（乐观锁）
    pub num_partition: u64,
    pub next_round_robin: u64,
}

/// 分区管理器（集成负载均衡）
#[derive(Debug, Clone)]
pub struct PartitionManager {
    // Topic -> TopicAssignment
    // Shared with ConsumerGroupManager.all_topics
    pub all_topics: Arc<DashMap<String, Vec<TopicPartitionDetail>>>,
    policy: Arc<PartitionPolicy>,
    // 与 raftx/storage.rs 使用同一个 Db
    db: Db,
}

impl PartitionManager {
    pub fn new<P: AsRef<std::path::Path>>(policy: PartitionPolicy, dbpath: P) -> Self {
        let pm = Self {
            all_topics: Arc::new(DashMap::new()),
            policy: Arc::new(policy),
            db: sled::open(dbpath).expect("Failed to open sled DB"),
        };
        pm.load_all_topics().expect("load all topics error");
        pm
    }

    /// 哈希函数（示例：使用简单的 DJB2 哈希）
    fn hash_key(&self, key: &str) -> u32 {
        let mut hash: u32 = 5381;
        for &byte in key.as_bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u32);
        }
        hash
    }

    /// 检查 Topic 是否存在
    pub fn has_topic(&self, topic: &str) -> bool {
        if !self.all_topics.contains_key(topic) {
            return self.load_topic_data(topic, true).is_ok();
        }
        true
    }

    /// 根据多条件筛选分区
    pub fn query_partitions(
        &self,
        topics: &[String],
        broker_ids: &[u32],
        partition_ids: &[u32],
        keys: &[String],
    ) -> Vec<(String, Vec<TopicPartitionDetail>)> {
        let mut result = Vec::new();

        // 使用统一的迭代器类型
        let assignments_iter = self
            .all_topics
            .iter()
            .filter(|entry| topics.is_empty() || topics.contains(entry.key()));

        // 遍历匹配的 Topic 的分区
        for assignment in assignments_iter {
            let mut tms = vec![];
            for p in assignment.value().iter() {
                // Broker 筛选：检查主副本或从副本是否在 broker_ids 中
                let match_broker = broker_ids.is_empty()
                    || broker_ids.contains(&p.broker_leader_id)
                    || p.broker_follower_ids.iter().any(|f| broker_ids.contains(f));

                // Partition ID 筛选
                let match_partition =
                    partition_ids.is_empty() || partition_ids.contains(&p.partition_id);

                let match_key = keys.is_empty() || p.pub_keys.iter().any(|k| keys.contains(k));

                if match_broker && match_partition && match_key {
                    tms.push(p.clone());
                }
            }
            result.push((assignment.key().clone(), tms));
        }

        result
    }

    /// 获取 Topic 的所有分区（辅助方法）
    fn get_partitions(&self, topic: &str) -> Vec<TopicPartitionDetail> {
        if let Some(v) = self.all_topics.get(topic) {
            return v.value().clone();
        }
        vec![]
    }

    /// 加载所有 Topic 的元数据
    pub fn load_all_topics(&self) -> Result<()> {
        let mut topics = Vec::new();
        for meta_res in self.db.scan_prefix(TOPIC_META_PREFIX) {
            let (meta_key, _) = meta_res?;
            let key_str = String::from_utf8(meta_key.to_vec())?;
            if key_str.starts_with(TOPIC_META_PREFIX) {
                let topic = key_str.trim_start_matches(TOPIC_META_PREFIX).to_string();
                topics.push(topic);
            }
        }

        for topic in topics {
            self.load_topic_data(&topic, true)?;
        }
        Ok(())
    }

    pub fn load_topic_data(&self, topic: &str, insert: bool) -> Result<Vec<TopicPartitionDetail>> {
        // 1. 加载元数据
        let meta_key = format!("{}{}", TOPIC_META_PREFIX, topic);
        let meta_bytes = match self.db.get(&meta_key)? {
            Some(bytes) => bytes,
            None => return Err(anyhow!("Topic metadata not found for {}", topic)),
        };

        let (meta, _): (TopicMeta, _) =
            bincode::serde::decode_from_slice(&meta_bytes, config::standard())
                .map_err(|e| anyhow!(e.to_string()))?;

        // 2. 加载所有分区数据
        let partition_prefix = format!("{}{}/", TOPIC_PARTITION_PREFIX, topic);
        let mut partition_list = Vec::new();

        for res in self.db.scan_prefix(&partition_prefix) {
            let (_, v) = res?;
            let (partition_meta_snapshot, _): (TopicPartitionDetailSnapshot, _) =
                bincode::serde::decode_from_slice(&v, config::standard())
                    .map_err(|e| anyhow!(e.to_string()))?;
            partition_list.push(TopicPartitionDetail::from(partition_meta_snapshot));
        }

        if insert {
            self.all_topics
                .insert(topic.to_string(), partition_list.clone());
        }

        Ok(partition_list)
    }

    pub fn build_allocator(
        &self,
        new_topic_partition_factor: String,
        brokers: Option<Arc<DashMap<u32, BrokerState>>>,
    ) -> Allocator {
        if brokers.is_none() {
            return Allocator {
                mngr: self,
                new_topic_partition_factor,
                brokers: Arc::default(),
                policy: &self.policy,
            };
        }

        Allocator {
            mngr: self,
            new_topic_partition_factor,
            brokers: brokers.unwrap(),
            policy: &self.policy,
        }
    }
}

impl PartitionApply for PartitionManager {
    /// 将 SinglePartition 落盘并写入内存中
    fn apply(&self, part: SinglePartition) -> Result<()> {
        // 开启事务
        let mut batch = sled::Batch::default();

        // 1. 存储元数据
        let meta_key = format!("{}{}", TOPIC_META_PREFIX, &part.topic);
        let meta = TopicMeta {
            version: 1, // 简化版本管理
            num_partition: part.partitions.len() as u64,
            next_round_robin: 1, // 简化轮询管理
        };
        batch.insert(
            meta_key.into_bytes(),
            bincode::serde::encode_to_vec(&meta, config::standard())?,
        );

        // 2. 存储分区数据
        for p in &part.partitions {
            let partition_key = format!("{}{}/{}", TOPIC_PARTITION_PREFIX, &part.topic, p.id);
            batch.insert(
                partition_key.into_bytes(),
                bincode::serde::encode_to_vec(p, config::standard())?,
            );
        }

        // 3. 原子性写入
        self.db.apply_batch(batch)?;

        // 4. 更新内存状态

        // match self.assignments.get(&part.topic){
        //     Some(entry) => {
        //         for pm in &part.partitions{
        //             for v in entry.iter_mut(){
        //                 if v.id==pm.id{
        //                     // 有则更新
        //                 }
        //             }
        //         }
        //     },
        //     None => todo!(),
        // }

        self.all_topics
            .entry(part.topic)
            .and_modify(|pms| {
                for pm in pms {
                    if let Some(v) = part.partitions.iter().find(|f| f.id == pm.partition_id) {
                        // 该 partition_id 已经存在，更新
                        // TODO: 如何应用？
                        // if v.broker_leader_id
                    }
                }
            })
            .or_insert(
                part.partitions
                    .iter()
                    .map(|v| TopicPartitionDetail::from(v.clone()))
                    .collect(),
            );

        Ok(())
    }

    fn get_db(&self) -> Db {
        self.db.clone()
    }
}

pub struct Allocator<'a> {
    mngr: &'a PartitionManager,
    new_topic_partition_factor: String,
    brokers: Arc<DashMap<u32, BrokerState>>,
    policy: &'a PartitionPolicy,
}

impl Allocator<'_> {
    /// 为新 Topic 的 Partition 分配 broker（根据策略选择主副本）
    pub fn assign_new_topic(
        &self,
        topic: &str,
        mut num_partitions: u32,
    ) -> Result<SinglePartition> {
        if self.brokers.is_empty() {
            return Err(anyhow!("not have brokers to assign"));
        }
        if self.mngr.all_topics.contains_key(topic) {
            return Err(anyhow!("the topic has assigned partition"));
        }

        // 1. 解析分区因子
        if num_partitions == 0 {
            let (num, has_n) = extract_value(&self.new_topic_partition_factor)?;
            num_partitions = num as u32;
            if has_n {
                num_partitions *= self.brokers.len() as u32;
            }
        }

        // 2. 选择可用 Broker
        let selected_brokers = self.select_available_brokers();
        // 3. 分配分区
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for id in 1..num_partitions + 1 {
            let (leader_id, followers) = self.select_replicas(&selected_brokers, id as usize);

            // 获取 Broker 地址
            let leader_addr = self
                .brokers
                .get(&leader_id)
                .map(|b| b.addr.clone())
                .unwrap_or_default();

            let follower_addrs = followers
                .iter()
                .filter_map(|id| self.brokers.get(id).map(|b| b.addr.clone()))
                .collect::<Vec<_>>();

            let partition = TopicPartitionDetail {
                topic: topic.to_string(),
                partition_id: id,
                broker_leader_id: leader_id,
                broker_leader_addr: leader_addr,
                broker_follower_ids: Arc::new(followers),
                broker_follower_addrs: Arc::new(follower_addrs),
                pub_keys: Arc::default(),
                sub_member_ids: vec![],
            };

            self.mngr
                .all_topics
                .entry(topic.to_string())
                .and_modify(|pms| {
                    pms.push(partition.clone());
                })
                .or_insert(vec![partition.clone()]);

            partitions.push(partition.snapshot());
        }

        // 4. 返回新分区的副本信息
        Ok(SinglePartition {
            unique_id: chrono::Local::now().timestamp_micros().to_string(),
            topic: topic.to_string(),
            partitions,
        })
    }

    /// 为现有 Topic 增加分区（自动分配主副本）
    pub fn add_partitions(&self, topic: &str, additional: u32) -> Result<SinglePartition> {
        if self.brokers.is_empty() {
            return Err(anyhow!("not brokers to assign"));
        }
        if self.mngr.all_topics.get(topic).is_none() {
            return Err(anyhow!("not has topic"));
        }
        // 计算新分区的起始 ID
        let start_id = self.mngr.all_topics.get(topic).unwrap().len() as u32 + 1;

        // 3. 选择可用 Broker
        let selected_brokers = self.select_available_brokers();

        // 4. 分配新分区
        let mut new_partitions = Vec::with_capacity(additional as usize);
        for id in start_id..start_id + additional {
            let (leader_id, followers) = self.select_replicas(&selected_brokers, id as usize);

            // 获取 Broker 地址
            let leader_addr = self
                .brokers
                .get(&leader_id)
                .map(|b| b.addr.clone())
                .unwrap_or_default();

            let follower_addrs = followers
                .iter()
                .filter_map(|id| self.brokers.get(id).map(|b| b.addr.clone()))
                .collect::<Vec<_>>();

            let partition = TopicPartitionDetail {
                topic: topic.to_string(),
                partition_id: id,
                broker_leader_id: leader_id,
                broker_leader_addr: leader_addr,
                broker_follower_ids: Arc::new(followers),
                broker_follower_addrs: Arc::new(follower_addrs),
                pub_keys: Arc::default(),
                sub_member_ids: vec![],
            };
            self.mngr
                .all_topics
                .entry(topic.to_string())
                .and_modify(|pms| {
                    pms.push(partition.clone());
                })
                .or_insert(vec![partition.clone()]);

            self.mngr
                .all_topics
                .entry(topic.to_string())
                .and_modify(|v| {
                    v.push(partition.clone());
                });
            new_partitions.push(partition.snapshot());
        }

        Ok(SinglePartition {
            unique_id: "".to_string(),
            topic: topic.to_string(),
            partitions: new_partitions,
        })
    }

    /// 根据 Key 和策略分配分区
    pub fn assign_key_partition(&self, topic: &str, key: &str) -> Result<u32, PartitionError> {
        // 获取 Topic 的分配信息
        let partitions = self.mngr.get_partitions(topic);
        if partitions.is_empty() {
            return Err(PartitionError::NoPartitions);
        }

        match self.policy.strategy {
            PartitionStrategy::HashKey | PartitionStrategy::LoadAware => {
                let hash = self.mngr.hash_key(key);
                let partition_id = hash % partitions.len() as u32;
                Ok(partition_id)
            }
            PartitionStrategy::RoundRobin => {
                // 简化实现：随机选择一个分区
                // 实际实现应使用原子计数器
                let partition_id = rand::random::<u32>() % partitions.len() as u32;
                Ok(partition_id)
            }
        }
    }

    /// 选择可用 Broker（根据策略）
    fn select_available_brokers(&self) -> Vec<u32> {
        let mut broker_ids: Vec<u32> = self.brokers.iter().map(|b| *b.key()).collect();

        match self.policy.strategy {
            PartitionStrategy::LoadAware => {
                // 负载感知策略：按负载排序选择
                broker_ids.sort_by_key(|id| {
                    let broker = self.brokers.get(id).unwrap();
                    (broker.cpurate as f64 * 100.0
                        + broker.memrate as f64 * 100.0
                        + broker.diskrate as f64 * 100.0) as u32
                });
            }
            _ => {
                // 其他策略：随机打乱顺序
                use rand::seq::SliceRandom;
                let mut rng = rand::thread_rng();
                broker_ids.shuffle(&mut rng);
            }
        }

        // 取前 N 个 Broker
        broker_ids
            .into_iter()
            .take(self.policy.replication_factor)
            .collect()
    }

    /// 选择主副本和从副本
    fn select_replicas(&self, selected_brokers: &[u32], partition_index: usize) -> (u32, Vec<u32>) {
        if selected_brokers.is_empty() {
            return (0, Vec::new());
        }

        // 主副本选择逻辑
        let leader_index = partition_index % selected_brokers.len();
        let leader_id = selected_brokers[leader_index];

        // 从副本选择剩余 Broker
        let mut followers = selected_brokers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != leader_index)
            .map(|(_, id)| *id)
            .collect::<Vec<_>>();

        // 确保副本数量不超过配置
        if followers.len() > self.policy.replication_factor - 1 {
            followers.truncate(self.policy.replication_factor - 1);
        }

        (leader_id, followers)
    }
}

fn extract_value(s: &str) -> Result<(u64, bool), ParseIntError> {
    let (num_str, has_n) = match s.chars().last() {
        Some('n') | Some('N') => (&s[..s.len() - 1], true),
        _ => (s, false),
    };
    num_str.parse::<u64>().map(|num| (num, has_n))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SinglePartition {
    pub unique_id: String, // 唯一 id, 用于标识该消息
    pub topic: String,
    pub partitions: Vec<TopicPartitionDetailSnapshot>,
}

impl From<SinglePartition> for Vec<TopicPartitionMeta> {
    fn from(val: SinglePartition) -> Self {
        val.partitions
            .iter()
            .map(|v| v.convert_to_topic_partition_meta(&val.topic.clone()))
            .collect()
    }
}
