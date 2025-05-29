use crate::{BrokerNode, raftx::PartitionApply};
use anyhow::{Result, anyhow};
use bincode::config;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sled::Db;
use std::{num::ParseIntError, sync::Arc};

const TOPIC_META_PREFFIX: &str = "topic_meta/";
const TOPIC_PARTITION_PREFFIX: &str = "topic_partition/";
const TOPIC_KEY_PREFFIX: &str = "topic_key/";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// 分区元数据（明确主副本和从副本）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub id: u32,             // 分区 ID
    pub leader: u32,         // 主副本 Broker ID
    pub followers: Vec<u32>, // 从副本 Broker IDs
    pub key: Option<String>, // 关联的 Key（仅 HashKey 策略）
    pub brokers: Vec<String>,
}

/// Topic 的分区分配信息（统一存储分区）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicAssignment {
    pub partitions: DashMap<u32, PartitionInfo>, // 分区 ID -> 元数据
    pub key_to_partition: DashMap<String, Vec<u32>>, // Key -> 分区 ID（仅 HashKey 策略）
    pub version: u64,                            // 版本号（乐观锁）
    pub next_round_robin: u64,
}

#[derive(Serialize, Deserialize)]
pub struct TopicMeta {
    pub version: u64, // 版本号（乐观锁）
    pub num_partition: u64,
    pub next_round_robin: u64,
}

/// 分区管理器（集成负载均衡）
#[derive(Debug, Clone)]
pub struct PartitionManager {
    // Topic -> TopicAssignment
    assignments: Arc<DashMap<String, TopicAssignment>>,
    policy: Arc<PartitionPolicy>,
    // 与 raftx/storage.rs 使用同一个 Db
    db: Db,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SinglePartition {
    pub unique_id: String, // 唯一 id, 用于标识该消息
    pub topic: String,
    pub partitions: TopicAssignment,
}

impl PartitionManager {
    pub fn new<P: AsRef<std::path::Path>>(policy: PartitionPolicy, dbpath: P) -> Self {
        let pm = Self {
            assignments: Arc::new(DashMap::new()),
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
        if !self.assignments.contains_key(topic) {
            return self.load_topic_data(topic, true).is_ok();
        }
        true
    }

    /// 根据多条件筛选分区
    /// - topic: 要查询的 Topic 列表（空列表表示查询所有 Topic）
    /// - broker_ids: 要匹配的 Broker ID 列表（空列表表示不筛选 Broker）
    /// - partition_ids: 要匹配的分区 ID 列表（空列表表示不筛选分区）
    /// - keys: 要匹配的 Key 列表（空列表表示不筛选 Key）
    pub fn query_partitions(
        &self,
        topics: &[String],
        broker_ids: &[u32],
        partition_ids: &[u32],
        keys: &[String],
    ) -> Vec<(String, TopicAssignment)> {
        let mut result = Vec::new();

        // 使用统一的迭代器类型
        let assignments_iter = self
            .assignments
            .iter()
            .filter(|entry| topics.is_empty() || topics.contains(entry.key()));

        // 遍历匹配的 Topic 的分区
        for assignment in assignments_iter {
            let mut ta = TopicAssignment::default();
            assignment.partitions.iter().for_each(|p| {
                // Broker 筛选：检查主副本或从副本是否在 broker_ids 中
                let match_broker = broker_ids.is_empty()
                    || broker_ids.contains(&p.leader)
                    || p.followers.iter().any(|f| broker_ids.contains(f));

                // Partition ID 筛选
                let match_partition = partition_ids.is_empty() || partition_ids.contains(&p.id);

                if match_broker && match_partition {
                    ta.partitions.insert(p.key().clone(), p.value().clone());
                }
            });
            assignment.key_to_partition.iter().for_each(|p| {
                // Key 筛选（仅针对 HashKey 策略）
                let match_key = keys.is_empty() || keys.contains(p.key());

                if match_key {
                    ta.key_to_partition
                        .insert(p.key().clone(), p.value().clone());
                }
            });
            result.push((assignment.key().clone(), ta));
        }

        result
    }

    /// 获取 Topic 的所有分区（辅助方法）
    fn get_partitions(&self, topic: &str) -> Vec<PartitionInfo> {
        self.assignments
            .get(topic)
            .map(|a| a.partitions.iter().map(|p| p.value().clone()).collect())
            .unwrap_or_default()
    }

    /// 加载所有 Topic 的元数据
    pub fn load_all_topics(&self) -> Result<()> {
        let mut topics = Vec::new();
        for meta_res in self.db.scan_prefix(TOPIC_META_PREFFIX) {
            let (meta_key, _) = meta_res?;
            let topic = String::from_utf8(meta_key.to_vec())?
                .trim_start_matches(TOPIC_META_PREFFIX)
                .to_string();
            topics.push(topic);
        }

        for topic in topics {
            self.load_topic_data(&topic, true)?;
        }
        Ok(())
    }

    pub fn load_topic_data(&self, topic: &str, insert: bool) -> Result<TopicAssignment> {
        // 1. 加载元数据
        let meta_key = format!("{}{}", TOPIC_META_PREFFIX, topic);
        let meta_bytes = self.db.get(&meta_key)?.unwrap();
        let res = bincode::serde::decode_from_slice(&meta_bytes, config::standard())
            .map_err(|e| anyhow!(e.to_string()))?;
        let meta: TopicMeta = res.0;

        // 2. 加载所有分区数据
        let partition_prefix = format!("{}{}/", TOPIC_PARTITION_PREFFIX, topic);
        let mut partition_list: Vec<PartitionInfo> = self
            .db
            .scan_prefix(partition_prefix)
            .filter_map(|res| {
                let (_, v) = res.ok()?;
                let res: PartitionInfo = bincode::serde::decode_from_slice(&v, config::standard())
                    .ok()
                    .unwrap()
                    .0;
                Some(res)
            })
            .collect();

        let partitions = DashMap::new();
        for v in partition_list.drain(..) {
            partitions.insert(v.id, v);
        }

        // 3. 加载 Key 映射
        let key_prefix = format!("{}{}/", TOPIC_KEY_PREFFIX, topic);
        let key_mappings = self
            .db
            .scan_prefix(key_prefix)
            .filter_map(|res| {
                let (k, v) = res.ok()?;
                let key = String::from_utf8(k.to_vec())
                    .ok()?
                    .split('/')
                    .next_back()?
                    .to_string();
                let partition_id = bincode::serde::decode_from_slice(&v, config::standard())
                    .ok()
                    .unwrap()
                    .0;
                Some((key, partition_id))
            })
            .collect::<DashMap<_, _>>();

        let topic_assignment = TopicAssignment {
            partitions,
            key_to_partition: key_mappings,
            version: meta.version,
            next_round_robin: meta.next_round_robin,
        };
        if insert {
            self.assignments
                .insert(topic.to_string(), topic_assignment.clone());
        }
        Ok(topic_assignment)
    }

    pub fn build_allocator(
        &self,
        new_topic_partition_factor: String,
        brokers: Option<Arc<DashMap<u32, BrokerNode>>>,
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
        let meta_key = format!("{}{}", TOPIC_META_PREFFIX, &part.topic);
        let meta = TopicMeta {
            version: part.partitions.version,
            num_partition: part.partitions.partitions.len() as u64,
            next_round_robin: part.partitions.next_round_robin,
        };
        batch.insert(
            meta_key.into_bytes(),
            bincode::serde::encode_to_vec(&meta, config::standard()).unwrap(),
        );

        // 2. 存储分区数据
        for p in part.partitions.partitions.iter() {
            let partition_key = format!("{}{}/{}", TOPIC_PARTITION_PREFFIX, &part.topic, p.id);
            batch.insert(
                partition_key.into_bytes(),
                bincode::serde::encode_to_vec(p.value(), config::standard()).unwrap(),
            );
        }

        // 3. 存储 Key 映射
        for entry in part.partitions.key_to_partition.iter() {
            let key = entry.key();
            let partition_id = entry.value();
            let key_map_key = format!("{}{}/{}", TOPIC_KEY_PREFFIX, &part.topic, key);
            batch.insert(
                key_map_key.into_bytes(),
                bincode::serde::encode_to_vec(partition_id, config::standard()).unwrap(),
            );
        }

        // 原子性写入
        self.db.apply_batch(batch)?;
        self.assignments.insert(part.topic, part.partitions);
        Ok(())
    }

    fn get_db(&self) -> Db {
        self.db.clone()
    }
}

pub struct Allocator<'a> {
    mngr: &'a PartitionManager,
    new_topic_partition_factor: String,
    brokers: Arc<DashMap<u32, BrokerNode>>,
    policy: &'a PartitionPolicy,
}

impl Allocator<'_> {
    /// 分配新 Topic 的分区（根据策略选择主副本）
    pub fn assign_new_topic(
        &self,
        topic: &str,
        mut num_partitions: u32,
    ) -> Result<SinglePartition> {
        if self.brokers.is_empty() {
            return Err(anyhow!("Not brokers to assign"));
        }
        // 1. 选择可用 Broker（基于策略）
        let selected_brokers = self.select_available_brokers();

        if num_partitions == 0 {
            let (_num_partitions, only_number) = extract_value(&self.new_topic_partition_factor)?;
            num_partitions = _num_partitions as u32;
            if !only_number {
                num_partitions *= (self.brokers.len() as u32);
            }
        }

        // 2. 分配分区
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for id in 1..num_partitions + 1 {
            let (leader, followers) = self.select_replicas(&selected_brokers, id as usize);
            let mut brokers = vec![leader.1];
            brokers.extend_from_slice(&followers.1);
            let partition = PartitionInfo {
                id,
                leader: leader.0,
                followers: followers.0,
                key: None, // 初始无 Key 关联
                brokers,
            };
            partitions.push(partition);
        }

        // 3. 插入 Topic 分配信息
        let assignment = TopicAssignment {
            partitions: DashMap::from_iter(partitions.into_iter().map(|p| (p.id, p))),
            key_to_partition: DashMap::new(),
            version: chrono::Local::now().timestamp() as u64,
            next_round_robin: 1,
        };

        // 4. 返回新分区的副本信息
        Ok(SinglePartition {
            unique_id: chrono::Local::now().timestamp_micros().to_string(),
            topic: topic.to_string(),
            partitions: assignment,
        })
    }

    /// 为现有 Topic 增加分区（自动分配主副本）
    pub fn add_partitions(&self, topic: &str, additional: u32) -> Result<SinglePartition> {
        if self.brokers.is_empty() {
            return Err(anyhow!("Not brokers to assign"));
        }
        // 1. 检查 Topic 是否存在
        let mut assignment = self
            .mngr
            .assignments
            .get_mut(topic)
            .ok_or(PartitionError::TopicNotFound)?;

        // 2. 计算新分区的起始 ID
        let start_id = assignment.partitions.len() as u32;

        // 3. 选择可用 Broker
        let broker_ids = self.select_available_brokers();

        // 4. 分配新分区
        let mut new_partitions = Vec::with_capacity(additional as usize);
        for id in start_id..start_id + additional {
            let (leader, followers) = self.select_replicas(&broker_ids, id as usize);
            let mut brokers = vec![leader.1];
            brokers.extend_from_slice(&followers.1);
            let partition = PartitionInfo {
                id,
                leader: leader.0,
                followers: followers.0,
                key: None,
                brokers,
            };
            new_partitions.push(partition);
        }

        // 5. 更新版本号
        assignment.version += 1;

        Ok(SinglePartition {
            unique_id: "".to_string(),
            topic: topic.to_string(),
            partitions: TopicAssignment {
                partitions: DashMap::from_iter(new_partitions.into_iter().map(|p| (p.id, p))),
                key_to_partition: DashMap::new(),
                version: 0,
                next_round_robin: 0,
            },
        })
    }

    /// 根据 Key 和策略分配分区
    pub fn assign_key_partition(&self, topic: &str, key: &str) -> Result<Vec<u32>, PartitionError> {
        // 获取 Topic 的分配信息
        let assignment = self
            .mngr
            .assignments
            .get(topic)
            .ok_or(PartitionError::TopicNotFound)?;

        // 检查分区是否存在
        let num_partitions = assignment.partitions.len() as u32;
        if num_partitions == 0 {
            return Err(PartitionError::NoPartitions);
        }

        match self.policy.strategy {
            // HashKey 或 LoadAware 策略：Key 决定固定分区
            PartitionStrategy::HashKey | PartitionStrategy::LoadAware => {
                let hash = self.mngr.hash_key(key);
                let partition_id = hash % num_partitions;

                // 插入或获取 Key 到分区的映射
                let entry = assignment
                    .key_to_partition
                    .entry(key.to_string())
                    .or_insert(vec![partition_id]);
                Ok(entry.clone())
            }

            // RoundRobin 策略：Key 被忽略，轮询分配
            PartitionStrategy::RoundRobin => {
                let next_id = assignment.next_round_robin + 1;
                let partition_id = (next_id % num_partitions as u64) as u32;
                Ok(vec![partition_id])
            }
        }
    }

    /// 选择可用 Broker（根据策略）
    fn select_available_brokers(&self) -> Vec<(u32, String)> {
        match self.policy.strategy {
            PartitionStrategy::LoadAware => {
                // 负载感知策略：按负载排序选择
                let mut candidates: Vec<_> = self
                    .brokers
                    .iter()
                    .map(|entry| {
                        let bn = entry.value();
                        let load_score = bn.state.cpurat as f32 * 0.3
                            + bn.state.memrate as f32 * 0.3
                            + bn.state.diskrate as f32 * 0.4;
                        (*entry.key(), load_score, bn.state.addr.clone()) // 捕获 addr
                    })
                    .collect();

                // 按负载分数升序排序（选择负载低的）
                candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

                // 取前 N 个 Broker，返回 (id, addr)
                candidates
                    .iter()
                    .take(self.policy.replication_factor)
                    .map(|(id, _, addr)| (*id, addr.clone()))
                    .collect()
            }
            _ => {
                // 其他策略：直接取前 N 个 Broker，返回 (id, addr)
                self.brokers
                    .iter()
                    .take(self.policy.replication_factor)
                    .map(|entry| (*entry.key(), entry.value().state.addr.clone()))
                    .collect()
            }
        }
    }

    /// 选择主副本和从副本（动态轮询主副本）
    fn select_replicas(
        &self,
        selected_brokers: &[(u32, String)],
        partition_index: usize,
    ) -> ((u32, String), (Vec<u32>, Vec<String>)) {
        // 主副本选择逻辑
        let leader = match self.policy.strategy {
            PartitionStrategy::RoundRobin => {
                // 轮询策略：按分区索引轮询
                selected_brokers[partition_index % selected_brokers.len()].clone()
            }
            PartitionStrategy::HashKey | PartitionStrategy::LoadAware => {
                // 哈希或负载策略：选择第一个 Broker 作为主副本
                selected_brokers[0].clone()
            }
        };

        // 从副本选择剩余 Broker
        let followers = selected_brokers
            .iter()
            .filter(|id| **id != leader)
            .take(self.policy.replication_factor - 1)
            .cloned()
            .collect();

        (leader, followers)
    }
}

fn extract_value(s: &str) -> Result<(u64, bool), ParseIntError> {
    let (num_str, has_n) = match s.chars().last() {
        Some('n') | Some('N') => (&s[..s.len() - 1], true),
        _ => (s, false),
    };
    num_str.parse::<u64>().map(|num| (num, has_n))
}
