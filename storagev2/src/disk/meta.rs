use anyhow::Result;
use bincode::{Decode, Encode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::{fs, sync::RwLock};

// 可序列化的中间表示结构
#[derive(Debug, Serialize, Deserialize)]
struct SerializableTopicMeta {
    keys: Vec<(String, u32)>,
}

// Topic 元数据
#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub keys: Arc<DashMap<String, u32>>, // key -> partition_id
}

impl TopicMeta {
    pub async fn load(path: &PathBuf) -> Result<Self> {
        // 读取并解析为中间结构
        let data = fs::read_to_string(path).await?;
        let serialized: SerializableTopicMeta = serde_json::from_str(&data)?;

        // 转换为目标结构
        let dashmap = DashMap::new();
        for (k, v) in serialized.keys {
            dashmap.insert(k, v);
        }

        Ok(Self {
            keys: Arc::new(dashmap),
        })
    }

    pub async fn save(&self, path: &PathBuf) -> Result<()> {
        // 转换为可序列化的中间结构
        let serialized = SerializableTopicMeta {
            keys: self
                .keys
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect(),
        };

        // 序列化并保存
        let data = serde_json::to_string_pretty(&serialized)?;
        fs::write(path, data).await?;
        Ok(())
    }
}

// Partition 元数据
#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub commit_ptr: Arc<RwLock<PositionPtr>>,
    pub read_ptr: Arc<RwLock<PositionPtr>>,
    pub write_ptr: Arc<RwLock<WriterPositionPtr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct PositionPtr {
    pub filename: PathBuf,
    pub offset: u64,
}

impl PositionPtr {
    pub fn new(filename: PathBuf) -> Self {
        Self {
            filename,
            offset: Default::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WriterPositionPtr {
    pub filename: PathBuf,
    pub offset: u64,        // 当前文件的写位置
    pub current_count: u64, // 当前文件消息数量
    #[serde(skip)]
    pub flush_offset: u64, // 刷盘写的偏移量
}

impl WriterPositionPtr {
    pub fn new(filename: PathBuf) -> Self {
        Self {
            filename,
            ..Default::default()
        }
    }
}

// 用于序列化的中间结构体
#[derive(Serialize, Deserialize)]
struct SerializedPartitionMeta {
    commit_ptr: PositionPtr,
    read_ptr: PositionPtr,
    write_ptr: WriterPositionPtr,
}

impl PartitionMeta {
    pub async fn load(path: &PathBuf) -> Result<Self> {
        // 读取文件内容
        let data = tokio::fs::read_to_string(path).await?;

        // 反序列化中间结构体
        let mut serialized: SerializedPartitionMeta = serde_json::from_str(&data)?;
        serialized.write_ptr.flush_offset = serialized.write_ptr.offset;
        // 用 commit_ptr 初始化 read_ptr，用于重启后后继续从 commit_ptr 位置读给消费者
        serialized.read_ptr = serialized.commit_ptr.clone();

        // 转换为目标结构体
        Ok(Self {
            commit_ptr: Arc::new(RwLock::new(serialized.commit_ptr)),
            read_ptr: Arc::new(RwLock::new(serialized.read_ptr)),
            write_ptr: Arc::new(RwLock::new(serialized.write_ptr)),
        })
    }

    pub async fn save(&self, path: &PathBuf) -> Result<()> {
        // 并行获取所有指针的读锁
        let commit_ptr = self.commit_ptr.read().await;
        let read_ptr = self.read_ptr.read().await;
        let write_ptr = self.write_ptr.read().await;

        // 创建可序列化结构体
        let serialized = SerializedPartitionMeta {
            commit_ptr: (*commit_ptr).clone(),
            read_ptr: (*read_ptr).clone(),
            write_ptr: (*write_ptr).clone(),
        };

        // 序列化为JSON并保存
        let json_data = serde_json::to_string_pretty(&serialized)?;
        tokio::fs::write(path, json_data).await?;

        Ok(())
    }
}
