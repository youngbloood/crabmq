use anyhow::Result;
use bincode::{Decode, Encode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, path::PathBuf, sync::Arc};
use tokio::{fs, sync::RwLock};

pub const META_NAME: &str = "meta.bin";

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
pub struct PartitionWriterMeta {
    inner: Arc<RwLock<WriterPositionPtr>>,
}

impl Deref for PartitionWriterMeta {
    type Target = Arc<RwLock<WriterPositionPtr>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl PartitionWriterMeta {
    pub fn new(init_filename: PathBuf) -> Self {
        PartitionWriterMeta {
            inner: Arc::new(RwLock::new(WriterPositionPtr::new(init_filename))),
        }
    }

    pub fn get_inner(&self) -> Arc<RwLock<WriterPositionPtr>> {
        self.inner.clone()
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        let ptr = WriterPositionPtr::load(path).await?;
        Ok(Self {
            inner: Arc::new(RwLock::new(ptr)),
        })
    }

    pub async fn save(&self, path: &PathBuf) -> Result<()> {
        self.inner.read().await.save(path).await?;
        Ok(())
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

    pub async fn load(path: &PathBuf) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let mut ptr: WriterPositionPtr = serde_json::from_str(&data)?;
        ptr.flush_offset = ptr.offset;
        Ok(ptr)
    }

    pub async fn save(&self, path: &PathBuf) -> Result<()> {
        let json_data = serde_json::to_string_pretty(&self)?;
        tokio::fs::write(path, json_data).await?;

        Ok(())
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ReaderPositionPtr {
    pub group_id: u32,
    pub filename: PathBuf,
    pub offset: u64,
}

impl ReaderPositionPtr {
    pub fn new(group_id: u32, filename: PathBuf) -> Self {
        Self {
            group_id,
            filename,
            offset: Default::default(),
        }
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let ptr: ReaderPositionPtr = serde_json::from_str(&data)?;
        Ok(ptr)
    }

    pub async fn save(&self, path: &PathBuf) -> Result<()> {
        // 序列化为JSON并保存
        let json_data = serde_json::to_string_pretty(&self)?;
        tokio::fs::write(path, json_data).await?;

        Ok(())
    }
}

pub fn gen_filename(factor: u64) -> String {
    format!("{:0>20}", factor)
}

pub fn gen_record_filename(factor: u64) -> String {
    format!("{}.record", gen_filename(factor))
}
