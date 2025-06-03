use anyhow::Result;
use bincode::{Decode, Encode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, fs::File as AsyncFile, sync::RwLock};

use crate::disk::fd_cache::create_writer_fd;

use super::fd_cache::FileWriterHandlerAsync;

pub const WRITER_PTR_FILENAME: &str = ".writer.ptr";
pub const TOPIC_META: &str = "meta.bin";

// 可序列化的中间表示结构
#[derive(Debug, Serialize, Deserialize)]
struct SerializableTopicMeta {
    keys: Vec<(String, u32)>,
}

// Topic 元数据
#[derive(Debug, Clone)]
pub struct TopicMeta {
    fd: FileWriterHandlerAsync,
    pub keys: Arc<DashMap<String, u32>>, // key -> partition_id
}

impl TopicMeta {
    pub fn with(f: AsyncFile) -> Self {
        Self {
            fd: FileWriterHandlerAsync::new(f),
            keys: Arc::default(),
        }
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        // 读取并解析为中间结构
        let data = fs::read_to_string(path).await?;
        let serialized: SerializableTopicMeta = serde_json::from_str(&data)?;
        let fd = FileWriterHandlerAsync::new(create_writer_fd(path)?);

        // 转换为目标结构
        let dashmap = DashMap::new();
        for (k, v) in serialized.keys {
            dashmap.insert(k, v);
        }

        Ok(Self {
            fd,
            keys: Arc::new(dashmap),
        })
    }

    pub async fn save(&self) -> Result<()> {
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
        let mut wl = self.fd.write().await;
        // 截断文件，确保清除旧内容
        wl.set_len(0).await?;
        // 将指针移到开头
        wl.seek(std::io::SeekFrom::Start(0)).await?;
        wl.write_all(data.as_bytes()).await?;
        wl.sync_data().await?;
        Ok(())
    }
}

// // Partition 元数据
// #[derive(Debug, Clone)]
// pub struct PartitionWriterPtr {
//     inner: Arc<WriterPositionPtr>,
// }

// impl Deref for PartitionWriterPtr {
//     type Target = Arc<RwLock<WriterPositionPtr>>;

//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }

// impl PartitionWriterPtr {
//     pub fn new(init_filename: PathBuf) -> Self {
//         PartitionWriterPtr {
//             inner: Arc::new(RwLock::new(WriterPositionPtr::new(init_filename))),
//         }
//     }

//     pub fn get_inner(&self) -> Arc<RwLock<WriterPositionPtr>> {
//         self.inner.clone()
//     }

//     pub async fn load(path: &PathBuf) -> Result<Self> {
//         let ptr = WriterPositionPtr::load(path).await?;
//         Ok(Self {
//             inner: Arc::new(RwLock::new(ptr)),
//         })
//     }

//     pub async fn save(&self, path: &PathBuf) -> Result<()> {
//         self.inner.read().await.save(path).await?;
//         Ok(())
//     }

//     pub async fn save_to(&self, fd: FileWriterHandlerAsync) -> Result<()> {
//         self.inner.read().await.save_to(fd).await?;
//         Ok(())
//     }
// }

#[derive(Debug, Clone)]
pub struct WriterPositionPtr {
    fd: FileWriterHandlerAsync,     // 存放该 ptr 信息的文件
    filename: Arc<RwLock<PathBuf>>, // 当前写的文件
    offset: Arc<AtomicU64>,         // 当前文件的写位置
    current_count: Arc<AtomicU64>,  // 当前文件消息数量
    flush_offset: Arc<AtomicU64>,   // 刷盘写的偏移量

                                    //  预创建的下一个文件的信息
                                    // pub next_filename: PathBuf, // 下一个文件名
                                    // pub next_offset: u64,       // 写一个文件的写偏移量
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WriterPositionPtrSnapshot {
    pub filename: PathBuf,  // 当前写的文件
    pub offset: u64,        // 当前文件的写位置
    pub current_count: u64, // 当前文件消息数量
}

impl WriterPositionPtr {
    pub fn new(ptr_filename: PathBuf, record_filename: PathBuf) -> Result<Self> {
        let fd = FileWriterHandlerAsync::new(create_writer_fd(&ptr_filename)?);
        Ok(Self {
            fd,
            filename: Arc::new(RwLock::new(record_filename)),
            offset: Arc::default(),
            current_count: Arc::default(),
            flush_offset: Arc::default(),
        })
    }

    #[inline]
    pub async fn get_filename(&self) -> PathBuf {
        self.filename.read().await.clone()
    }

    #[inline]
    pub fn rotate_offset(&self, num: u64) {
        self.offset.fetch_add(num, Ordering::Relaxed);
    }

    #[inline]
    pub fn rotate_current_count(&self, count: u64) {
        self.current_count.fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn rotate_flush_offset(&self, num: u64) {
        self.flush_offset.load(Ordering::Relaxed);
    }

    #[inline]
    pub fn get_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get_current_count(&self) -> u64 {
        self.current_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get_flush_offset(&self) -> u64 {
        self.flush_offset.load(Ordering::Relaxed)
    }

    pub async fn reset_with_filename(&self, filename: PathBuf) {
        *self.filename.write().await = filename;
        self.offset.store(0, Ordering::Relaxed);
        self.current_count.store(0, Ordering::Relaxed);
        self.flush_offset.store(0, Ordering::Relaxed);
    }

    pub async fn snapshot(&self) -> WriterPositionPtrSnapshot {
        WriterPositionPtrSnapshot {
            filename: self.filename.read().await.clone(),
            offset: self.offset.load(Ordering::Relaxed),
            current_count: self.current_count.load(Ordering::Relaxed),
        }
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let sp: WriterPositionPtrSnapshot = serde_json::from_str(&data)?;

        let fd = FileWriterHandlerAsync::new(create_writer_fd(path)?);
        Ok(WriterPositionPtr {
            filename: Arc::new(RwLock::new(sp.filename)),
            fd,
            offset: Arc::new(AtomicU64::new(sp.offset)),
            current_count: Arc::new(AtomicU64::new(sp.current_count)),
            flush_offset: Arc::new(AtomicU64::new(sp.offset)),
        })
    }

    // pub async fn save(&self, path: &PathBuf) -> Result<()> {
    //     let json_data = serde_json::to_string_pretty(&self.snapshot().await)?;
    //     tokio::fs::write(path, json_data).await?;

    //     Ok(())
    // }

    pub async fn save_to(&self, should_sync: bool) -> Result<()> {
        let json_data = serde_json::to_string_pretty(&self.snapshot().await)?;
        let mut wl = self.fd.write().await;
        // 截断文件，确保清除旧内容
        wl.set_len(0).await?;
        // 将指针移到开头
        wl.seek(std::io::SeekFrom::Start(0)).await?;
        wl.write_all(json_data.as_bytes()).await?;
        if should_sync {
            let _fd = self.fd.clone();
            tokio::spawn(async move {
                let _ = _fd.write().await.sync_data().await;
            });
        }

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

    pub fn with_dir_and_group_id(dir: PathBuf, group_id: u32) -> Self {
        ReaderPositionPtr {
            group_id,
            filename: dir.join(gen_record_filename(0)),
            offset: 0,
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
