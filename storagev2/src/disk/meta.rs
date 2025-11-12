use anyhow::Result;
use bincode::{Decode, Encode};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, fs::File as AsyncFile};

use crate::disk::fd_cache::{create_simple_async_file, MetaFileHandler};

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
    fd: MetaFileHandler,
    pub keys: Arc<DashMap<String, u32>>, // key -> partition_id
}

impl TopicMeta {
    pub fn with(f: AsyncFile) -> Self {
        Self {
            fd: MetaFileHandler::new(f),
            keys: Arc::default(),
        }
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        // 读取并解析为中间结构
        let data = fs::read_to_string(path).await?;
        let fd = MetaFileHandler::new(create_simple_async_file(path).await?);
        if data.is_empty() {
            return Ok(Self {
                fd,
                keys: Arc::default(),
            });
        }
        let serialized: SerializableTopicMeta = serde_json::from_str(&data)?;
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
        let mut wl = self.fd.lock().await;
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
    fd: MetaFileHandler, // 存放该 ptr 信息的文件
    // ============== 写入文件内容 ===============
    // 优化：使用 segment_id 代替 filename，避免 RwLock
    // filename = dir.join(format!("{:0>20}.record", segment_id))
    base_dir: Arc<PathBuf>,
    current_segment_id: Arc<AtomicU64>,

    // 写入指针文件：当前文件的写位置
    offset: Arc<AtomicU64>,
    // 写入指针文件：当前文件含有的消息数量
    current_count: Arc<AtomicU64>,
    // ============== 写入文件内容结束 ===============

    // 刷盘写的偏移量
    flush_offset: Arc<AtomicU64>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WriterPositionPtrSnapshot {
    pub segment_id: u64,    // 当前segment ID
    pub offset: u64,        // 当前文件的写位置
    pub current_count: u64, // 当前文件消息数量
}

impl WriterPositionPtr {
    pub async fn new(ptr_filename: PathBuf, record_filename: PathBuf) -> Result<Self> {
        let fd = MetaFileHandler::new(create_simple_async_file(&ptr_filename).await?);

        // 从 record_filename 提取 segment_id
        let segment_id = extract_segment_id_from_filename(&record_filename);

        // 从 ptr_filename 推导 base_dir (去掉最后的 .writer.ptr)
        let base_dir = ptr_filename.parent().unwrap().to_path_buf();

        Ok(Self {
            fd,
            base_dir: Arc::new(base_dir),
            current_segment_id: Arc::new(AtomicU64::new(segment_id)),
            offset: Arc::default(),
            current_count: Arc::default(),
            flush_offset: Arc::default(),
        })
    }

    // ============== 完全无锁的热路径方法 ===============

    #[inline(always)]
    pub fn get_filename(&self) -> PathBuf {
        // 完全无锁：直接从 segment_id 构造
        let segment_id = self.current_segment_id.load(Ordering::Acquire);
        self.base_dir.join(gen_record_filename(segment_id))
    }

    #[inline(always)]
    pub fn get_segment_id(&self) -> u64 {
        self.current_segment_id.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn rotate_offset(&self, num: u64) {
        self.offset.fetch_add(num, Ordering::Release);
    }

    #[inline(always)]
    pub fn rotate_current_count(&self, count: u64) {
        self.current_count.fetch_add(count, Ordering::Release);
    }

    #[inline(always)]
    pub fn rotate_flush_offset(&self, num: u64) {
        self.flush_offset.fetch_add(num, Ordering::Release);
    }

    #[inline(always)]
    pub fn get_offset(&self) -> u64 {
        self.offset.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn get_current_count(&self) -> u64 {
        self.current_count.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn get_flush_offset(&self) -> u64 {
        self.flush_offset.load(Ordering::Acquire)
    }

    pub fn reset_with_filename(&self, filename: PathBuf) {
        // 完全无锁：从文件名提取 segment_id
        let segment_id = extract_segment_id_from_filename(&filename);
        self.current_segment_id.store(segment_id, Ordering::Release);
        self.offset.store(0, Ordering::Release);
        self.current_count.store(0, Ordering::Release);
        self.flush_offset.store(0, Ordering::Release);
    }

    pub fn snapshot(&self) -> WriterPositionPtrSnapshot {
        WriterPositionPtrSnapshot {
            segment_id: self.current_segment_id.load(Ordering::Acquire),
            offset: self.offset.load(Ordering::Acquire),
            current_count: self.current_count.load(Ordering::Acquire),
        }
    }

    pub async fn load(path: &PathBuf) -> Result<Self> {
        let data = tokio::fs::read_to_string(path).await?;
        let fd = MetaFileHandler::new(create_simple_async_file(path).await?);
        let base_dir = path.parent().unwrap().to_path_buf();

        if data.is_empty() {
            return Ok(WriterPositionPtr {
                fd,
                base_dir: Arc::new(base_dir),
                current_segment_id: Arc::new(AtomicU64::new(0)),
                offset: Arc::default(),
                current_count: Arc::default(),
                flush_offset: Arc::default(),
            });
        }

        let sp: WriterPositionPtrSnapshot = serde_json::from_str(&data)?;
        Ok(WriterPositionPtr {
            fd,
            base_dir: Arc::new(base_dir),
            current_segment_id: Arc::new(AtomicU64::new(sp.segment_id)),
            offset: Arc::new(AtomicU64::new(sp.offset)),
            current_count: Arc::new(AtomicU64::new(sp.current_count)),
            flush_offset: Arc::new(AtomicU64::new(sp.offset)),
        })
    }

    pub async fn save(&self, fsync: bool) -> Result<()> {
        let json_data = serde_json::to_string_pretty(&self.snapshot())?;
        let mut wl = self.fd.lock().await;
        // 截断文件，确保清除旧内容
        wl.set_len(0).await?;
        // 将指针移到开头
        wl.seek(std::io::SeekFrom::Start(0)).await?;
        wl.write_all(json_data.as_bytes()).await?;
        if fsync {
            let _fd = self.fd.clone();
            tokio::spawn(async move {
                let _ = _fd.lock().await.sync_data().await;
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

/// 从文件名提取 segment_id
/// 例如："/path/to/00000000000000000123.record" -> 123
fn extract_segment_id_from_filename(filename: &std::path::Path) -> u64 {
    filename
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|s| s.strip_suffix(".record"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}
