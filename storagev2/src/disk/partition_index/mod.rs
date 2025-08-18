use crate::disk::ROCKSDB_INDEX_DIR;
use crate::{MessageMeta, SegmentOffset, StorageError, StorageResult};
use anyhow::Result;
use dashmap::DashMap;
use murmur3::murmur3_32;
use rocksdb::{DB, DBCompressionType, IteratorMode, Options, WriteBatch};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

// 基于 storage 根目录的全局 PartitionIndexManager 注册表
// key: storage_root_dir
static GLOBAL_PARTITION_INDEX_MANAGERS: once_cell::sync::Lazy<
    DashMap<PathBuf, (Arc<PartitionIndexManager>, AtomicUsize)>,
> = once_cell::sync::Lazy::new(|| DashMap::new());

/// 初始化或增加指定 storage 根目录下的 PartitionIndexManager 引用计数
pub fn init_global_partition_index_manager(dir: PathBuf, size: usize) -> Result<()> {
    if let Some(mut entry) = GLOBAL_PARTITION_INDEX_MANAGERS.get_mut(&dir) {
        entry.value().1.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let mgr = Arc::new(PartitionIndexManager::new(dir.clone(), size));
    GLOBAL_PARTITION_INDEX_MANAGERS.insert(dir, (mgr, AtomicUsize::new(1)));
    Ok(())
}

/// 获取指定 storage 根目录的 PartitionIndexManager
pub fn get_global_partition_index_manager_for_root(
    root: &PathBuf,
) -> Option<Arc<PartitionIndexManager>> {
    GLOBAL_PARTITION_INDEX_MANAGERS
        .get(root)
        .map(|v| v.value().0.clone())
}

/// 释放（减少引用计数），引用为 0 时移除并触发 Drop
pub fn release_global_partition_index_manager(root: &PathBuf) {
    if let Some(mut entry) = GLOBAL_PARTITION_INDEX_MANAGERS.get_mut(root) {
        let remain = entry.value().1.fetch_sub(1, Ordering::Release) - 1;
        if remain == 0 {
            // 移除，从而触发 Arc 最终 Drop
            GLOBAL_PARTITION_INDEX_MANAGERS.remove(root);
        }
    }
}

#[derive(Debug)]
struct RefCountedPartitionMetaManager {
    pub pmm: Arc<PartitionMetaManager>,
    pub ref_count: AtomicUsize,
}

impl RefCountedPartitionMetaManager {
    pub fn new(pmm: Arc<PartitionMetaManager>) -> Self {
        Self {
            pmm,
            ref_count: AtomicUsize::new(1),
        }
    }
    pub fn inc(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) -> usize {
        self.ref_count.fetch_sub(1, Ordering::Release) - 1
    }
}

#[derive(Clone, Debug)]
pub struct PartitionIndexManager {
    // storage_dir
    dir: PathBuf,

    size: usize,
    // key: (topic, partition_id)
    partitions: Arc<DashMap<PathBuf, RefCountedPartitionMetaManager>>,
    partitions_lock: Arc<DashMap<PathBuf, Mutex<()>>>,
    // (topic, partition_id) -> PathBuf
    route: Arc<DashMap<(String, u32), PathBuf>>,
}

impl PartitionIndexManager {
    pub fn new(dir: PathBuf, size: usize) -> Self {
        Self {
            dir,
            size,
            partitions: Arc::default(),
            partitions_lock: Arc::default(),
            route: Arc::default(),
        }
    }

    fn get_index(&self, partition_id: u32) -> Result<usize> {
        let hash = murmur3_32(&mut Cursor::new(partition_id.to_string()), 0)?;
        Ok(hash as usize % self.size)
    }

    pub async fn get_or_create(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<Arc<PartitionMetaManager>> {
        let index = self.get_index(partition_id)?;

        let key = self
            .dir
            .join(topic)
            .join(ROCKSDB_INDEX_DIR)
            .join(index.to_string());

        // route 维护
        self.route
            .insert((topic.to_string(), partition_id), key.clone());

        if let Some(rcpm) = self.partitions.get(&key) {
            rcpm.value().inc();
            return Ok(rcpm.value().pmm.clone());
        }

        // 1. 获取锁（短时间）
        let _partition_lock = self
            .partitions_lock
            .entry(key.clone())
            .or_insert_with(|| Mutex::new(()));

        // 2. 再次检查（不持锁）
        if let Some(rcpm) = self.partitions.get(&key) {
            rcpm.value().inc();
            return Ok(rcpm.value().pmm.clone());
        }

        // 3. 初始化（不持锁）
        let key_clone = key.clone();
        let partitions = self.partitions.clone();
        let pm = tokio::task::spawn_blocking(move || {
            let pm = Arc::new(PartitionMetaManager::open(&key_clone)?);
            let rcpm = RefCountedPartitionMetaManager::new(pm.clone());
            partitions.insert(key_clone, rcpm);
            Ok::<_, anyhow::Error>(pm)
        })
        .await??;
        Ok(pm)
    }

    // pub fn get_or_create_from_path(&self, p: &PathBuf) -> Result<Arc<PartitionMetaManager>> {
    //     if let Some(pm) = self.partitions.get(p) {
    //         Ok(pm.clone())
    //     } else {
    //         let pm = Arc::new(PartitionMetaManager::open(p)?);
    //         self.partitions.insert(p.clone(), pm.clone());
    //         Ok(pm)
    //     }
    // }

    pub fn get(&self, topic: &str, partition_id: u32) -> Option<Arc<PartitionMetaManager>> {
        let index = self.get_index(partition_id);
        if index.is_err() {
            return None;
        }
        let index = index.unwrap();

        let key = self
            .dir
            .join(topic)
            .join(ROCKSDB_INDEX_DIR)
            .join(index.to_string());

        self.partitions.get(&key).map(|v| v.pmm.clone())
    }

    pub fn remove(&self, topic: &str, partition_id: u32) -> Option<Arc<PartitionMetaManager>> {
        let index = self.get_index(partition_id).ok()?;
        let key = self
            .dir
            .join(topic)
            .join(ROCKSDB_INDEX_DIR)
            .join(index.to_string());

        // 1. 移除 route
        self.route.remove(&(topic.to_string(), partition_id));

        // 2. 计数减一
        if let Some(rcpm) = self.partitions.get(&key) {
            if rcpm.value().dec() == 0 {
                // 没有其他引用，移除 partitions
                if let Some((_, rcpm)) = self.partitions.remove(&key) {
                    return Some(rcpm.pmm);
                }
            }
        }
        None
    }
}

// 不同的分区可以存在相同的 PartitionMetaManager 下，在 key 中设置 partition_id 来区分
// 同一个 PartitionMetaManager 肯定属于同一个 topic
#[derive(Clone, Debug)]
pub struct PartitionMetaManager {
    pub dir: PathBuf, // ${topic}/${ROCKSDB_INDEX_DIR}
    pub db: Arc<rocksdb::DB>,
}

impl Drop for PartitionMetaManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.db) == 1 {
            let _ = self.db.flush_wal(false);
            let _ = self.db.flush();
        }
    }
}

impl PartitionMetaManager {
    pub fn open(dir: &PathBuf) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(DBCompressionType::Lz4);
        let db = DB::open(&opts, dir)?;
        Ok(Self {
            dir: dir.clone(),
            db: Arc::new(db),
        })
    }

    /// 批量写入
    pub fn batch_put(&self, partition_id: u32, mms: &[MessageMeta]) -> anyhow::Result<u64> {
        let mut batch = WriteBatch::default();
        let mut bytes_num = 0;
        for mm in mms {
            // 1. 按 msg_id 建索引
            let encode_vec = bincode::serde::encode_to_vec(mm, bincode::config::standard())?;
            bytes_num += 3 * encode_vec.len();
            batch.put(format!("id_{}:{}", partition_id, mm.msg_id), &encode_vec);
            // 2. 按时间建索引
            batch.put(
                format!(
                    "ts_{}:{:020}:{}:{}",
                    partition_id, mm.timestamp, mm.segment_id, mm.offset
                ),
                &encode_vec,
            );
            // 3. 按 segment_offset 建索引（可选）
            batch.put(
                format!("so_{}:{:08}:{:016}", partition_id, mm.segment_id, mm.offset),
                &encode_vec,
            );
        }
        self.db.write(batch)?;
        Ok(bytes_num as u64)
    }

    /// 按 msg_id 查询
    pub fn get_by_msg_id(
        &self,
        partition_id: u32,
        msg_id: &str,
    ) -> anyhow::Result<Option<SegmentOffset>> {
        if let Some(val) = self.db.get(format!("id_{}:{}", partition_id, msg_id))? {
            let (meta, _): (MessageMeta, usize) =
                bincode::serde::decode_from_slice(&val, bincode::config::standard())?;
            Ok(Some(SegmentOffset {
                segment_id: meta.segment_id,
                offset: meta.offset,
            }))
        } else {
            Ok(None)
        }
    }

    /// 按时间范围查询，返回起止 SegmentOffset
    pub fn get_range_by_time(
        &self,
        partition_id: u32,
        start_ts: u64,
        end_ts: u64,
    ) -> anyhow::Result<Option<(SegmentOffset, SegmentOffset)>> {
        let mut start: Option<SegmentOffset> = None;
        let mut end: Option<SegmentOffset> = None;
        let prefix = format!("ts_{}:", partition_id);
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let (k, v) = item?;
            let kstr = std::str::from_utf8(&k)?;
            if kstr.starts_with(&prefix) {
                let ts: u64 = kstr[3..23].parse().unwrap_or(0);
                if ts >= start_ts && ts <= end_ts {
                    let (meta, _): (MessageMeta, usize) =
                        bincode::serde::decode_from_slice(&v, bincode::config::standard())?;
                    let so = SegmentOffset {
                        segment_id: meta.segment_id,
                        offset: meta.offset,
                    };
                    if start.is_none() {
                        start = Some(so.clone());
                    }
                    end = Some(so);
                }
            }
        }
        if let (Some(s), Some(e)) = (start, end) {
            Ok(Some((s, e)))
        } else {
            Ok(None)
        }
    }

    pub fn get_msg_id_by_segment_offset(
        &self,
        partition_id: u32,
        so: &SegmentOffset,
    ) -> StorageResult<String> {
        let key = format!("so_{}:{:08}:{:016}", partition_id, so.segment_id, so.offset);
        if let Some(val) = self
            .db
            .get(key)
            .map_err(|e| StorageError::Unknown(e.to_string()))?
        {
            let (meta, _): (MessageMeta, _) =
                bincode::serde::decode_from_slice(&val, bincode::config::standard())
                    .map_err(|e| StorageError::Unknown(e.to_string()))?;
            Ok(meta.msg_id)
        } else {
            Err(StorageError::OffsetMismatch(format!(
                "segment_id={}, offset={}",
                so.segment_id, so.offset
            )))
        }
    }
}
