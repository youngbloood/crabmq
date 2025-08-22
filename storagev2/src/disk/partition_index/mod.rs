use crate::disk::ROCKSDB_INDEX_DIR;
use crate::{MessageMeta, SegmentOffset, StorageError, StorageResult};
use anyhow::Result;
use dashmap::DashMap;
use murmur3::murmur3_32;
use rocksdb::{DB, DBCompressionType, IteratorMode, Options, WriteBatch};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

// RocksDB 实例类型
#[derive(Debug, Clone)]
pub enum RocksDBInstanceType {
    ReadWrite, // 读写实例（主实例）
    ReadOnly,  // 只读实例（从主实例复制）
}

// RocksDB 实例包装器
#[derive(Debug)]
pub struct RocksDBInstance {
    pub db: Arc<rocksdb::DB>,
    pub instance_type: RocksDBInstanceType,
    pub dir: PathBuf,
}

impl RocksDBInstance {
    pub fn new(db: rocksdb::DB, instance_type: RocksDBInstanceType, dir: PathBuf) -> Self {
        Self {
            db: Arc::new(db),
            instance_type,
            dir,
        }
    }

    pub fn is_readable(&self) -> bool {
        true // 所有实例都支持读
    }

    pub fn is_writable(&self) -> bool {
        matches!(self.instance_type, RocksDBInstanceType::ReadWrite)
    }
}

impl Drop for RocksDBInstance {
    fn drop(&mut self) {
        if Arc::strong_count(&self.db) == 1 {
            let _ = self.db.flush_wal(false);
            let _ = self.db.flush();
        }
    }
}

// RocksDB 实例管理器
#[derive(Debug)]
pub struct RocksDBInstanceManager {
    // 主实例（读写）
    primary_instances: Arc<DashMap<PathBuf, Arc<RocksDBInstance>>>,
    // 只读实例池
    read_only_instances: Arc<DashMap<PathBuf, Vec<Arc<RocksDBInstance>>>>,
    // 实例创建锁
    create_locks: Arc<DashMap<PathBuf, Mutex<()>>>,
}

impl RocksDBInstanceManager {
    pub fn new() -> Self {
        Self {
            primary_instances: Arc::default(),
            read_only_instances: Arc::default(),
            create_locks: Arc::default(),
        }
    }

    /// 获取或创建主实例（读写）
    pub async fn get_or_create_primary_instance(
        &self,
        db_path: &PathBuf,
    ) -> Result<Arc<RocksDBInstance>> {
        if let Some(instance) = self.primary_instances.get(db_path) {
            return Ok(instance.value().clone());
        }

        // 获取创建锁
        let lock = self
            .create_locks
            .entry(db_path.clone())
            .or_insert_with(|| Mutex::new(()));
        let _guard = lock.value().lock().await;

        // 双重检查
        if let Some(instance) = self.primary_instances.get(db_path) {
            return Ok(instance.value().clone());
        }

        // 创建主实例
        let db_path_clone = db_path.clone();
        let instance = tokio::task::spawn_blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_compression_type(DBCompressionType::Lz4);
            // 主实例配置
            opts.set_max_open_files(10000);
            opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
            opts.set_max_write_buffer_number(4);
            opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
            opts.set_max_background_jobs(4);

            let db = DB::open(&opts, &db_path_clone)?;
            Ok::<_, anyhow::Error>(RocksDBInstance::new(
                db,
                RocksDBInstanceType::ReadWrite,
                db_path_clone,
            ))
        })
        .await??;

        let instance = Arc::new(instance);
        self.primary_instances
            .insert(db_path.clone(), instance.clone());
        Ok(instance)
    }

    /// 获取只读实例
    pub async fn get_read_only_instance(&self, db_path: &PathBuf) -> Result<Arc<RocksDBInstance>> {
        // 首先确保主实例存在
        let _primary = self.get_or_create_primary_instance(db_path).await?;

        // 检查是否有可用的只读实例
        if let Some(read_only_list) = self.read_only_instances.get(db_path) {
            let instances = read_only_list.value();
            if !instances.is_empty() {
                // 返回第一个可用的只读实例
                return Ok(instances[0].clone());
            }
        }

        // 创建新的只读实例
        let db_path_clone = db_path.clone();
        let instance = tokio::task::spawn_blocking(move || {
            let mut opts = Options::default();
            opts.create_if_missing(false); // 只读实例不创建数据库
            opts.set_compression_type(DBCompressionType::Lz4);
            // 只读实例配置
            opts.set_max_open_files(5000);
            opts.set_allow_mmap_reads(true);
            opts.set_allow_mmap_writes(false);
            opts.set_max_background_jobs(2);
            opts.enable_statistics();

            let db = DB::open_for_read_only(&opts, &db_path_clone, false)?;
            Ok::<_, anyhow::Error>(RocksDBInstance::new(
                db,
                RocksDBInstanceType::ReadOnly,
                db_path_clone,
            ))
        })
        .await??;

        let instance = Arc::new(instance);

        // 添加到只读实例池
        self.read_only_instances
            .entry(db_path.clone())
            .and_modify(|list| list.push(instance.clone()))
            .or_insert_with(|| vec![instance.clone()]);

        Ok(instance)
    }

    /// 移除实例
    pub fn remove_instance(&self, db_path: &PathBuf) {
        self.primary_instances.remove(db_path);
        self.read_only_instances.remove(db_path);
        self.create_locks.remove(db_path);
    }

    /// 清理指定存储根目录的所有实例
    pub fn cleanup_storage_root(&self, storage_root: &PathBuf) {
        let keys_to_remove: Vec<_> = self
            .primary_instances
            .iter()
            .filter(|entry| entry.key().starts_with(storage_root))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.remove_instance(&key);
        }
    }

    /// 获取实例统计信息
    pub fn get_stats(&self) -> InstanceManagerStats {
        let mut stats = InstanceManagerStats::default();

        stats.primary_instances = self.primary_instances.len();

        for entry in self.read_only_instances.iter() {
            stats.read_only_instances += entry.value().len();
        }

        stats
    }
}

#[derive(Debug, Default)]
pub struct InstanceManagerStats {
    pub primary_instances: usize,
    pub read_only_instances: usize,
}

// 全局实例管理器
static GLOBAL_ROCKSDB_INSTANCE_MANAGER: once_cell::sync::Lazy<Arc<RocksDBInstanceManager>> =
    once_cell::sync::Lazy::new(|| Arc::new(RocksDBInstanceManager::new()));

/// 获取全局 RocksDB 实例管理器
pub fn get_global_rocksdb_instance_manager() -> Arc<RocksDBInstanceManager> {
    GLOBAL_ROCKSDB_INSTANCE_MANAGER.clone()
}

// 读写分离的 PartitionIndexManager
#[derive(Clone, Debug)]
pub struct ReadWritePartitionIndexManager {
    dir: PathBuf,
    size: usize,
    instance_manager: Arc<RocksDBInstanceManager>,
}

impl ReadWritePartitionIndexManager {
    pub fn new(dir: PathBuf, size: usize) -> Self {
        Self {
            dir,
            size,
            instance_manager: get_global_rocksdb_instance_manager(),
        }
    }

    fn get_index(&self, partition_id: u32) -> Result<usize> {
        let hash = murmur3_32(&mut Cursor::new(partition_id.to_string()), 0)?;
        Ok(hash as usize % self.size)
    }

    /// 获取写实例（主实例）
    pub async fn get_write_instance(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<Arc<RocksDBInstance>> {
        let index = self.get_index(partition_id)?;
        let db_path = self
            .dir
            .join(topic)
            .join(ROCKSDB_INDEX_DIR)
            .join(index.to_string());

        self.instance_manager
            .get_or_create_primary_instance(&db_path)
            .await
    }

    /// 获取读实例（只读实例）
    pub async fn get_read_instance(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<Arc<RocksDBInstance>> {
        let index = self.get_index(partition_id)?;
        let db_path = self
            .dir
            .join(topic)
            .join(ROCKSDB_INDEX_DIR)
            .join(index.to_string());

        self.instance_manager.get_read_only_instance(&db_path).await
    }

    /// 批量写入索引
    pub async fn batch_put(
        &self,
        topic: &str,
        partition_id: u32,
        mms: &[MessageMeta],
    ) -> Result<u64> {
        let instance = self.get_write_instance(topic, partition_id).await?;
        let mms = mms.to_vec(); // 克隆数据以避免生命周期问题

        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            let mut bytes_num = 0;
            for mm in &mms {
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
            instance.db.write(batch)?;
            Ok::<_, anyhow::Error>(bytes_num as u64)
        })
        .await?
    }

    /// 按 msg_id 查询
    pub async fn get_by_msg_id(
        &self,
        topic: &str,
        partition_id: u32,
        msg_id: &str,
    ) -> Result<Option<SegmentOffset>> {
        let instance = self.get_read_instance(topic, partition_id).await?;
        let msg_id = msg_id.to_string(); // 克隆数据以避免生命周期问题

        tokio::task::spawn_blocking(move || {
            if let Some(val) = instance.db.get(format!("id_{}:{}", partition_id, msg_id))? {
                let (meta, _): (MessageMeta, usize) =
                    bincode::serde::decode_from_slice(&val, bincode::config::standard())?;
                Ok(Some(SegmentOffset {
                    segment_id: meta.segment_id,
                    offset: meta.offset,
                }))
            } else {
                Ok(None)
            }
        })
        .await?
    }

    /// 按时间范围查询
    pub async fn get_range_by_time(
        &self,
        topic: &str,
        partition_id: u32,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Option<(SegmentOffset, SegmentOffset)>> {
        let instance = self.get_read_instance(topic, partition_id).await?;

        tokio::task::spawn_blocking(move || {
            let mut start: Option<SegmentOffset> = None;
            let mut end: Option<SegmentOffset> = None;
            let prefix = format!("ts_{}:", partition_id);
            let iter = instance.db.iterator(IteratorMode::Start);
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
        })
        .await?
    }

    /// 按 segment_offset 获取 msg_id
    pub async fn get_msg_id_by_segment_offset(
        &self,
        topic: &str,
        partition_id: u32,
        so: &SegmentOffset,
    ) -> StorageResult<String> {
        let instance = self
            .get_read_instance(topic, partition_id)
            .await
            .map_err(|e| StorageError::Unknown(e.to_string()))?;
        let so_clone = so.clone();

        let result = tokio::task::spawn_blocking(move || {
            let key = format!(
                "so_{}:{:08}:{:016}",
                partition_id, so_clone.segment_id, so_clone.offset
            );
            if let Some(val) = instance.db.get(key)? {
                let (meta, _): (MessageMeta, _) =
                    bincode::serde::decode_from_slice(&val, bincode::config::standard())?;
                Ok(meta.msg_id)
            } else {
                Err(anyhow::anyhow!("Offset not found"))
            }
        })
        .await
        .map_err(|e| StorageError::Unknown(e.to_string()))?;

        match result {
            Ok(msg_id) => Ok(msg_id),
            Err(_) => Err(StorageError::OffsetMismatch(format!(
                "segment_id={}, offset={}",
                so.segment_id, so.offset
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_read_write_separation() {
        let temp_dir = TempDir::new().unwrap();
        let storage_root = temp_dir.path().to_path_buf();

        // 创建读写分离的索引管理器
        let index_manager = ReadWritePartitionIndexManager::new(
            storage_root.clone(),
            10, // 10个索引分片
        );

        let topic = "test_topic";
        let partition_id = 1;

        // 创建测试数据
        let test_meta = MessageMeta {
            msg_id: "test_msg_1".to_string(),
            timestamp: 1000,
            segment_id: 1,
            offset: 100,
            msg_len: 50,
        };

        // 使用写实例写入数据
        let write_result = index_manager
            .batch_put(topic, partition_id, &[test_meta.clone()])
            .await;
        assert!(write_result.is_ok(), "写入应该成功");

        // 使用读实例查询数据
        let read_result = index_manager
            .get_by_msg_id(topic, partition_id, "test_msg_1")
            .await;
        assert!(read_result.is_ok(), "读取应该成功");

        if let Ok(Some(offset)) = read_result {
            assert_eq!(offset.segment_id, 1);
            assert_eq!(offset.offset, 100);
        } else {
            panic!("应该找到消息");
        }

        // 测试时间范围查询
        let time_range_result = index_manager
            .get_range_by_time(topic, partition_id, 999, 1001)
            .await;
        assert!(time_range_result.is_ok(), "时间范围查询应该成功");

        // 测试 segment_offset 查询
        let segment_offset = SegmentOffset {
            segment_id: 1,
            offset: 100,
        };
        let msg_id_result = index_manager
            .get_msg_id_by_segment_offset(topic, partition_id, &segment_offset)
            .await;
        assert!(msg_id_result.is_ok(), "segment_offset 查询应该成功");
        assert_eq!(msg_id_result.unwrap(), "test_msg_1");
    }

    #[tokio::test]
    async fn test_rocksdb_instance_manager() {
        let temp_dir = TempDir::new().unwrap();
        let storage_root = temp_dir.path().to_path_buf();

        let manager = RocksDBInstanceManager::new();
        let db_path = storage_root.join("test_db");

        // 创建主实例
        let primary_instance = manager.get_or_create_primary_instance(&db_path).await;
        assert!(primary_instance.is_ok(), "创建主实例应该成功");
        let primary_instance = primary_instance.unwrap();
        assert!(primary_instance.is_readable());
        assert!(primary_instance.is_writable());

        // 创建只读实例
        let read_instance = manager.get_read_only_instance(&db_path).await;
        assert!(read_instance.is_ok(), "创建只读实例应该成功");
        let read_instance = read_instance.unwrap();
        assert!(read_instance.is_readable());
        assert!(!read_instance.is_writable());

        // 测试统计信息
        let stats = manager.get_stats();
        assert_eq!(stats.primary_instances, 1);
        assert_eq!(stats.read_only_instances, 1);
    }

    #[tokio::test]
    async fn test_multiple_read_instances() {
        let temp_dir = TempDir::new().unwrap();
        let storage_root = temp_dir.path().to_path_buf();

        let manager = RocksDBInstanceManager::new();
        let db_path = storage_root.join("test_db");

        // 创建主实例
        let _primary = manager
            .get_or_create_primary_instance(&db_path)
            .await
            .unwrap();

        // 创建多个只读实例
        let read_instance1 = manager.get_read_only_instance(&db_path).await.unwrap();
        let read_instance2 = manager.get_read_only_instance(&db_path).await.unwrap();

        // 验证是不同的实例
        assert_ne!(
            Arc::as_ptr(&read_instance1.db),
            Arc::as_ptr(&read_instance2.db)
        );

        // 测试统计信息
        let stats = manager.get_stats();
        assert_eq!(stats.primary_instances, 1);
        assert_eq!(stats.read_only_instances, 2);
    }

    #[tokio::test]
    async fn test_concurrent_safety() {
        let temp_dir = TempDir::new().unwrap();
        let storage_root = temp_dir.path().to_path_buf();

        let manager = RocksDBInstanceManager::new();
        let db_path = storage_root.join("concurrent_test_db");

        // 创建主实例和只读实例
        let primary_instance = manager
            .get_or_create_primary_instance(&db_path)
            .await
            .unwrap();
        let read_instance = manager.get_read_only_instance(&db_path).await.unwrap();

        // 测试1：写入后立即读取
        println!("测试1：写入后立即读取");

        // 写入数据
        let mut batch = WriteBatch::default();
        batch.put(b"test_key", b"test_value");
        primary_instance.db.write(batch).unwrap();

        // 立即从只读实例读取
        let value = read_instance.db.get(b"test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
        println!("✅ 写入后立即读取成功");

        // 测试2：并发写入和读取
        println!("测试2：并发写入和读取");

        let primary_clone = primary_instance.clone();
        let read_clone = read_instance.clone();

        // 启动写入任务
        let write_handle = tokio::spawn(async move {
            for i in 0..100 {
                let mut batch = WriteBatch::default();
                batch.put(
                    format!("key_{}", i).as_bytes(),
                    format!("value_{}", i).as_bytes(),
                );
                primary_clone.db.write(batch).unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
        });

        // 启动读取任务
        let read_handle = tokio::spawn(async move {
            let mut last_seen = -1;
            for _ in 0..200 {
                for i in 0..100 {
                    if let Some(value) = read_clone.db.get(format!("key_{}", i).as_bytes()).unwrap()
                    {
                        let expected = format!("value_{}", i);
                        assert_eq!(value, expected.as_bytes());
                        last_seen = last_seen.max(i);
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
            }
            last_seen
        });

        // 等待任务完成
        let (_, last_seen) = tokio::join!(write_handle, read_handle);
        println!("✅ 并发测试成功，最后读取到的值: {:?}", last_seen);
        assert!(
            last_seen.is_ok() && last_seen.unwrap() >= 0,
            "应该能读取到写入的数据"
        );
    }
}
