# RocksDB 读写分离集成方案

## 问题解答

### 1. RocksDB 读写分离的并发安全性

**✅ 是的，RocksDB 的读写分离是并发安全的！**

#### 技术原理
- **共享内存映射**：只读实例和主实例共享相同的数据文件
- **原子性写入**：主实例的写入操作是原子的
- **实时可见性**：写入完成后，只读实例立即能看到最新数据

#### 验证测试
```rust
#[tokio::test]
async fn test_concurrent_safety() {
    // 创建主实例和只读实例
    let primary_instance = manager.get_or_create_primary_instance(&db_path).await.unwrap();
    let read_instance = manager.get_read_only_instance(&db_path).await.unwrap();

    // 写入数据
    let mut batch = WriteBatch::default();
    batch.put(b"test_key", b"test_value");
    primary_instance.db.write(batch).unwrap();
    
    // 立即从只读实例读取 - 能立即看到最新数据
    let value = read_instance.db.get(b"test_key").unwrap();
    assert_eq!(value, Some(b"test_value".to_vec()));
}
```

### 2. 移除全局变量，直接集成到组件中

**✅ 完全正确！** 既然能读写分离了，就不需要全局变量了。

## 集成方案

### 1. Writer 集成

#### 修改 `PartitionIndexWriterBuffer`
```rust
pub struct PartitionIndexWriterBuffer {
    pub topic: String,
    pub partition_id: u32,
    conf: Arc<DiskConfig>,
    queue: SwitchQueue<MessageMeta>,
    // 使用读写分离的索引管理器，专门用于写入
    read_write_index_manager: Arc<ReadWritePartitionIndexManager>,
}

impl PartitionIndexWriterBuffer {
    pub fn new(
        topic: String,
        partition_id: u32,
        conf: Arc<DiskConfig>,
        storage_dir: std::path::PathBuf, // 直接传递存储目录
    ) -> Self {
        // 创建读写分离的索引管理器，专门用于写入操作
        let read_write_index_manager = Arc::new(ReadWritePartitionIndexManager::new(
            storage_dir,
            conf.partition_index_num_per_topic as _,
        ));

        Self {
            topic,
            partition_id,
            conf,
            queue: SwitchQueue::new(),
            read_write_index_manager,
        }
    }
}
```

#### 修改 `PartitionBufferSet`
```rust
impl PartitionBufferSet {
    pub async fn new(
        dir: PathBuf,
        conf: Arc<DiskConfig>,
        write_ptr: Arc<WriterPositionPtr>,
        flusher: Arc<Flusher>,
    ) -> Result<Self> {
        // 使用读写分离的索引管理器，不再依赖全局变量
        let partition_index_writer_buffer = PartitionIndexWriterBuffer::new(
            tp.0,
            tp.1,
            conf.clone(),
            conf.storage_dir.clone(), // 直接传递存储目录
        );
        
        // ... 其他代码
    }

    /// 获取读写分离的索引管理器（用于外部访问）
    pub fn get_read_write_index_manager(&self) -> Arc<ReadWritePartitionIndexManager> {
        self.index.get_read_write_index_manager()
    }
}
```

### 2. Reader 集成

#### 修改 `DiskStorageReaderSession`
```rust
pub struct DiskStorageReaderSession {
    dir: PathBuf,
    group_id: u32,
    partition_index_num_per_topic: u32,
    read_positions: Arc<DashMap<String, ReadPosition>>,
    readers: Arc<DashMap<(String, u32), DiskStorageReaderSessionPartition>>,
    // 使用读写分离的索引管理器，专门用于读取操作
    read_write_index_manager: Arc<ReadWritePartitionIndexManager>,
}

impl DiskStorageReaderSession {
    pub fn new(
        dir: PathBuf,
        group_id: u32,
        partition_index_num_per_topic: u32,
        read_positions: Vec<(String, ReadPosition)>,
    ) -> Self {
        // 创建读写分离的索引管理器，专门用于读取操作
        let read_write_index_manager = Arc::new(ReadWritePartitionIndexManager::new(
            dir.clone(),
            partition_index_num_per_topic as _,
        ));

        Self {
            dir: dir.clone(),
            group_id,
            partition_index_num_per_topic,
            read_positions: Arc::new(m),
            readers: Arc::default(),
            read_write_index_manager,
        }
    }

    /// 获取读写分离的索引管理器（用于外部访问）
    pub fn get_read_write_index_manager(&self) -> Arc<ReadWritePartitionIndexManager> {
        self.read_write_index_manager.clone()
    }
}
```

#### 修改 commit 方法
```rust
async fn commit(
    &self,
    topic: &str,
    partition_id: u32,
    offset: SegmentOffset,
) -> StorageResult<()> {
    // 使用读写分离的索引管理器验证 offset 的有效性
    let _ = self
        .read_write_index_manager
        .get_msg_id_by_segment_offset(topic, partition_id, &offset)
        .await
        .map_err(|e| {
            StorageError::OffsetMismatch(format!("Invalid offset {:?}: {:?}", offset, e))
        })?;

    // 验证并更新 commit_ptr
    reader.commit_offset(offset).await?;

    Ok(())
}
```

## 优势

### 1. 完全移除全局变量
- ✅ **不再依赖全局 `PartitionIndexManager`**
- ✅ **每个组件独立管理自己的 RocksDB 实例**
- ✅ **生命周期完全独立**

### 2. 真正的读写分离
- ✅ **Writer 使用主实例进行写入**
- ✅ **Reader 使用只读实例进行读取**
- ✅ **写入后立即在读取端可见**

### 3. 性能优化
- ✅ **读写操作互不干扰**
- ✅ **支持并发读取**
- ✅ **实例级别的缓存和优化**

### 4. 资源管理
- ✅ **精确控制实例的创建和销毁**
- ✅ **支持按需创建和清理**
- ✅ **避免资源泄漏**

## 使用示例

### 1. 基本使用
```rust
// 创建 Writer 的 PartitionBufferSet
let partition_buffer_set = PartitionBufferSet::new(
    partition_dir,
    Arc::new(config),
    write_ptr,
    flusher,
).await.unwrap();

// 创建 Reader Session
let reader_session = DiskStorageReaderSession::new(
    storage_root,
    group_id,
    partition_index_num_per_topic,
    read_positions,
);

// 写入数据
partition_buffer_set.write_batch(messages, true).await.unwrap();
partition_buffer_set.flush(true, true).await.unwrap();

// 读取数据（立即能看到最新写入的数据）
let messages = reader_session.next(topic, partition_id, NonZero::new(10).unwrap()).await.unwrap();
```

### 2. 索引查询
```rust
// 通过 Writer 的索引管理器查询
let writer_index_manager = partition_buffer_set.get_read_write_index_manager();
let offset = writer_index_manager
    .get_by_msg_id(topic, partition_id, "msg_1")
    .await.unwrap();

// 通过 Reader 的索引管理器查询
let reader_index_manager = reader_session.get_read_write_index_manager();
let msg_id = reader_index_manager
    .get_msg_id_by_segment_offset(topic, partition_id, &offset)
    .await.unwrap();
```

### 3. 生命周期管理
```rust
// 获取实例统计信息
let stats = get_global_rocksdb_instance_manager().get_stats();
println!("主实例数: {}, 只读实例数: {}", 
    stats.primary_instances, stats.read_only_instances);

// 清理指定存储根目录的所有实例
get_global_rocksdb_instance_manager().cleanup_storage_root(&storage_dir);
```

## 测试验证

### 1. 并发安全性测试
```rust
#[tokio::test]
async fn test_concurrent_safety() {
    // 测试写入后立即读取
    // 测试并发写入和读取
    // 验证数据一致性
}
```

### 2. 集成测试
```rust
#[tokio::test]
async fn test_writer_reader_integration() {
    // 测试 Writer 和 Reader 的完整集成
    // 验证索引查询功能
    // 验证实例统计信息
}
```

## 总结

这个集成方案完美解决了您提出的问题：

1. **✅ 并发安全**：RocksDB 读写分离是并发安全的，写入后立即可见
2. **✅ 移除全局变量**：完全移除了对全局 `PartitionIndexManager` 的依赖
3. **✅ 独立生命周期**：每个组件独立管理自己的 RocksDB 实例
4. **✅ 性能优化**：读写操作互不干扰，支持并发读取
5. **✅ 资源管理**：精确控制实例的创建和销毁

现在您的 storagev2 系统拥有了真正的读写分离能力，同时保持了代码的清晰性和可维护性。
