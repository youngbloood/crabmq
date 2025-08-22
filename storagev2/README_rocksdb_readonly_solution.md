# RocksDB 只读实例读写分离解决方案

## 问题背景

您提出的问题非常准确：**RocksDB 对一个文件夹只能有一个实例**，这是由其内部文件锁机制决定的。但是，RocksDB 支持以只读模式打开同一个数据库的多个实例，这为我们提供了读写分离的可能性。

## 解决方案核心

### 1. RocksDB 只读实例机制

RocksDB 提供了 `DB::open_for_read_only()` 方法，允许以只读模式打开同一个数据库的多个实例：

```rust
// 主实例（读写）
let mut opts = Options::default();
opts.create_if_missing(true);
let primary_db = DB::open(&opts, &db_path)?;

// 只读实例（可以创建多个）
let mut read_opts = Options::default();
read_opts.create_if_missing(false);
let read_only_db = DB::open_for_read_only(&read_opts, &db_path, false)?;
```

### 2. 读写分离架构

```
┌─────────────────┐    ┌─────────────────┐
│   主实例        │    │   只读实例池     │
│  (ReadWrite)    │    │  (ReadOnly)     │
│                 │    │                 │
│  - 支持读写     │    │  - 仅支持读     │
│  - 唯一实例     │    │  - 多个实例     │
│  - 文件锁       │    │  - 共享数据     │
└─────────────────┘    └─────────────────┘
         │                       │
         └───────────┬───────────┘
                     │
              ┌─────────────┐
              │ 数据文件    │
              │ (共享)      │
              └─────────────┘
```

## 实现方案

### 1. RocksDB 实例管理器

```rust
pub struct RocksDBInstanceManager {
    // 主实例（读写）
    primary_instances: Arc<DashMap<PathBuf, Arc<RocksDBInstance>>>,
    // 只读实例池
    read_only_instances: Arc<DashMap<PathBuf, Vec<Arc<RocksDBInstance>>>>,
    // 实例创建锁
    create_locks: Arc<DashMap<PathBuf, Mutex<()>>>,
}
```

### 2. 读写分离的索引管理器

```rust
pub struct ReadWritePartitionIndexManager {
    dir: PathBuf,
    size: usize,
    instance_manager: Arc<RocksDBInstanceManager>,
}
```

## 使用方法

### 1. 基本使用

```rust
// 创建读写分离的索引管理器
let index_manager = ReadWritePartitionIndexManager::new(
    PathBuf::from("./data"),
    100, // 索引分片数
);

// 写入操作（使用主实例）
let message_metas = vec![
    MessageMeta {
        msg_id: "msg_1".to_string(),
        timestamp: 1000,
        segment_id: 1,
        offset: 100,
        msg_len: 50,
    }
];

let bytes_written = index_manager
    .batch_put("topic", 1, &message_metas)
    .await?;

// 读取操作（使用只读实例）
let offset = index_manager
    .get_by_msg_id("topic", 1, "msg_1")
    .await?;
```

### 2. 生命周期管理

```rust
// 获取实例统计信息
let stats = get_global_rocksdb_instance_manager().get_stats();
println!("主实例数: {}, 只读实例数: {}", 
    stats.primary_instances, stats.read_only_instances);

// 清理指定存储根目录的所有实例
get_global_rocksdb_instance_manager().cleanup_storage_root(&storage_dir);
```

## 优势

### 1. 真正的读写分离
- ✅ **写入操作**：使用主实例，避免文件锁冲突
- ✅ **读取操作**：使用只读实例，支持并发读取
- ✅ **性能提升**：读写操作互不干扰

### 2. 生命周期独立管理
- ✅ **主实例**：由写入组件管理生命周期
- ✅ **只读实例**：由读取组件管理生命周期
- ✅ **资源清理**：支持按需创建和销毁

### 3. 扩展性
- ✅ **多只读实例**：支持创建多个只读实例
- ✅ **负载均衡**：可以在只读实例间分配查询负载
- ✅ **故障隔离**：单个实例故障不影响其他实例

### 4. 兼容性
- ✅ **向后兼容**：保持原有 API 不变
- ✅ **渐进迁移**：可以逐步迁移到新方案
- ✅ **配置灵活**：支持不同的 RocksDB 配置

## 技术细节

### 1. 主实例配置

```rust
// 主实例（读写）配置
opts.set_max_open_files(10000);
opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
opts.set_max_write_buffer_number(4);
opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
opts.set_max_background_jobs(4);
```

### 2. 只读实例配置

```rust
// 只读实例配置
opts.set_max_open_files(5000);
opts.set_allow_mmap_reads(true);
opts.set_allow_mmap_writes(false);
opts.set_max_background_jobs(2);
opts.enable_statistics();
```

### 3. 实例管理策略

```rust
// 获取只读实例的策略
pub async fn get_read_only_instance(&self, db_path: &PathBuf) -> Result<Arc<RocksDBInstance>> {
    // 1. 确保主实例存在
    let _primary = self.get_or_create_primary_instance(db_path).await?;
    
    // 2. 检查是否有可用的只读实例
    if let Some(read_only_list) = self.read_only_instances.get(db_path) {
        let instances = read_only_list.value();
        if !instances.is_empty() {
            return Ok(instances[0].clone());
        }
    }
    
    // 3. 创建新的只读实例
    let instance = create_read_only_instance(db_path).await?;
    
    // 4. 添加到只读实例池
    self.read_only_instances
        .entry(db_path.clone())
        .and_modify(|list| list.push(instance.clone()))
        .or_insert_with(|| vec![instance.clone()]);
    
    Ok(instance)
}
```

## 性能优化

### 1. 只读实例池

- **实例复用**：避免频繁创建和销毁只读实例
- **负载均衡**：在多个只读实例间分配查询负载
- **内存优化**：只读实例可以共享内存映射

### 2. 配置优化

- **主实例**：优化写入性能，较大的写缓冲区
- **只读实例**：优化读取性能，启用内存映射和统计

### 3. 并发控制

- **创建锁**：避免并发创建实例时的竞争条件
- **双重检查**：减少锁的持有时间
- **异步操作**：使用 `spawn_blocking` 避免阻塞

## 测试验证

### 1. 功能测试

```rust
#[tokio::test]
async fn test_read_write_separation() {
    // 测试读写分离的基本功能
    let index_manager = ReadWritePartitionIndexManager::new(storage_root, 10);
    
    // 写入数据
    let write_result = index_manager.batch_put("topic", 1, &message_metas).await;
    assert!(write_result.is_ok());
    
    // 读取数据
    let read_result = index_manager.get_by_msg_id("topic", 1, "msg_1").await;
    assert!(read_result.is_ok());
}
```

### 2. 性能测试

```rust
#[tokio::test]
async fn test_performance() {
    // 测试读写分离的性能提升
    let start = Instant::now();
    
    // 并发写入
    let write_handles: Vec<_> = (0..10).map(|i| {
        let manager = index_manager.clone();
        tokio::spawn(async move {
            manager.batch_put("topic", i, &message_metas).await
        })
    }).collect();
    
    // 并发读取
    let read_handles: Vec<_> = (0..20).map(|i| {
        let manager = index_manager.clone();
        tokio::spawn(async move {
            manager.get_by_msg_id("topic", i % 10, "msg_1").await
        })
    }).collect();
    
    // 等待所有操作完成
    let _: Vec<_> = join_all(write_handles).await;
    let _: Vec<_> = join_all(read_handles).await;
    
    let duration = start.elapsed();
    println!("总耗时: {:?}", duration);
}
```

## 总结

这个解决方案完美解决了您提出的问题：

1. **✅ 解决 RocksDB 单实例限制**：利用 RocksDB 的只读实例机制
2. **✅ 实现真正的读写分离**：写入使用主实例，读取使用只读实例
3. **✅ 独立生命周期管理**：读写组件可以独立管理各自的实例
4. **✅ 性能优化**：读写操作互不干扰，支持并发读取
5. **✅ 扩展性强**：支持多个只读实例，便于负载均衡

这个方案既保持了 RocksDB 的文件锁机制，又实现了您需要的读写分离和独立生命周期管理。
