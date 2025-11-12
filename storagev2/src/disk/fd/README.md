# FD 抽象层 - 统一的磁盘 I/O 接口

## 概述

`fd` 模块提供了统一的磁盘读写抽象，支持多种 I/O 模式：
- **WriteVectored**: 零拷贝的 `write_vectored` 实现
- **Mmap**: 内存映射的高性能实现

通过 `DiskWriteMode` 配置可以轻松切换不同的 I/O 模式，无需修改业务代码。

## 核心特性

### 1. 并发安全
- 所有实现均使用 `Mutex` 保护文件句柄
- 支持多线程并发读写
- 原子操作追踪读写位置

### 2. 高性能
- **WriteVectored**: 使用 `writev` 系统调用，零拷贝批量写入
- **Mmap**: 内存映射减少用户态/内核态切换
- 预分配文件空间减少磁盘碎片
- 使用 `O_DSYNC` 标志确保数据持久化

### 3. 易用性
- 统一的 `Writer` 和 `Reader` trait
- 工厂函数自动根据模式创建对应实现
- 支持配置化切换 I/O 模式

## 使用示例

### 基础使用

```rust
use storagev2::disk::fd::{create_writer, Writer};
use storagev2::disk::DiskWriteMode;
use std::io::IoSlice;

// 创建 WriteVectored 写入器
let mut writer = create_writer(
    Path::new("test.dat"),
    DiskWriteMode::WriteVectored,
    true,  // 预分配
    1024 * 1024 * 1024,  // 1GB
).await?;

// 零拷贝批量写入
let data1 = b"Hello";
let data2 = b"World";
let iovecs = [
    IoSlice::new(data1),
    IoSlice::new(data2),
];
let written = writer.write(&iovecs).await?;
println!("写入 {} 字节", written);

// 同步到磁盘
writer.sync_data().await?;
```

### 切换 I/O 模式

```rust
// 切换到 Mmap 模式
let mut writer = create_writer(
    Path::new("test.dat"),
    DiskWriteMode::Mmap,  // 使用 Mmap
    true,
    1024 * 1024 * 1024,
).await?;

// 使用方式完全相同
let written = writer.write(&iovecs).await?;
```

### 与 storagev2 集成

在 `Config` 中配置：

```rust
use storagev2::disk::{default_config, DiskWriteMode};

let mut config = default_config();
config.disk_write_mode = DiskWriteMode::WriteVectored; // 或 Mmap
```

## 性能对比

| 模式 | 优点 | 缺点 | 适用场景 |
|------|------|------|----------|
| **WriteVectored** | - 零拷贝<br>- 系统调用少 | - 受 IOV_MAX 限制 | 批量写入小块数据 |
| **Mmap** | - 减少系统调用<br>- 适合大文件 | - 需要内存拷贝 | 大文件顺序写入 |

## 实现细节

### WriteVectored

```rust
pub struct VectoredWriter {
    fd: Mutex<AsyncFile>,
    write_pos: AtomicU64,
}
```

- 使用 `Mutex<AsyncFile>` 保证并发安全
- `write_pos` 原子追踪写入位置
- 使用 `O_DSYNC` 确保数据同步

### Mmap

```rust
pub struct MmapWriter {
    fd: Mutex<File>,
    mmap: Mutex<MmapMut>,
    write_pos: AtomicU64,
    file_len: AtomicU64,
}
```

- 支持自动扩容和重新映射
- 使用信号量限制并发 mmap 操作
- 指数增长策略减少扩容次数

## 扩展性

未来可以轻松添加新的 I/O 模式：
- **io_uring**: Linux 的异步 I/O
- **DirectIO**: 绕过页缓存的直接 I/O
- **SPDK**: 用户态 NVMe 驱动

只需：
1. 在 `DiskWriteMode` 添加新的枚举值
2. 实现 `Writer` 和 `Reader` trait
3. 在工厂函数中添加对应分支

## 注意事项

1. **预分配空间**: 建议启用预分配以减少磁盘碎片
2. **并发控制**: Mmap 模式限制并发操作数量（默认 2048）
3. **错误处理**: 写入失败时需要检查磁盘空间和权限
4. **性能测试**: 根据实际负载选择最优模式

## 测试

运行性能基准测试：

```bash
cd cmd/storage_bench
cargo run --release
```
