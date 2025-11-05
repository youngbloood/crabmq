use anyhow::{Result, anyhow};
use std::path::PathBuf;

/// 查询系统的 IOV_MAX 值
///
/// IOV_MAX 是 writev/readv 系统调用一次能接受的最大 IoSlice 数量
///
/// # 返回值
/// - 成功：返回系统的 IOV_MAX 值
/// - 失败：返回默认值 1024（POSIX 系统通常为 1024）
pub fn get_system_iov_max() -> usize {
    #[cfg(unix)]
    {
        // 使用 sysconf 查询 IOV_MAX
        unsafe {
            let value = libc::sysconf(libc::_SC_IOV_MAX);
            if value > 0 {
                return value as usize;
            }
        }
    }

    // 默认值：1024（Linux/macOS 标准值）
    1024
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub storage_dir: PathBuf,

    // 刷盘相关配置
    // 刷盘周期，单位 ms
    pub flusher_period: u64,
    // 刷盘因子，内存中的消息量超过该值时刷盘，默认： 4 * 1024 * 1024（4m）
    pub flusher_factor: u64,
    // 刷盘配置：刷分区的任务数量
    pub flusher_partition_writer_buffer_tasks_num: usize,
    // 刷盘配置：刷分区写指针的任务数量
    pub flusher_partition_writer_ptr_tasks_num: usize,
    // 刷盘配置：刷数据索引的任务数量
    pub flusher_partition_meta_tasks_num: usize,

    // 每个分区配置
    // partition record file prealloc
    pub partition_writer_prealloc: bool,
    // 分区活跃检查间隔(秒)
    pub partition_cleanup_interval: u64,
    // 分区不活跃阈值(秒)，超过该值会被移除内存，等待下次活跃时加载
    pub partition_inactive_threshold: u64,
    // 默认从分区buffer中弹出的大小
    pub batch_pop_size_from_buffer: u64,
    // writev 系统调用的最大 IoSlice 数量限制（IOV_MAX）
    // Linux/macOS 通常是 1024，POSIX 最小要求是 16
    // 可以通过 getconf IOV_MAX 查询系统值
    pub iov_max: usize,

    // 每个 topic 下默认多少个索引分区（不同partition根据规则路由至index下进行存储，防止Too many open files）
    pub partition_index_num_per_topic: u32,

    // 写时的 worker 任务数量
    pub writer_worker_tasks_num: usize,
    // 默认每个消息文件中的最大消息数量
    pub max_msg_num_per_file: u64,
    // 默认每个消息文件中的最大消息字节数
    pub max_size_per_file: u64,
    pub compress_type: u8,

    // 预创建下一个消息文件的阈值，默认: 90，即当前文件已写了90%，开启下一个文件的预创建
    pub create_next_record_file_threshold: u8,

    // 开 metrics 统计
    pub with_metrics: bool,

    // 是否启用索引（性能测试时可禁用）
    pub enable_index: bool,

    // RocksDB 配置参数
    pub rocksdb_max_open_files: i32,
    pub rocksdb_write_buffer_size: usize,             // 单位：字节
    pub rocksdb_max_write_buffer_number: i32,
    pub rocksdb_target_file_size_base: u64,           // 单位：字节
    pub rocksdb_max_background_jobs: i32,
    pub rocksdb_level_zero_file_num_compaction_trigger: i32,
    pub rocksdb_level_zero_slowdown_writes_trigger: i32,
    pub rocksdb_level_zero_stop_writes_trigger: i32,
    pub rocksdb_disable_wal: bool,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let must_gt_zero = |attr, v| -> Result<()> {
            if v == 0 {
                return Err(anyhow!(format!("'{}' must be grater than zero", attr)));
            }
            Ok(())
        };
        must_gt_zero("flusher_period", self.flusher_period)?;
        must_gt_zero("flusher_factor", self.flusher_factor)?;
        must_gt_zero("max_msg_num_per_file", self.max_msg_num_per_file)?;
        must_gt_zero("max_size_per_file", self.max_size_per_file)?;
        must_gt_zero(
            "create_next_record_file_threshold",
            self.create_next_record_file_threshold as _,
        )?;
        must_gt_zero("pop_size_from_buffer", self.batch_pop_size_from_buffer as _)?;

        Ok(())
    }

    pub fn with_storage_dir(mut self, storage_dir: PathBuf) -> Self {
        self.storage_dir = storage_dir;
        self
    }
}

pub fn default_config() -> Config {
    Config {
        storage_dir: PathBuf::from("./messages"),
        flusher_period: 50,              // 50ms
        flusher_factor: 1024 * 1024 * 4, // 4M
        flusher_partition_writer_buffer_tasks_num: 64,
        flusher_partition_writer_ptr_tasks_num: 64,
        flusher_partition_meta_tasks_num: 64,
        partition_writer_prealloc: false,
        partition_cleanup_interval: 150,
        partition_inactive_threshold: 300,
        batch_pop_size_from_buffer: 128, // 优化：128 条消息 × 7 IoSlice = 896 < 1024（单次 write_vectored）
        iov_max: 1024, // Linux/macOS 系统默认值，可通过 getconf IOV_MAX 查询
        partition_index_num_per_topic: 100,
        max_msg_num_per_file: 1024 * 1024 * 1024 * 10,
        max_size_per_file: 1024 * 1024 * 1024, // 1G
        compress_type: 0,
        writer_worker_tasks_num: 100,
        create_next_record_file_threshold: 90,
        with_metrics: false,
        enable_index: true, // 默认启用索引，性能测试时可设为 false

        // RocksDB 配置默认值（针对高性能写入优化）
        rocksdb_max_open_files: 10000,
        rocksdb_write_buffer_size: 128 * 1024 * 1024, // 128MB
        rocksdb_max_write_buffer_number: 8,
        rocksdb_target_file_size_base: 128 * 1024 * 1024, // 128MB
        rocksdb_max_background_jobs: 8,
        rocksdb_level_zero_file_num_compaction_trigger: 8,
        rocksdb_level_zero_slowdown_writes_trigger: 20,
        rocksdb_level_zero_stop_writes_trigger: 36,
        rocksdb_disable_wal: false, // 默认不禁用 WAL，可根据需求设为 true
    }
}
