use crate::err::{ErrorCode, StorageError, StorageResult};
use std::path::PathBuf;

/// 磁盘写入方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DiskReadWriteMode {
    /// 使用 write_vectored 零拷贝写入（默认）
    WriteVectored,
    /// 使用 mmap 写入（需要内存拷贝组装连续内存）
    Mmap,
}

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
pub struct WriterConfig {
    pub storage_dir: PathBuf,

    // 刷盘相关配置
    // 刷盘周期，单位 ms
    pub flusher_period: u64,
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

    // 每个消息文件中的最大消息数量
    pub max_msg_num_per_file: u64,
    // 每个消息文件中的最大消息字节数
    pub max_size_per_file: u64,
    pub compress_type: u8,

    // 预创建下一个消息文件的阈值，默认: 90，即当前文件已写了90%，开启下一个文件的预创建
    pub create_next_record_file_threshold: u8,

    // 开 metrics 统计
    pub with_metrics: bool,

    // 磁盘写入方式：WriteVectored（零拷贝）或 Mmap（内存拷贝）
    pub disk_write_mode: DiskReadWriteMode,

    // 每个分区缓冲区的最大消息字节数，超过该值会写入失败
    pub message_size_limit_per_partition: u64,
    // 全局消息大小限制，超过该值会写入失败
    pub message_size_limit_global: u64,
    // RocksDB 配置参数
    // pub rocksdb_max_open_files: i32,
    // pub rocksdb_write_buffer_size: usize, // 单位：字节
    // pub rocksdb_max_write_buffer_number: i32,
    // pub rocksdb_target_file_size_base: u64, // 单位：字节
    // pub rocksdb_max_background_jobs: i32,
    // pub rocksdb_level_zero_file_num_compaction_trigger: i32,
    // pub rocksdb_level_zero_slowdown_writes_trigger: i32,
    // pub rocksdb_level_zero_stop_writes_trigger: i32,
    // pub rocksdb_disable_wal: bool,
}

impl WriterConfig {
    pub fn validate(&self) -> StorageResult<()> {
        let must_gt_zero = |attr, v| -> StorageResult<()> {
            if v == 0 {
                return Err(StorageError::with_message(
                    ErrorCode::InvalidConfigParameter,
                    format!("'{}' must be grater than 0", attr),
                )
                .into());
            }
            Ok(())
        };

        let must_gt_const = |attr: &str, v: usize, const_value: usize| -> StorageResult<()> {
            if v <= const_value {
                return Err(StorageError::with_message(
                    ErrorCode::InvalidConfigParameter,
                    format!("'{}' must be grater than {}", attr, const_value),
                )
                .into());
            }
            Ok(())
        };
        must_gt_zero("flusher_period", self.flusher_period)?;
        must_gt_zero(
            "flusher_partition_writer_buffer_tasks_num",
            self.flusher_partition_writer_buffer_tasks_num as _,
        )?;
        must_gt_zero(
            "flusher_partition_writer_ptr_tasks_num",
            self.flusher_partition_writer_ptr_tasks_num as _,
        )?;
        must_gt_zero(
            "flusher_partition_meta_tasks_num",
            self.flusher_partition_meta_tasks_num as _,
        )?;
        must_gt_zero(
            "partition_cleanup_interval",
            self.partition_cleanup_interval,
        )?;
        must_gt_zero(
            "partition_inactive_threshold",
            self.partition_inactive_threshold,
        )?;
        must_gt_zero("max_msg_num_per_file", self.max_msg_num_per_file)?;
        must_gt_zero("max_size_per_file", self.max_size_per_file)?;
        must_gt_zero(
            "create_next_record_file_threshold",
            self.create_next_record_file_threshold as _,
        )?;
        must_gt_zero("pop_size_from_buffer", self.batch_pop_size_from_buffer as _)?;

        must_gt_const(
            "message_size_limit_per_partition",
            self.message_size_limit_per_partition as _,
            2,
        )?;
        must_gt_const(
            "message_size_limit_global",
            self.message_size_limit_global as _,
            100, // 100 条数据
        )?;

        Ok(())
    }

    pub fn with_storage_dir(mut self, storage_dir: PathBuf) -> Self {
        self.storage_dir = storage_dir;
        self
    }

    pub fn fix(mut self) -> Self {
        // 获取系统 IOV_MAX 值，覆盖默认值
        if self.storage_dir.as_os_str().is_empty() {
            self.storage_dir = PathBuf::from("./messages");
        }
        if self.flusher_period == 0 {
            self.flusher_period = 50;
        }
        if self.flusher_partition_writer_buffer_tasks_num == 0 {
            self.flusher_partition_writer_buffer_tasks_num = 64;
        }
        if self.flusher_partition_writer_ptr_tasks_num == 0 {
            self.flusher_partition_writer_ptr_tasks_num = 64;
        }
        if self.flusher_partition_meta_tasks_num == 0 {
            self.flusher_partition_meta_tasks_num = 64;
        }
        if self.partition_cleanup_interval == 0 {
            self.partition_cleanup_interval = 150;
        }
        if self.partition_inactive_threshold == 0 {
            self.partition_inactive_threshold = 300;
        }
        if self.max_msg_num_per_file == 0 {
            self.max_msg_num_per_file = 1024 * 1024 * 1024 * 10;
        }
        if self.max_size_per_file == 0 {
            self.max_size_per_file = 1024 * 1024 * 1024;
        }
        if self.compress_type == 0 {
            self.compress_type = 0;
        }
        if self.batch_pop_size_from_buffer == 0 {
            self.batch_pop_size_from_buffer = 128;
        }
        if self.iov_max == 0 {
            self.iov_max = get_system_iov_max();
        }
        if self.create_next_record_file_threshold == 0 {
            self.create_next_record_file_threshold = 90;
        }
        if self.message_size_limit_per_partition == 0 {
            self.message_size_limit_per_partition = 100;
        }
        if self.message_size_limit_global == 0 {
            self.message_size_limit_global = 10000;
        }
        self
    }
}

impl Default for WriterConfig {
    fn default() -> Self {
        WriterConfig {
            storage_dir: PathBuf::from("./messages"),
            flusher_period: 50, // 50ms
            flusher_partition_writer_buffer_tasks_num: 64,
            flusher_partition_writer_ptr_tasks_num: 64,
            flusher_partition_meta_tasks_num: 64,
            partition_writer_prealloc: false,
            partition_cleanup_interval: 150,
            partition_inactive_threshold: 300,
            batch_pop_size_from_buffer: 128, // 优化：128 条消息 × 7 IoSlice = 896 < 1024（单次 write_vectored）
            iov_max: get_system_iov_max(),   // Linux/macOS 系统默认值，可通过 getconf IOV_MAX 查询
            max_msg_num_per_file: 1024 * 1024 * 1024 * 10,
            max_size_per_file: 1024 * 1024 * 1024, // 1G
            compress_type: 0,
            create_next_record_file_threshold: 90,
            with_metrics: false,
            disk_write_mode: DiskReadWriteMode::WriteVectored, // 默认使用零拷贝 write_vectored

            message_size_limit_per_partition: 1024 * 1024 * 1024 * 1, // 1GB
            message_size_limit_global: 1024 * 1024 * 1024 * 10,       // 10GB
        }
    }
}

pub struct ReaderConfig {
    pub dir: PathBuf,
}
