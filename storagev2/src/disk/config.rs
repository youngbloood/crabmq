use anyhow::{Result, anyhow};
use std::path::PathBuf;
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
    // PartitionWriter 中内存缓冲区长度，每个分区缓存写入消息的长度
    pub partition_writer_buffer_size: usize,
    // partition record file prealloc
    pub partition_writer_prealloc: bool,
    // 分区活跃检查间隔(秒)
    pub partition_cleanup_interval: u64,
    // 分区不活跃阈值(秒)，超过该值会被移除内存，等待下次活跃时加载
    pub partition_inactive_threshold: u64,
    // 默认从分区buffer中弹出的大小
    pub batch_pop_size_from_buffer: u64,

    // 每个 topic 下默认多少个索引分区（不同partition根据规则路由至index下进行存储，防止Too many open files）
    pub partition_index_num_per_topic: u32,

    // 写时的 worker 任务数量
    pub writer_worker_tasks_num: usize,
    // 默认每个消息文件中的最大消息数量
    pub max_msg_num_per_file: u64,
    // 默认每个消息文件中的最大消息字节数
    pub max_size_per_file: u64,
    pub compress_type: u8,

    pub fd_cache_size: u64,

    // 预创建下一个消息文件的阈值，默认: 90，即当前文件已写了90%，开启下一个文件的预创建
    pub create_next_record_file_threshold: u8,

    // 开 metrics 统计
    pub with_metrics: bool,
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
        must_gt_zero("fd_cache_size", self.fd_cache_size)?;
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
        partition_writer_buffer_size: 100,
        partition_writer_prealloc: false,
        partition_cleanup_interval: 150,
        partition_inactive_threshold: 300,
        batch_pop_size_from_buffer: 200,
        partition_index_num_per_topic: 100,
        max_msg_num_per_file: 1024 * 1024 * 1024 * 10,
        max_size_per_file: 1024 * 1024 * 1024, // 1G
        compress_type: 0,
        writer_worker_tasks_num: 100,
        fd_cache_size: 256,
        create_next_record_file_threshold: 90,
        with_metrics: false,
    }
}
