use anyhow::{Result, anyhow};
use std::path::PathBuf;
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub storage_dir: PathBuf,
    // 刷盘周期，单位 ms
    pub flusher_period: u64,
    // 刷盘因子，内存中的消息量超过该值时刷盘，默认： 4 * 1024 * 1024（4m）
    pub flusher_factor: u64,
    // 默认每个消息文件中的最大消息数量
    pub max_msg_num_per_file: u64,
    // 默认每个消息文件中的最大消息字节数
    pub max_size_per_file: u64,
    pub compress_type: u8,

    pub fd_cache_size: u64,

    // 预创建下一个消息文件的阈值，默认: 80，即当前文件已写了80%，开启下一个文件的预创建
    pub create_next_record_file_threshold: u8,
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

        Ok(())
    }
}

pub fn default_config() -> Config {
    Config {
        storage_dir: PathBuf::from("./messages"),
        flusher_period: 50,
        flusher_factor: 1024 * 1024 * 4,               // 4M
        max_msg_num_per_file: 1024 * 1024 * 1024 * 15, // 15G
        max_size_per_file: 1024 * 1024 * 1024 * 15 * 5,
        compress_type: 0,
        fd_cache_size: 15,
        create_next_record_file_threshold: 80,
    }
}
