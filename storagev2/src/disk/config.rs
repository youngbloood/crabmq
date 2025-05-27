use std::path::PathBuf;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiskConfig {
    pub storage_dir: PathBuf,
    /// 刷盘周期，单位 ms
    pub persist_period: u64,
    pub persist_factor: u64,
    // 默认每个消息文件中的最大消息数量
    pub max_msg_num_per_file: u64,
    // 默认每个消息文件中的最大消息字节数
    pub max_size_per_file: u64,
    pub compress_type: u8,
    pub fd_cache_size: u64,
}
