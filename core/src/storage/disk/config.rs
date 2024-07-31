use clap::Parser;
use protocol::ProtocolHead;
use std::path::PathBuf;

#[derive(Parser, Debug, serde::Deserialize, Clone)]
pub struct DiskConfig {
    #[arg(long = "storage-dir", default_value = "./message")]
    pub storage_dir: PathBuf,

    #[arg(long = "persist-message-period", default_value_t = 500)]
    pub persist_period: u64,

    #[arg(long = "persist-message-factor", default_value_t = 50)]
    pub persist_factor: u64,

    #[arg(
        long = "default-defer-message-format",
        default_value = "{daily}/{hourly}/{minutely:20}"
    )]
    pub default_defer_message_format: String,

    /// [`max_num_per_file`] * 100 约等于 10G
    ///
    /// 每个文件最多存放消息数量
    #[arg(long = "max-num-per-file", default_value_t = 107374180)]
    pub default_max_msg_num_per_file: u64,

    /// 每个文件最多存放消息大小(Byte)，默认5G
    #[arg(long = "max-size-per-file", default_value_t = 10737418240)]
    pub default_max_size_per_file: u64,

    #[arg(long = "compress-type", default_value_t = 0)]
    pub default_compress_type: u8,

    #[arg(long = "subscript-type", default_value_t = 0)]
    pub default_subscribe_type: u8,

    #[arg(long = "record-num-per-file", default_value_t = 107374180)]
    pub default_record_num_per_file: u64,

    /// 每个文件最多存放记录大小(Byte)，默认5G
    #[arg(long = "record-size-per-file", default_value_t = 10737418240)]
    pub default_record_size_per_file: u64,

    #[arg(long = "fd-cache-size", default_value_t = 20)]
    pub default_fd_cache_size: u64,
}

impl DiskConfig {
    pub fn mix_with_protocolhead(&self, head: &ProtocolHead) -> Self {
        let mut res = self.clone();
        match head {
            ProtocolHead::V1(v1) => {
                if !v1.get_defer_msg_format().is_empty() {
                    res.default_defer_message_format = v1.get_defer_msg_format().to_string();
                }
                if v1.get_max_msg_num_per_file() != 0 {
                    res.default_max_msg_num_per_file = v1.get_max_msg_num_per_file();
                }
                if v1.get_max_size_per_file() != 0 {
                    res.default_max_size_per_file = v1.get_max_size_per_file();
                }
                if v1.get_compress_type() != 0 {
                    res.default_compress_type = v1.get_compress_type();
                }
                if v1.get_subscribe_type() != 0 {
                    res.default_subscribe_type = v1.get_subscribe_type();
                }
                if v1.get_record_num_per_file() != 0 {
                    res.default_record_num_per_file = v1.get_record_num_per_file();
                }
                if v1.get_record_size_per_file() != 0 {
                    res.default_record_size_per_file = v1.get_record_size_per_file();
                }
                if v1.get_fd_cache_size() != 0 {
                    res.default_fd_cache_size = v1.get_fd_cache_size();
                }
            }
        }
        res
    }
}
