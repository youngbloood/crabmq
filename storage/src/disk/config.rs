use clap::Parser;
use std::path::PathBuf;

use crate::TopicMeta;

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
    pub fn mix_with_topicmeta(&self, meta: &TopicMeta) -> Self {
        let mut res = self.clone();

        if !meta.defer_message_format.is_empty() {
            res.default_defer_message_format = meta.defer_message_format.clone();
        }
        if meta.max_msg_num_per_file != 0 {
            res.default_max_msg_num_per_file = meta.max_msg_num_per_file;
        }
        if meta.max_size_per_file != 0 {
            res.default_max_size_per_file = meta.max_size_per_file;
        }
        if meta.compress_type != 0 {
            res.default_compress_type = meta.compress_type;
        }
        if meta.subscribe_type != 0 {
            res.default_subscribe_type = meta.subscribe_type;
        }
        if meta.record_num_per_file != 0 {
            res.default_record_num_per_file = meta.record_num_per_file;
        }
        if meta.record_size_per_file != 0 {
            res.default_record_size_per_file = meta.record_size_per_file;
        }
        if meta.fd_cache_size != 0 {
            res.default_fd_cache_size = meta.fd_cache_size;
        }
        res
    }

    pub fn gen_topic_meta(&self) -> TopicMeta {
        TopicMeta {
            prohibit_instant: false,
            prohibit_defer: false,
            defer_message_format: String::new(),
            max_msg_num_per_file: 0,
            max_size_per_file: 0,
            compress_type: 0,
            subscribe_type: 0,
            record_num_per_file: 0,
            record_size_per_file: 0,
            fd_cache_size: 0,
        }
    }
}
