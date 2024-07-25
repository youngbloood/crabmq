use anyhow::Result;
use clap::Parser;
use std::fs;
use tracing::Level;
use tracing_appender::rolling::{daily, hourly, minutely, never};

use crate::storage::disk::config::DiskConfig;

const DEFAULT_BUFFER: u64 = 10000;
const DEFAULT_FACTOR: u16 = 100;
const DEFAULT_NUM: u64 = 10000;

#[derive(Parser, Debug, serde::Deserialize)]
pub struct Global {
    #[arg(long = "log-dir", default_value = "")]
    pub log_dir: String,

    #[arg(long = "log-filename", default_value = "")]
    pub log_filename: String,

    #[arg(long = "log-level", default_value = "trace")]
    pub log_level: String,

    #[arg(long = "log-rolling", default_value = "")]
    /// log rolling type
    pub log_rolling: String,

    #[arg(short = 'p', long = "port", default_value_t = 3890)]
    /// [`tcp`] 监听端口
    pub tcp_port: u32,

    #[arg(long = "msg-num-buffer", default_value_t = DEFAULT_BUFFER as u16)]
    /// 客户端发送的params缓存数量
    pub msg_num_buffer: u16,

    #[arg(long = "memory-msg-size", default_value_t = DEFAULT_BUFFER)]
    /// 内存中存储消息体的大小
    pub memory_message_size: u64,

    #[arg(long = "memory-msg-count", default_value_t = DEFAULT_BUFFER)]
    /// 内存中存储消息的数量
    pub memory_message_count: u64,

    #[arg(long = "topic-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    /// [`topic`] 中消息数量缓存数
    pub topic_message_buffer: u64,

    #[arg(long = "channel-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    /// [`channel`] 中的消息数量缓存数
    pub channel_message_buffer: u64,

    #[arg(long = "channel-num-in-topic", default_value_t = DEFAULT_BUFFER)]
    /// 一个 [`topic`] 下的 [`channel`] 数量
    pub channel_num_in_topic: u64,

    #[arg(long = "max-topic-num", default_value_t = DEFAULT_BUFFER)]
    /// 一个 [`crabmq`] 下的topic数量
    pub max_topic_num: u64,

    #[arg(long = "client-heartbeat-interval", default_value_t = 30)]
    /// 一个 [`client`] 收到心跳包的时间间隔，单位(s)
    pub client_heartbeat_interval: u16,

    #[arg(long = "client-expire-count", default_value_t = 3)]
    /// 一个 [`client超时次数限制，超过该限制client会主动断开
    ///
    /// 超时定义：固定[`client_heartbeat_interval`]时间内未收到包为超时
    pub client_expire_count: u16,

    #[arg(long = "client-read-timeout", default_value_t = 30)]
    /// 一个 [`client`] 读取数据超时时间
    pub client_read_timeout: u64,

    #[arg(long = "client-read-timeout-count", default_value_t = 3)]
    /// 一个 [`client`] 读取数据超时次数，超过该次数会断开链接
    pub client_read_timeout_count: u64,

    #[arg(long = "client-write-timeout", default_value_t = 30)]
    /// 一个 [`client`] 写入数据超时时间
    pub client_write_timeout: u64,

    #[arg(long = "client-write-timeout-count", default_value_t = 3)]
    /// 一个 [`client`] 写入数据超时次数，超过该次数会断开链接
    pub client_write_timeout_count: u64,

    #[arg(long = "message-storage-type", default_value = "local")]
    /// 设置消息持久化类型
    pub message_storage_type: String,

    #[arg(long = "message-cache-type", default_value = "memory")]
    /// 设置消息 [`cache`] 类型，从 [`message-storage-type`] 中读取的消息会放入该 [`cache`] 中
    pub message_cache_type: String,
}

pub struct Config {
    pub global: Global,
    pub message_storage_disk_config: DiskConfig,
}

impl Config {
    pub fn from_config(filename: &str) -> Result<Config> {
        let mut global = Global::parse();
        let mut disk_cfg = DiskConfig::parse();
        if !filename.is_empty() {
            let content = fs::read_to_string(filename)?;
            global = serde_yaml::from_str(&content)?;
            disk_cfg = serde_yaml::from_str(&content)?;
        }

        Ok(Config {
            global,
            message_storage_disk_config: disk_cfg,
        })
    }

    pub fn get_log_level(&self) -> Level {
        match self.global.log_level.as_str() {
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            "debug" => Level::DEBUG,
            "trace" => Level::TRACE,
            _ => Level::INFO,
        }
    }

    pub fn init_log(&self) -> Result<()> {
        // 初始化并设置日志格式
        let suber = tracing_subscriber::fmt().with_max_level(self.get_log_level());
        // .with_ansi(false);

        if self.global.log_dir.is_empty() && self.global.log_filename.is_empty() {
            suber.init();
            return Ok(());
        }

        let appender = match self.global.log_rolling.as_str() {
            "hourly" => hourly(
                self.global.log_dir.as_str(),
                self.global.log_filename.as_str(),
            ),
            "minutely" => minutely(
                self.global.log_dir.as_str(),
                self.global.log_filename.as_str(),
            ),
            "never" => never(
                self.global.log_dir.as_str(),
                self.global.log_filename.as_str(),
            ),
            _ => daily(
                self.global.log_dir.as_str(),
                self.global.log_filename.as_str(),
            ),
        };

        let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
        suber.with_writer(non_blocking).init();

        Ok(())
    }
}
