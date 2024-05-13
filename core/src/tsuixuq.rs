use crate::channel::Channel;
use crate::message::Message;
use crate::topic::Topic;
use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;
use common::global::Guard;
use config::{Config, File};
use std::collections::HashMap;
use tracing::Level;
use tracing_appender::rolling::{daily, hourly, minutely, never, RollingFileAppender};

const DEFAULT_BUFFER: u64 = 10000;
const DEFAULT_FACTOR: u16 = 100;
const DEFAULT_NUM: u64 = 10000;

#[derive(Parser, Debug, serde::Deserialize)]
pub struct TsuixuqOption {
    #[arg(long = "log-dir", default_value = "")]
    pub log_dir: String,

    #[arg(long = "log-filename", default_value = "")]
    pub log_filename: String,

    #[arg(long = "log-level", default_value = "debug")]
    pub log_level: String,

    #[arg(long = "log-rolling", default_value = "")]
    /// log rolling type
    pub log_rolling: String,

    #[arg(short = 'p', long = "port", default_value_t = 3890)]
    /// tcp监听端口
    pub tcp_port: u16,

    #[arg(long = "msg-num-buffer", default_value_t = DEFAULT_BUFFER as u16)]
    /// 客户端发送的params缓存数量
    pub msg_num_buffer: u16,

    #[arg(long = "memory-msg-size", default_value_t = DEFAULT_BUFFER)]
    /// 内存中存储message的大小
    pub memory_message_size: u64,

    #[arg(long = "memory-msg-count", default_value_t = DEFAULT_BUFFER)]
    /// 内存中存储message的数量
    pub memory_message_count: u64,

    #[arg(long = "msg-flush-factor", default_value_t = DEFAULT_FACTOR)]
    /// messag flush至disk因子
    pub message_flush_factor: u16,

    #[arg(long = "msg-flush-interval", default_value_t = DEFAULT_FACTOR as u64)]
    /// messaage flush至disk的时间间隔
    pub message_flush_interval: u64,

    #[arg(long = "topic-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    /// topic中message数量缓存数
    pub topic_message_buffer: u64,

    #[arg(long = "channel-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    /// channel中的message数量缓存数
    pub channel_message_buffer: u64,

    #[arg(long = "channel-num-in-topic", default_value_t = DEFAULT_BUFFER)]
    /// 一个topic下的channel数量
    pub channel_num_in_topic: u64,

    #[arg(long = "topic-num-in-tsuixuq", default_value_t = DEFAULT_BUFFER)]
    /// 一个tsuixuq下的topic数量
    pub topic_num_in_tsuixuq: u64,

    #[arg(long = "client-heartbeat-interval", default_value_t = 30)]
    /// 一个client收到心跳包的时间间隔，单位(s)
    pub client_heartbeat_interval: u16,

    #[arg(long = "client-expire-count", default_value_t = 3)]
    /// 一个client超时次数限制，超过该限制client会主动断开
    ///
    /// 超时定义：固定[`client_heartbeat_interval`]时间内未收到包为超时
    pub client_expire_count: u16,

    #[arg(long = "client-read-timeout", default_value_t = 30)]
    /// 一个client读取数据超时时间
    pub client_read_timeout: u64,

    #[arg(long = "client-read-timeout-count", default_value_t = 3)]
    /// 一个client读取数据超时次数，超过该次数会断开链接
    pub client_read_timeout_count: u64,

    #[arg(long = "client-write-timeout", default_value_t = 30)]
    /// 一个client写入数据超时时间
    pub client_write_timeout: u64,

    #[arg(long = "client-write-timeout-count", default_value_t = 3)]
    /// 一个client写入数据超时次数，超过该次数会断开链接
    pub client_write_timeout_count: u64,
}

impl TsuixuqOption {
    pub fn from_config(filename: &str) -> Result<TsuixuqOption> {
        let mut opt = TsuixuqOption::parse();
        if filename.len() != 0 {
            let cfg = Config::builder()
                .add_source(File::with_name(filename))
                .build()?;
            opt = cfg.try_deserialize()?;
        }

        Ok(opt)
    }

    pub fn get_log_level(&self) -> Level {
        match self.log_level.as_str() {
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
        let suber = tracing_subscriber::fmt()
            .with_max_level(self.get_log_level())
            .with_ansi(false);

        if self.log_dir.len() == 0 && self.log_filename.len() == 0 {
            suber.init();
            return Ok(());
        }

        let appender: RollingFileAppender;
        match self.log_rolling.as_str() {
            "hourly" => {
                appender = hourly(self.log_dir.as_str(), self.log_filename.as_str());
            }

            "minutely" => {
                appender = minutely(self.log_dir.as_str(), self.log_filename.as_str());
            }

            "never" => {
                appender = never(self.log_dir.as_str(), self.log_filename.as_str());
            }

            _ => {
                appender = daily(self.log_dir.as_str(), self.log_filename.as_str());
            }
        }

        let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
        suber.with_writer(non_blocking).init();

        Ok(())
    }
}

pub struct Tsuixuq {
    opt: Guard<TsuixuqOption>,
    topics: HashMap<String, Guard<Topic>>,
}

unsafe impl Sync for Tsuixuq {}
unsafe impl Send for Tsuixuq {}

impl Tsuixuq {
    pub fn new(opt: Guard<TsuixuqOption>) -> Self {
        Tsuixuq {
            opt,
            topics: HashMap::new(),
        }
    }

    pub fn get_or_create_topic(&mut self, topic_name: &str) -> Result<Guard<Topic>> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.opt.get().topic_num_in_tsuixuq as _)
        {
            return Err(anyhow!("exceed upperlimit of topic"));
        }

        let topic = self
            .topics
            .entry(topic_name.to_string())
            .or_insert_with(|| Topic::new(topic_name).builder());

        Ok(topic.clone())
    }

    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        let topic_name = msg.get_topic();
        let topic = self.get_or_create_topic(topic_name)?;
        topic.get_mut().send_msg(msg).await?;
        Ok(())
    }

    pub fn get_topic_channel(
        &mut self,
        topic_name: &str,
        chan_name: &str,
    ) -> Result<Guard<Channel>> {
        let topic = self.get_or_create_topic(topic_name)?;
        Ok(topic.get_mut().get_mut_channel(chan_name)?)
    }

    pub async fn delete_client_from_channel(&mut self, chan_name: &str) {
        let mut iter = self.topics.iter_mut();
        while let Some((_addr, topic)) = iter.next() {
            topic.get_mut().delete_client_from_channel(chan_name).await;
        }
    }
}
