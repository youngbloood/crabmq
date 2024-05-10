use crate::message::Message;
use crate::{channel::Channel, topic::Topic};
use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;
use config::{Config, File};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
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
    pub log_rolling: String,

    #[arg(short = 'p', long = "port", default_value_t = 3890)]
    pub tcp_port: u16, // tcp监听端口

    #[arg(long = "msg-num-buffer", default_value_t = DEFAULT_BUFFER as u16)]
    pub msg_num_buffer: u16, // 客户端发送的params缓存数量

    #[arg(long = "memory-msg-size", default_value_t = DEFAULT_BUFFER)]
    pub memory_message_size: u64, // 内存中存储message的大小

    #[arg(long = "memory-msg-count", default_value_t = DEFAULT_BUFFER)]
    pub memory_message_count: u64, // 内存中存储message的数量

    #[arg(long = "msg-flush-factor", default_value_t = DEFAULT_FACTOR)]
    pub message_flush_factor: u16, // messag flush至disk因子

    #[arg(long = "msg-flush-interval", default_value_t = DEFAULT_FACTOR as u64)]
    pub message_flush_interval: u64, // messaage flush至disk的时间间隔

    #[arg(long = "topic-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    pub topic_message_buffer: u64, // topic中message数量缓存数

    #[arg(long = "channel-msg-buffer", default_value_t = DEFAULT_BUFFER)]
    pub channel_message_buffer: u64, // channel中的message数量缓存数

    #[arg(long = "channel-num-in-topic", default_value_t = DEFAULT_BUFFER)]
    pub channel_num_in_topic: u64, // 一个topic下的channel数量

    #[arg(long = "topic-num-in-tsuixuq", default_value_t = DEFAULT_BUFFER)]
    pub topic_num_in_tsuixuq: u64, // 一个tsuixuq下的topic数量

    #[arg(long = "client-timeout", default_value_t = 30)]
    pub client_timeout: u16, // 一个client多长时间没收到包，单位(s)

    #[arg(long = "client-timeout-count", default_value_t = 3)]
    pub client_timeout_count: u16, // 一个client超时次数限制，超过该限制client会主动断开
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
    opt: Arc<TsuixuqOption>,
    topics: HashMap<String, Topic>,

    params: Receiver<Vec<Vec<u8>>>,
    params_sender: Sender<Vec<Vec<u8>>>,
}

unsafe impl Sync for Tsuixuq {}
unsafe impl Send for Tsuixuq {}

impl Tsuixuq {
    pub fn new(opt: Arc<TsuixuqOption>) -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Tsuixuq {
            opt,
            topics: HashMap::<String, Topic>::new(),
            params: rx,
            params_sender: tx,
        }
    }

    pub fn get_or_create_topic(&mut self, topic_name: &str) -> Result<&mut Topic> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.opt.topic_num_in_tsuixuq as usize)
        {
            return Err(anyhow!("exceed upperlimit of topic"));
        }

        let topic = self
            .topics
            .entry(topic_name.to_string())
            .or_insert_with(|| Topic::new(topic_name));

        Ok(topic)
    }

    pub async fn send_message(&mut self, msg: Message) -> Result<()> {
        let topic_name = msg.get_topic();
        let topic = self.get_or_create_topic(topic_name)?;
        topic.send_msg(msg).await?;
        Ok(())
    }

    pub fn get_topic_channel(&mut self, topic_name: &str, chan_name: &str) -> Result<Arc<Channel>> {
        let topic = self.get_or_create_topic(topic_name)?;
        Ok(topic.get_mut_channel(chan_name))
    }

    pub fn fin(&mut self) {}

    pub fn rdy(&mut self) {}

    pub fn publish(&mut self) {}
    // fn req(&mut self    fn publish(&mut self) {}

    pub fn mpub(&mut self) {}

    pub fn dpub(&mut self) {}

    pub fn nop(&mut self) {}

    pub fn touch(&mut self) {}

    pub fn sub(&mut self) {}

    pub fn cls(&mut self) {}

    pub fn auth(&mut self) {}
}
