use crate::client::Client;
use crate::message::Message;
use crate::message_sub::new_message_manager;
use crate::message_sub::MessageSub;
use anyhow::Result;
use clap::Parser;
use common::global::Guard;
use config::{Config, File};
use futures::executor::block_on;
use tokio::sync::mpsc::Sender;
use tracing::Level;
use tracing_appender::rolling::{daily, hourly, minutely, never};

const DEFAULT_BUFFER: u64 = 10000;
const DEFAULT_FACTOR: u16 = 100;
const DEFAULT_NUM: u64 = 10000;

#[derive(Parser, Debug, serde::Deserialize)]
pub struct TsuixuqOption {
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
    /// tcp监听端口
    pub tcp_port: u32,

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

    #[arg(long = "message-dir", default_value = "./message")]
    /// message默认存放的位置
    pub message_dir: String,

    /// max_num_per_file * 100 约等于 10G
    #[arg(long = "max-num-per-file", default_value_t = 107374180)]
    /// 每个文件最多存放消息数量
    pub max_num_per_file: u64,

    #[arg(long = "max-size-per-file", default_value_t = 10737418240)]
    /// 每个文件最多存放消息大小(Byte)，默认5G
    pub max_size_per_file: u64,

    #[arg(long = "persist-message-factor", default_value_t = 50)]
    /// 每persist_message_factor条消息触发一次消息持久化至磁盘
    pub persist_message_factor: u64,

    #[arg(long = "persist-message-period", default_value_t = 50)]
    /// 每persist_message_period时长触发一次消息持久化至磁盘，单位：ms
    pub persist_message_period: u64,

    #[arg(long = "message-storage-type", default_value = "local")]
    /// 每persist_message_period时长触发一次消息持久化至磁盘，单位：ms
    pub message_storage_type: String,

    #[arg(long = "message-cache-type", default_value = "memory")]
    /// 每persist_message_period时长触发一次消息持久化至磁盘，单位：ms
    pub message_cache_type: String,
}

impl TsuixuqOption {
    pub fn from_config(filename: &str) -> Result<TsuixuqOption> {
        let mut opt = TsuixuqOption::parse();
        if !filename.is_empty() {
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
        let suber = tracing_subscriber::fmt().with_max_level(self.get_log_level());
        // .with_ansi(false);

        if self.log_dir.is_empty() && self.log_filename.is_empty() {
            suber.init();
            return Ok(());
        }

        let appender = match self.log_rolling.as_str() {
            "hourly" => hourly(self.log_dir.as_str(), self.log_filename.as_str()),
            "minutely" => minutely(self.log_dir.as_str(), self.log_filename.as_str()),
            "never" => never(self.log_dir.as_str(), self.log_filename.as_str()),
            _ => daily(self.log_dir.as_str(), self.log_filename.as_str()),
        };

        let (non_blocking, _guard) = tracing_appender::non_blocking(appender);
        suber.with_writer(non_blocking).init();

        Ok(())
    }
}

pub struct Tsuixuq {
    opt: Guard<TsuixuqOption>,
    mb: Guard<MessageSub>,
    pub out_sender: Option<Sender<(String, Message)>>,
}

unsafe impl Sync for Tsuixuq {}
unsafe impl Send for Tsuixuq {}

impl Tsuixuq {
    pub fn new(opt: Guard<TsuixuqOption>) -> Result<Self> {
        let tsuixuq = Tsuixuq {
            mb: block_on(new_message_manager(opt.clone()))?,
            out_sender: None,
            opt,
        };

        // let message_dir = tsuixuq.opt.get().message_dir.as_str();
        // if !check_exist(message_dir) {
        //     return Ok(tsuixuq);
        // }
        // debug!("Tsuixuq: message_dir = {message_dir}");
        // for entry in fs::read_dir(tsuixuq.opt.get().message_dir.as_str())? {
        //     let entry = entry?;
        //     if !entry.file_type()?.is_dir() {
        //         continue;
        //     }
        //     let topic_name = entry.file_name();
        //     debug!("Tsuixuq: load topic: {}", topic_name.to_str().unwrap());
        //     tsuixuq.get_or_create_topic(topic_name.to_str().unwrap(), false)?;
        // }

        Ok(tsuixuq)
    }

    // pub fn get_or_create_topic(
    //     &mut self,
    //     topic_name: &str,
    //     ephemeral: bool,
    // ) -> Result<Guard<Topic>> {
    //     let topics_len = self.topics.len();
    //     if !self.topics.contains_key(topic_name)
    //         && topics_len >= (self.opt.get().topic_num_in_tsuixuq as _)
    //     {
    //         return Err(anyhow!("exceed upper limit of topic"));
    //     }

    //     if !self.topics.contains_key(topic_name) {
    //         let topic = new_topic(self.opt.clone(), self.mm.clone(), topic_name, ephemeral)?;
    //         self.topics.insert(topic_name.to_string(), topic.clone());
    //         return Ok(topic.clone());
    //     }

    //     Ok(self.topics.get(topic_name).unwrap().clone())
    // }

    pub async fn handle_message(
        &self,
        client: Guard<Client>,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) {
        self.mb
            .get_mut()
            .handle_message(client, out_sender, addr, msg)
            .await;
    }

    // pub fn get_topic_channel(
    //     &mut self,
    //     topic_name: &str,
    //     chan_name: &str,
    // ) -> Result<Guard<Channel>> {
    //     let topic = self.get_or_create_topic(topic_name)?;
    //     Ok(topic.get_mut().get_mut_channel(chan_name)?)
    // }

    pub async fn delete_client_from_channel(&mut self, client_addr: &str) {
        self.mb
            .get_mut()
            .delete_client_from_channel(client_addr)
            .await;
    }
}
