use crate::{channel::Channel, message::Message, topic::Topic};
use anyhow::anyhow;
use anyhow::Result;
use common::ArcCell;
use common::ArcMuxRefCell;
use common::ArcRefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};

const DEFAULT_BUFFER: u64 = 10000;
const DEFAULT_FACTOR: u16 = 100;
const DEFAULT_NUM: u64 = 10000;

#[derive(Clone, Copy)]
pub struct TsuixuqOption {
    pub tcp_port: u16,               // tcp监听端口
    pub params_num_buffer: u16,      // 客户端发送的params缓存数量
    pub memory_message_size: u64,    // 内存中存储message的大小
    pub memory_message_count: u64,   // 内存中存储message的数量
    pub message_flush_factor: u16,   // messag flush至disk因子
    pub message_flush_interval: u64, // messaage flush至disk的时间间隔
    pub topic_message_buffer: u64,   // topic中message数量缓存数
    pub channel_message_buffer: u64, // channel中的message数量缓存数
    pub channel_num_in_topic: u64,   // 一个topic下的channel数量
    pub topic_num_in_tsuixuq: u64,   // 一个tsuixuq下的topic数量
}

impl TsuixuqOption {
    pub fn new() -> Self {
        TsuixuqOption {
            params_num_buffer: DEFAULT_BUFFER as u16,
            tcp_port: 3890,
            memory_message_size: DEFAULT_BUFFER,
            memory_message_count: DEFAULT_BUFFER,
            message_flush_factor: DEFAULT_FACTOR,
            message_flush_interval: DEFAULT_BUFFER,
            topic_message_buffer: DEFAULT_BUFFER,
            channel_message_buffer: DEFAULT_BUFFER,
            channel_num_in_topic: DEFAULT_NUM,
            topic_num_in_tsuixuq: DEFAULT_NUM,
        }
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
}
