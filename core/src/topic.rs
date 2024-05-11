use crate::channel::Channel;
use crate::message::Message;
use anyhow::{anyhow, Result};
use common::ArcMux;
use common::Name;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

pub struct Topic {
    name: Name,
    message_count: u64, // 消息的数量
    message_bytes: u64, // 消息的大小

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    ephemeral: bool,

    channels: HashMap<String, ArcMux<Channel>>,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        let n = Name::new(name);
        let mut topic = Topic {
            name: n,
            ephemeral: false,
            channels: HashMap::new(),
            message_count: 0,
            message_bytes: 0,
            pub_num: 0,
            sub_num: 0,
        };

        // 每个topic默认有个default的channel
        topic.channels.insert(
            "default".to_string(),
            Arc::new(Mutex::new(Channel::new("default"))),
        );

        topic
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<()> {
        let mut iter = self.channels.iter();
        while let Some((_chan_name, chan)) = iter.next() {
            chan.lock().await.send_msg(msg.clone()).await?;
        }
        Ok(())
    }

    pub fn get_mut_channel(&mut self, chan_name: &str) -> Result<ArcMux<Channel>> {
        let topic = self.name.as_str();
        match self.channels.get(chan_name) {
            Some(chan) => Ok(chan.clone()),
            None => Err(anyhow!("not found channel in topic[{topic}]")),
        }
    }

    pub fn get_create_mut_channel(&mut self, chan_name: &str) -> ArcMux<Channel> {
        let chan = self
            .channels
            .entry(chan_name.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(Channel::new(chan_name))));

        chan.clone()
    }

    pub fn can_drop_ephemeral(&self) -> bool {
        if !self.ephemeral {
            return false;
        }
        // if !self.message_count.eq(0)
        //     || !self.message_bytes.eq(0)
        //     || !self.pub_num.eq(0)
        //     || !self.sub_num.eq(0)
        // {
        //     return false;
        // }

        true
    }

    pub async fn delete_client_from_channel(&mut self, chan_name: &str) {
        let mut iter = self.channels.iter_mut();

        while let Some((_addr, chan)) = iter.next() {
            chan.lock().await.delete_channel(chan_name)
        }
    }
}
