use crate::message::{Message, MessageUnitHeap};
use crate::{channel::Channel, message::MessageUnit};
use anyhow::{anyhow, Result};
use common::global::Guard;
use common::Name;
use parking_lot::RwLock;
use std::collections::{BinaryHeap, HashMap};
use tokio::sync::mpsc::Sender;

pub struct Topic {
    name: Name,
    message_count: u64, // 消息的数量
    message_bytes: u64, // 消息的大小

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    persist_filename: String,
    defer_heap: MessageUnitHeap,

    ephemeral: bool,

    channels: HashMap<String, Guard<Channel>>,
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
            persist_filename: "".to_string(),
            defer_heap: MessageUnitHeap::new(""),
        };

        // 每个topic默认有个default的channel
        topic.channels.insert(
            "default".to_string(),
            Guard::new(Channel::new(topic.name.as_str(), "default")),
        );

        topic
    }

    pub fn builder(self: Self) -> Guard<Self> {
        Guard::new(self)
    }

    pub async fn send_msg(
        &mut self,
        sender: Sender<(String, Message)>,
        addr: &str,
        mut msg: Message,
    ) -> Result<()> {
        let mut iter = self.channels.iter();
        while let Some((_chan_name, chan)) = iter.next() {
            match &mut msg {
                Message::Null => unreachable!(),
                Message::V1(ref mut v1) => {
                    let mut bodys_iter = v1.bodys.list.iter_mut();
                    while let Some(body) = bodys_iter.next() {
                        // TODO: 处理各种body
                    }
                }
            }
            let sender_clone = sender.clone();
            chan.get_mut().send_msg(sender_clone, msg.clone()).await?;
        }

        let _ = msg.reset_body();
        let _ = sender.send((addr.to_string(), msg)).await;
        Ok(())
    }

    pub fn get_mut_channel(&mut self, chan_name: &str) -> Result<Guard<Channel>> {
        let topic = self.name.as_str();
        match self.channels.get(chan_name) {
            Some(chan) => Ok(chan.clone()),
            None => Err(anyhow!("not found channel in topic[{topic}]")),
        }
    }

    pub fn get_create_mut_channel(&mut self, chan_name: &str) -> Guard<Channel> {
        let chan = self
            .channels
            .entry(chan_name.to_owned())
            .or_insert_with(|| Channel::new(self.name.as_str(), chan_name).builder());

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
            chan.get_mut().delete_channel(chan_name)
        }
    }
}
