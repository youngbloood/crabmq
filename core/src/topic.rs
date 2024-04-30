use crate::{channel::Channel, message::Message};
use anyhow::Result;
use common::util::AtomicU64;
use common::{ArcMuxRefCell, ArcRefCell, Name};
use std::{collections::HashMap, sync::Arc};

pub struct Topic {
    message_count: AtomicU64, // 消息的数量
    message_bytes: AtomicU64, // 消息的大小

    pub_num: AtomicU64, // publisher的数量
    sub_num: AtomicU64, // subscriber的数量
    name: Name,
    ephemeral: bool,

    channels: HashMap<String, Arc<Channel>>,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        let n = Name::new();
        Topic {
            name: n,
            ephemeral: false,
            channels: HashMap::new(),
            message_count: AtomicU64::new(),
            message_bytes: AtomicU64::new(),
            pub_num: AtomicU64::new(),
            sub_num: AtomicU64::new(),
        }
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<()> {
        let mut iter = self.channels.iter_mut();
        while iter.next().is_some() {
            let channel = iter.next().unwrap().1;
            channel.send_msg(msg.clone()).await;
        }

        Ok(())
    }

    pub fn get_mut_channel(&mut self, chan_name: &str) -> Arc<Channel> {
        let chan = self.channels.get_mut(chan_name).unwrap().clone();
        chan
    }

    pub fn can_drop_ephemeral(&self) -> bool {
        if !self.ephemeral {
            return false;
        }
        if !self.message_count.eq(0)
            || !self.message_bytes.eq(0)
            || !self.pub_num.eq(0)
            || !self.sub_num.eq(0)
        {
            return false;
        }

        true
    }
}
