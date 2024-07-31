use std::ops::Deref;

use crate::{
    client::{Client, ClientWrapper},
    storage::TopicMeta,
};
use anyhow::Result;
use common::{global::Guard, Name, OrderedMap, Weight};
use protocol::{consts::*, message::Message};
use tokio::sync::mpsc::{self, Receiver, Sender};

#[derive(Clone)]
pub struct ChannelWrapper {
    inner: Guard<Channel>,
}

impl Weight for ChannelWrapper {
    fn get_weight(&self) -> usize {
        self.inner.get().get_weight()
    }
}

impl Deref for ChannelWrapper {
    type Target = Guard<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ChannelWrapper {
    pub fn new(guard: Guard<Channel>) -> Self {
        ChannelWrapper { inner: guard }
    }
}

pub struct Channel {
    topic_name: Name,
    name: Name,

    meta: TopicMeta,

    weight: usize,
    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    msg_sender: Sender<Message>,
    msg_recver: Receiver<Message>,

    pub clients: OrderedMap<String, ClientWrapper>,
}

unsafe impl Sync for Channel {}
unsafe impl Send for Channel {}

impl Channel {
    pub fn new(topic_name: &str, name: &str, meta: TopicMeta) -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Channel {
            topic_name: Name::new(topic_name),
            name: Name::new(name),
            meta,
            msg_sender: tx,
            msg_recver: rx,
            pub_num: 0,
            sub_num: 0,
            clients: OrderedMap::default(),
            weight: 0,
        }
    }

    pub fn builder(self) -> Guard<Self> {
        Guard::new(self)
    }

    pub fn get_weight(&self) -> usize {
        self.weight
    }

    pub fn set_client(&self, addr: &str, client_guard: Guard<Client>) {
        self.clients
            .insert(addr.to_string(), ClientWrapper::new(client_guard));
    }

    pub async fn send_msg(&self, msg: Message) -> Result<()> {
        let (_, sub_client) = split_subscribe_type(self.meta.subscribe_type);
        match sub_client {
            SUBSCRIBE_TYPE_BROADCAST_IN_CLIENT => {
                let iter = self.clients.values();
                for client in iter {
                    client.get().send_msg(msg.clone()).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_ROUNDROBIN_IN_CLIENT => {
                if let Some(client) = self.clients.roundrobin_next() {
                    client.get().send_msg(msg.clone()).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_RAND_IN_CLIENT => {
                if let Some(client) = self.clients.rand() {
                    client.get().send_msg(msg.clone()).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_RAND_PROPERTY_IN_CLIENT => {
                if let Some(client) = self.clients.rand_weight() {
                    client.get().send_msg(msg.clone()).await?;
                }
                Ok(())
            }

            _ => unreachable!(),
        }
    }

    pub async fn recv_msg(&mut self) -> Message {
        self.msg_recver.recv().await.unwrap()
    }

    pub fn pub_num_increase(&mut self) {
        // self.pub_num.increase();
    }

    pub fn delete_client(&self, client_addr: &str) {
        self.clients.remove(&client_addr.to_owned());
    }

    pub async fn is_client_empty(&self) -> bool {
        self.clients.is_empty()
    }
}
