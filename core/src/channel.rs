use crate::{client::Client, message::Message};
use anyhow::Result;
use common::{global::Guard, Name};
use parking_lot::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Channel {
    topic_name: Name,
    name: Name,

    ephemeral: bool,

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    msg_sender: Sender<Message>,
    msg_recver: Receiver<Message>,

    pub clients: RwLock<HashMap<String, Guard<Client>>>,
}

unsafe impl Sync for Channel {}
unsafe impl Send for Channel {}

impl Channel {
    pub fn new(topic_name: &str, name: &str) -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Channel {
            topic_name: Name::new(topic_name),
            name: Name::new(name),
            ephemeral: false,
            msg_sender: tx,
            msg_recver: rx,
            pub_num: 0,
            sub_num: 0,
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn builder(self) -> Guard<Self> {
        Guard::new(self)
    }

    pub fn set_client(&self, addr: &str, client_guard: Guard<Client>) {
        let mut rw = self.clients.write();
        rw.insert(addr.to_string(), client_guard);
    }

    pub async fn send_msg(&self, msg: Message) -> Result<()> {
        let rg = self.clients.read();
        let iter = rg.iter();
        for (_addr, client) in iter {
            client.get().send_msg(msg.clone()).await?;
        }
        Ok(())
    }

    pub async fn recv_msg(&mut self) -> Message {
        self.msg_recver.recv().await.unwrap()
    }

    pub fn pub_num_increase(&mut self) {
        // self.pub_num.increase();
    }

    pub fn delete_client(&self, client_addr: &str) {
        let mut wg = self.clients.write();
        wg.remove(client_addr);
    }

    pub async fn is_client_empty(&self) -> bool {
        let rd = self.clients.read();
        rd.is_empty()
    }
}
