use crate::{client::ClientGuard, message::Message};
use anyhow::Result;
use common::Name;
use std::collections::HashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Channel {
    name: Name,

    ephemeral: bool,

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    msg_sender: Sender<Message>,
    msg_recver: Receiver<Message>,

    clients: HashMap<String, ClientGuard>,
}

unsafe impl Sync for Channel {}
unsafe impl Send for Channel {}

impl Channel {
    pub fn new(name: &str) -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Channel {
            name: Name::new(name),
            ephemeral: false,
            msg_sender: tx,
            msg_recver: rx,
            pub_num: 0,
            sub_num: 0,
            clients: HashMap::new(),
        }
    }

    pub fn set_client(&mut self, addr: String, client_guard: ClientGuard) {
        self.clients.insert(addr, client_guard);
    }

    pub async fn send_msg(&self, msg: Message) -> Result<()> {
        let mut iter = self.clients.iter();
        while let Some((_addr, client)) = iter.next() {
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
}
