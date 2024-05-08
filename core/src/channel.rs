use std::sync::Arc;

use crate::message::Message;
use anyhow::Result;
use common::util::AtomicU64;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct Channel {
    ephemeral: bool,

    pub_num: AtomicU64, // publisher的数量
    sub_num: AtomicU64, // subscriber的数量

    msg_sender: Sender<Message>,
    msg_recver: Receiver<Message>,
}

unsafe impl Sync for Channel {}
unsafe impl Send for Channel {}

// unsafe impl Sync for RefCell<Channel> {}
// unsafe impl Send for RefCell<Channel> {}

impl Channel {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Channel {
            ephemeral: false,
            msg_sender: tx,
            msg_recver: rx,
            pub_num: AtomicU64::new(),
            sub_num: AtomicU64::new(),
        }
    }

    pub async fn send_msg(&self, msg: Message) -> Result<()> {
        self.msg_sender.send(msg).await?;
        Ok(())
    }

    pub async fn recv_msg(&mut self) -> Message {
        self.msg_recver.recv().await.unwrap()
    }

    pub fn pub_num_increase(&mut self) {
        self.pub_num.increase();
    }
}
