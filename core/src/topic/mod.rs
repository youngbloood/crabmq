pub mod message_manager;

use crate::channel::Channel;
use crate::message::Message;
use crate::tsuixuq::TsuixuqOption;
use anyhow::{anyhow, Result};
use common::global::{Guard, CANCEL_TOKEN};
use common::Name;
use futures::executor::block_on;
use std::time::Duration;
use std::{collections::HashMap, path::Path};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use self::message_manager::disk::build_disk_queue;
use self::message_manager::dummy::build_dummy_queue;
use self::message_manager::MessageManager;

pub struct Topic {
    opt: Guard<TsuixuqOption>,

    name: Name,
    message_count: u64, // 消息的数量
    message_bytes: u64, // 消息的大小

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    queue: MessageManager,

    ephemeral: bool,

    channels: HashMap<String, Guard<Channel>>,

    cancel: CancellationToken,
}

// impl Drop for Topic<T>
// where
//     T: MessageQueue,
// {
//     fn drop(&mut self) {
//         self.queue.stop();
//         self.cancel.cancel();
//     }
// }

impl Topic {
    pub fn new(opt: Guard<TsuixuqOption>, name: &str, ephemeral: bool) -> Result<Self> {
        let mut queue: MessageManager;
        if ephemeral {
            queue = build_dummy_queue(10);
        } else {
            queue = block_on(build_disk_queue(
                Path::new(opt.get().message_dir.as_str()).join(name),
                opt.get().max_num_per_file,
                opt.get().max_size_per_file,
                opt.get().persist_message_factor,
                opt.get().persist_message_period,
            ))?;
        }

        let n = Name::new(name);
        let mut topic = Topic {
            opt,
            name: n,
            ephemeral,
            channels: HashMap::new(),
            message_count: 0,
            message_bytes: 0,
            pub_num: 0,
            sub_num: 0,
            queue: queue,
            cancel: CancellationToken::new(),
        };

        // 每个topic默认有个default的channel
        topic.channels.insert(
            "default".to_string(),
            Guard::new(Channel::new(topic.name.as_str(), "default")),
        );

        Ok(topic)
    }

    pub fn builder(self: Self) -> Guard<Self> {
        Guard::new(self)
    }

    pub async fn send_msg(
        &mut self,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        mut msg: Message,
    ) -> Result<()> {
        let mut msg_resp = msg.clone();
        msg_resp.set_resp()?;
        out_sender.send((addr.to_string(), msg_resp)).await?;

        let msg_list: Vec<Message> = msg.split();
        let mut iter = msg_list.iter();
        while let Some(msg) = iter.next() {
            // TODO: 处理各种body
            self.queue.push(msg.clone()).await?;
            // if body.is_ack() {}
            // if body.is_persist() {}
        }

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

pub fn new_topic(opt: Guard<TsuixuqOption>, name: &str, ephemeral: bool) -> Result<Guard<Topic>> {
    let guard = Guard::new(Topic::new(opt, name, ephemeral)?);
    let guard_loop = guard.clone();
    tokio::spawn(topic_loop(guard_loop));
    Ok(guard)
}

/// [`topic_has_consumers`] means there is consumers connected and in channel, then will return.
///
/// Otherwise this function will be block until any consumer connected.
pub async fn topic_has_consumers(topic: Guard<Topic>) {
    let (notify_tx, notify_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }

                _ = ticker.tick() => {
                    for (_, chan) in &topic.get().channels {
                        let rg = chan.get().clients.read();
                        if rg.len() != 0 {
                            let _ = notify_tx.send(());
                            return;
                        }
                    }
                }
            }
        }
    });
    let _ = notify_rx.await;
}

async fn topic_loop(guard: Guard<Topic>) {
    loop {
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return;
            }

            _ = guard.get().cancel.cancelled() => {
                return ;
            }

            msg_opt = guard.get().queue.pop() => {
                if msg_opt.is_none(){
                    continue;
                }
                // 等待topic下的channel有client接受时，才进行消息的发送
                topic_has_consumers(guard.clone()).await;

                let msg = msg_opt.unwrap();
                guard.get().channels.iter().for_each(|(_, chan)| {
                    let chan_guard = chan.clone();
                    let msg_clone = msg.clone();
                    tokio::spawn(async move{
                            let _ = chan_guard.get().send_msg( msg_clone).await;
                        });
                });
            }
        }
    }
}
