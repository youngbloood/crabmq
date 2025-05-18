use crate::channel::{Channel, ChannelWrapper};
use crate::config::Config;
use anyhow::{anyhow, Result};
use common::global::Guard;
use common::{Name, OrderedMap};
use protocol::consts::*;
use storage::TopicMeta;

use protocol::message::{Message, MessageOperation};
use tokio_util::sync::CancellationToken;

pub struct Topic {
    opt: Guard<Config>,

    pub name: Name,
    message_count: u64, // 消息的数量
    message_bytes: u64, // 消息的大小

    pub_num: u64, // publisher的数量
    sub_num: u64, // subscriber的数量

    meta: TopicMeta,

    channels: OrderedMap<String, ChannelWrapper>,

    cancel: CancellationToken,
}

impl Topic {
    pub fn new(opt: Guard<Config>, name: &str, meta: TopicMeta) -> Result<Self> {
        let n = Name::new(name);
        let topic = Topic {
            opt,
            name: n,
            meta: meta.clone(),
            channels: OrderedMap::default(),
            message_count: 0,
            message_bytes: 0,
            pub_num: 0,
            sub_num: 0,
            cancel: CancellationToken::new(),
        };

        // 每个topic默认有个default的channel
        topic.channels.insert(
            "default".to_string(),
            ChannelWrapper::new(Guard::new(Channel::new(
                topic.name.as_str(),
                "default",
                meta,
            ))),
        );

        Ok(topic)
    }

    pub fn builder(self) -> Guard<Self> {
        Guard::new(self)
    }

    /// 将消息发下至consumers
    pub async fn deliver_message(&self, msg: Message) -> Result<()> {
        let prot = msg.convert_to_protocol();
        let (sub_channel, _) = split_subscribe_type(self.meta.subscribe_type);
        match sub_channel {
            SUBSCRIBE_TYPE_BROADCAST_IN_CHANNEL => {
                let iter = self.channels.values();
                for chan in iter {
                    chan.get().send_msg(prot.clone()).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_ROUNDROBIN_IN_CHANNEL => {
                if let Some(chan) = self.channels.roundrobin_next() {
                    chan.get().send_msg(prot).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_RAND_IN_CHANNEL => {
                if let Some(chan) = self.channels.rand() {
                    chan.get().send_msg(prot).await?;
                }
                Ok(())
            }

            SUBSCRIBE_TYPE_RAND_PROPERTY_IN_CHANNEL => {
                if let Some(chan) = self.channels.rand_weight() {
                    chan.get().send_msg(prot).await?;
                }
                Ok(())
            }

            _ => unreachable!(),
        }
    }

    // pub async fn send_msg(
    //     &mut self,
    //     out_sender: Sender<(String, Message)>,
    //     addr: &str,
    //     msg: Message,
    // ) -> Result<()> {
    //     let mut msg_resp = msg.clone();
    //     msg_resp.set_resp()?;
    //     out_sender.send((addr.to_string(), msg_resp)).await?;

    //     let msg_list: Vec<Message> = msg.split();
    //     let mut iter = msg_list.iter();
    //     while let Some(msg) = iter.next() {
    //         // TODO: 处理各种body
    //         self.queue.get_mut().push(msg.clone()).await?;
    //         // if body.is_ack() {}
    //         // if body.is_persist() {}
    //     }

    //     Ok(())
    // }

    pub fn get_mut_channel(&mut self, chan_name: &str) -> Result<ChannelWrapper> {
        let topic = self.name.as_str();
        match self.channels.get(&chan_name.to_string()) {
            Some(chan) => Ok(chan.clone()),
            None => Err(anyhow!("not found channel in topic[{topic}]")),
        }
    }

    pub fn get_create_mut_channel(&mut self, chan_name: &str) -> ChannelWrapper {
        let chan = self.channels.get_or_create(
            chan_name.to_owned(),
            ChannelWrapper::new(
                Channel::new(self.name.as_str(), chan_name, self.meta.clone()).builder(),
            ),
        );
        // .entry(chan_name.to_owned())
        // .or_insert_with(|| {
        //     Channel::new(self.name.as_str(), chan_name, self.meta.clone()).builder()
        // });

        chan.clone()
    }

    // pub fn can_drop_ephemeral(&self) -> bool {
    //     if !self.ephemeral {
    //         return false;
    //     }
    //     // if !self.message_count.eq(0)
    //     //     || !self.message_bytes.eq(0)
    //     //     || !self.pub_num.eq(0)
    //     //     || !self.sub_num.eq(0)
    //     // {
    //     //     return false;
    //     // }

    //     true
    // }

    pub async fn delete_client_from_channel(&mut self, chan_name: &str) {
        let iter = self.channels.values();

        for chan in iter {
            chan.get_mut().delete_client(chan_name)
        }
    }

    pub async fn is_client_empty(&self) -> bool {
        for chan in self.channels.values() {
            if !chan.get().is_client_empty().await {
                return false;
            }
        }
        true
    }
}

pub fn new_topic(opt: Guard<Config>, name: &str, meta: TopicMeta) -> Result<Guard<Topic>> {
    let guard = Guard::new(Topic::new(opt, name, meta)?);
    Ok(guard)
}

//
// pub async fn topic_has_consumers(topic: Guard<Topic>) {
//     let (notify_tx, notify_rx) = oneshot::channel();
//     tokio::spawn(async move {
//         let mut ticker = interval(Duration::from_secs(1));
//         loop {
//             select! {
//                 _ = CANCEL_TOKEN.cancelled() => {
//                     return;
//                 }

//                 _ = ticker.tick() => {
//                     for chan in topic.get().channels.values() {
//                         let rg = chan.get().clients.read();
//                         if rg.len() != 0 {
//                             let _ = notify_tx.send(());
//                             return;
//                         }
//                     }
//                 }
//             }
//         }
//     });
//     let _ = notify_rx.await;
// }

// async fn topic_loop(guard: Guard<Topic>) {
//     loop {
//         select! {
//             _ = CANCEL_TOKEN.cancelled() => {
//                 return;
//             }

//             _ = guard.get().cancel.cancelled() => {
//                 return ;
//             }

//             msg_opt = guard.get().queue.get_mut().pop() => {
//                 if msg_opt.is_none(){
//                     continue;
//                 }
//                 // 等待topic下的channel有client接受时，才进行消息的发送
//                 topic_has_consumers(guard.clone()).await;

//                 let msg = msg_opt.unwrap();
//                 guard.get().channels.iter().for_each(|(_, chan)| {
//                     let chan_guard = chan.clone();
//                     let msg_clone = msg.clone();
//                     tokio::spawn(async move{
//                             let _ = chan_guard.get().send_msg( msg_clone).await;
//                         });
//                 });
//             }
//         }
//     }
// }
