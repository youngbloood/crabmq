pub mod topic_bus;
use crate::{
    client::Client,
    crab::CrabMQOption,
    storage::{new_storage_wrapper, StorageWrapper, STORAGE_TYPE_DUMMY},
    topic::new_topic,
};
use anyhow::{anyhow, Result};
use common::global::Guard;
use dashmap::DashMap;
use protocol::{
    message::{convert_to_resp, Message},
    protocol::*,
};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::Sender;
pub use topic_bus::TopicBus;
use topic_bus::{new_topic_message, topic_message_loop, topic_message_loop_defer};
use tracing::info;

pub struct MessageBus {
    opt: Guard<CrabMQOption>,

    /// topics map，当某个客户端订阅topic时，从storage中获取到topic接口，并初始化一个TopicBus，循环从TopicBus中读取消息
    topics: DashMap<String, Guard<TopicBus>>,
    /// 真实存储message的地方，可能是memory
    ///
    /// 写入message的入口
    storage: Guard<StorageWrapper>,
}

impl MessageBus {
    pub async fn new(opt: Guard<CrabMQOption>) -> Result<Self> {
        if opt.get().message_storage_type.as_str() == STORAGE_TYPE_DUMMY {
            return Ok(MessageBus {
                opt,
                topics: DashMap::new(),
                storage: new_storage_wrapper(STORAGE_TYPE_DUMMY, PathBuf::new(), 10, 100, 10, 300)
                    .await?,
            });
        }

        let storage = new_storage_wrapper(
            opt.get().message_storage_type.as_str(),
            Path::new(opt.get().message_dir.as_str()).join(""),
            opt.get().max_num_per_file,
            opt.get().max_size_per_file,
            opt.get().persist_message_factor,
            opt.get().persist_message_period,
        )
        .await?;

        Ok(MessageBus {
            opt,
            topics: DashMap::new(),
            storage,
        })
    }

    async fn push(
        &mut self,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) -> Result<()> {
        self.storage.get().push(out_sender, addr, msg).await?;
        Ok(())
    }

    pub async fn delete_client_from_channel(&mut self, client_addr: &str) {
        let iter = self.topics.iter();
        for topic in iter {
            let _ = topic
                .value()
                .get_mut()
                .topic
                .get_mut()
                .delete_client_from_channel(client_addr)
                .await;
        }
    }

    pub async fn get_or_create_topic(
        &mut self,
        topic_name: &str,
        ephemeral: bool,
    ) -> Result<Guard<TopicBus>> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.opt.get().max_topic_num as _)
        {
            return Err(anyhow!("exceed upper limit of topic"));
        }

        if !self.topics.contains_key(topic_name) {
            let (_, topic_storage) = self
                .storage
                .get_mut()
                .get_or_create_topic(topic_name, ephemeral)
                .await?;
            let topic = new_topic(self.opt.clone(), topic_name, ephemeral)?;

            let topic_message = new_topic_message(self.opt.clone(), topic, topic_storage).await?;
            self.topics
                .insert(topic_name.to_string(), topic_message.clone());
            return Ok(topic_message);
        }

        Ok(self.topics.get(topic_name).unwrap().clone())
    }

    pub async fn handle_message(
        &mut self,
        client: Guard<Client>,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) {
        match msg.action() {
            ACTION_FIN => self.fin(out_sender, addr, msg).await,
            ACTION_RDY => self.rdy(out_sender, addr, msg).await,
            ACTION_REQ => self.req(out_sender, addr, msg).await,
            ACTION_PUB => self.publish(out_sender, addr, msg).await,
            ACTION_NOP => self.nop(out_sender, addr, msg).await,
            ACTION_TOUCH => self.touch(out_sender, addr, msg).await,
            ACTION_SUB => self.sub(out_sender, addr, msg, client).await,
            ACTION_CLS => self.cls(out_sender, addr, msg).await,
            ACTION_AUTH => self.auth(out_sender, addr, msg).await,
            _ => unreachable!(),
        }
    }

    //============================ Handle Action ==============================//
    pub async fn fin(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn rdy(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn publish(
        &mut self,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) {
        let _ = self.push(out_sender, addr, msg).await;
    }

    pub async fn req(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn nop(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn touch(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn sub(
        &mut self,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
        guard: Guard<Client>,
    ) {
        let topic_name = msg.get_topic();
        let chan_name = msg.get_channel();

        match self
            .get_or_create_topic(topic_name, msg.topic_ephemeral())
            .await
        {
            Ok(topic_sub) => {
                // 有订阅某个topic的client时，才进行相应的loop消息循环
                tokio::spawn(topic_message_loop(topic_sub.clone()));
                tokio::spawn(topic_message_loop_defer(topic_sub.clone()));

                let chan = topic_sub
                    .get()
                    .topic
                    .get_mut()
                    .get_create_mut_channel(chan_name);
                chan.get_mut().set_client(addr, guard);
                info!(addr = addr, "sub topic: {topic_name}, channel: {chan_name}",);
                let _ = out_sender
                    .send((addr.to_string(), convert_to_resp(msg)))
                    .await;
            }
            Err(e) => todo!("resp the err to client"),
        }
    }

    pub async fn cls(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}

    pub async fn auth(&self, out_sender: Sender<(String, Message)>, addr: &str, msg: Message) {}
    //============================ Handle Action ==============================//
}

pub async fn new_message_manager(opt: Guard<CrabMQOption>) -> Result<Guard<MessageBus>> {
    let mm = MessageBus::new(opt).await?;
    let guard = Guard::new(mm);
    Ok(guard)
}
