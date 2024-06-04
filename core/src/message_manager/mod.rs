pub mod topic_message;
use crate::{
    client::Client,
    message::{convert_to_resp, Message},
    protocol::*,
    storage::{
        storage::{new_storage_wrapper, StorageWrapper},
        STORAGE_TYPE_DUMMY,
    },
    topic::topic::new_topic,
    tsuixuq::TsuixuqOption,
};
use anyhow::{anyhow, Result};
use common::global::Guard;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use tokio::sync::mpsc::Sender;
pub use topic_message::TopicMessage;
use topic_message::{new_topic_message, topic_message_loop, topic_message_loop_defer};
use tracing::info;

pub struct MessageManager {
    opt: Guard<TsuixuqOption>,
    topics: HashMap<String, Guard<TopicMessage>>,
    /// 真实存储message的地方，可能是memory
    storage: Guard<StorageWrapper>,
}

impl MessageManager {
    pub async fn new(opt: Guard<TsuixuqOption>) -> Result<Self> {
        if opt.get().message_storage_type.as_str() == STORAGE_TYPE_DUMMY {
            return Ok(MessageManager {
                opt,
                topics: HashMap::new(),
                storage: new_storage_wrapper(STORAGE_TYPE_DUMMY, PathBuf::new(), 0, 0, 0, 0)
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

        Ok(MessageManager {
            opt,
            topics: HashMap::new(),
            storage: storage,
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
        let mut iter = self.topics.iter();
        while let Some((_, topic)) = iter.next() {
            let _ = topic
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
    ) -> Result<Guard<TopicMessage>> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.opt.get().topic_num_in_tsuixuq as _)
        {
            return Err(anyhow!("exceed upper limit of topic"));
        }

        if !self.topics.contains_key(topic_name) {
            let topic_storage = self
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
            Ok(topic_mm) => {
                // 有订阅某个topic的client时，才进行相应的loop消息循环
                tokio::spawn(topic_message_loop(topic_mm.clone()));
                tokio::spawn(topic_message_loop_defer(topic_mm.clone()));

                let chan = topic_mm
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

pub async fn new_message_manager(opt: Guard<TsuixuqOption>) -> Result<Guard<MessageManager>> {
    let mm = MessageManager::new(opt).await?;
    let guard = Guard::new(mm);
    Ok(guard)
}
