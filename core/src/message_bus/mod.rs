pub mod topic_bus;
use crate::{
    client::Client,
    config::Config,
    storage::{new_storage_wrapper, StorageWrapper, STORAGE_TYPE_DUMMY},
    topic::new_topic,
};
use anyhow::{anyhow, Result};
use common::global::Guard;
use dashmap::DashMap;
use protocol::{
    consts::*,
    error::ProtError,
    protocol::{v1::reply::ReplyBuilder, Builder, Protocol, ProtocolOperation},
};
use tokio::sync::mpsc::Sender;
pub use topic_bus::TopicBus;
use topic_bus::{new_topic_message, topic_message_loop, topic_message_loop_defer};
use tracing::{debug, error, info};

pub struct MessageBus {
    cfg: Guard<Config>,

    /// topics map，当某个客户端订阅topic时，从storage中获取到topic接口，并初始化一个TopicBus，循环从TopicBus中读取消息
    topics: DashMap<String, Guard<TopicBus>>,
    /// 真实存储message的地方，可能是memory
    ///
    /// 写入message的入口
    storage: Guard<StorageWrapper>,
}

impl MessageBus {
    pub async fn new(opt: Guard<Config>) -> Result<Self> {
        if opt.get().global.message_storage_type.as_str() == STORAGE_TYPE_DUMMY {
            return Ok(MessageBus {
                topics: DashMap::new(),
                storage: new_storage_wrapper(
                    opt.get().global.message_storage_type.as_str(),
                    opt.get().message_storage_disk_config.clone(),
                )
                .await?,
                cfg: opt,
            });
        }

        let storage = new_storage_wrapper(
            opt.get().global.message_storage_type.as_str(),
            opt.get().message_storage_disk_config.clone(),
        )
        .await?;

        Ok(MessageBus {
            cfg: opt,
            topics: DashMap::new(),
            storage,
        })
    }

    async fn push(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        prot: Protocol,
    ) -> Result<()> {
        // let msg = prot.convert_to_message()?;
        match prot.clone() {
            Protocol::V1(v1) => {
                let ephemeral = v1.get_publish().unwrap().is_ephemeral();
                self.storage
                    .get()
                    .push(out_sender, addr, prot, ephemeral)
                    .await?;
                Ok(())
            }
        }
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
        epehemral: bool,
    ) -> Result<Guard<TopicBus>> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.cfg.get().global.max_topic_num as _)
        {
            return Err(anyhow!("exceed upper limit of topic"));
        }

        if !self.topics.contains_key(topic_name) {
            let (_, topic_storage) = self
                .storage
                .get_mut()
                .get_or_create_topic(
                    topic_name,
                    epehemral,
                    &self.cfg.get().message_storage_disk_config.gen_topic_meta(),
                )
                .await?;
            let topic = new_topic(self.cfg.clone(), topic_name, topic_storage.get_meta()?)?;

            let topic_message = new_topic_message(self.cfg.clone(), topic, topic_storage).await?;
            self.topics
                .insert(topic_name.to_string(), topic_message.clone());
            return Ok(topic_message);
        }

        Ok(self.topics.get(topic_name).unwrap().clone())
    }

    pub async fn handle_message(
        &mut self,
        client: Guard<Client>,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        prot: Protocol,
    ) {
        match prot.get_action() {
            ACTION_FIN => self.fin(out_sender, addr, prot).await,
            ACTION_PUBLISH => self.publish(out_sender, addr, prot).await,
            ACTION_TOUCH => self.touch(out_sender, addr, prot).await,
            ACTION_SUBSCRIBE => self.subscribe(out_sender, addr, prot, client).await,
            ACTION_CLOSE => self.cls(out_sender, addr, prot).await,
            ACTION_AUTH => self.auth(out_sender, addr, prot).await,
            _ => unreachable!(),
        }
    }

    //============================ Handle Action ==============================//
    pub async fn fin(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}

    pub async fn rdy(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}

    pub async fn publish(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        prot: Protocol,
    ) {
        match self.push(out_sender.clone(), addr, prot.clone()).await {
            Ok(_) => {
                debug!("send msg successful");
                // 可能client已经关闭，忽略该err
                let _ = out_sender
                    .send((addr.to_string(), prot.build_reply_ok().build()))
                    .await;
            }

            Err(e) => {
                let err: ProtError = e.into();
                error!("send msg failed: {err:?}");
                // TODO:
                // let mut resp = convert_to_resp(msg);
                // resp.set_reject_code(err.code);
                let _ = out_sender
                    .send((addr.to_string(), prot.build_reply_err(err.code).build()))
                    .await;
            }
        }
    }

    pub async fn req(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}

    pub async fn nop(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}

    pub async fn touch(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        msg: Protocol,
    ) {
        // TODO:
        // let topic_name = msg.get_topic();
        // match self.get_or_create_topic(topic_name, &msg.get_head()).await {
        //     Ok(_) => todo!(),
        //     Err(_) => todo!(),
        // }
    }

    pub async fn subscribe(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        addr: &str,
        prot: Protocol,
        guard: Guard<Client>,
    ) {
        match prot {
            Protocol::V1(v1) => {
                let sub = v1.get_subscribe().unwrap();
                let topic_name = sub.get_topic();
                let chan_name = sub.get_channel();

                match self
                    .get_or_create_topic(topic_name, sub.is_ephemeral())
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
                        // TODO:
                        // let _ = out_sender
                        //     .send((addr.to_string(), convert_to_resp(msg)))
                        //     .await;
                    }
                    Err(e) => todo!("resp the err to client"),
                }
            }
        }
    }

    pub async fn cls(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}

    pub async fn auth(&self, out_sender: Sender<(String, Protocol)>, addr: &str, msg: Protocol) {}
    //============================ Handle Action ==============================//
}

pub async fn new_message_manager(opt: Guard<Config>) -> Result<Guard<MessageBus>> {
    let mm = MessageBus::new(opt).await?;
    let guard = Guard::new(mm);
    Ok(guard)
}
