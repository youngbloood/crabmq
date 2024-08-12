pub mod topic_bus;

use crate::{
    client::Client,
    config::Config,
    storage::{
        covert_protocol_to_topicmeta, new_storage_wrapper, StorageWrapper, STORAGE_TYPE_DUMMY,
    },
    topic::new_topic,
};
use anyhow::{anyhow, Result};
use common::{global::Guard, random_num, random_str};
use dashmap::DashMap;
use protocol::{
    consts::*,
    error::*,
    protocol::{
        v1::{auth::AuthType, identity::IdentityReply, reply::ReplyBuilder},
        Builder, Protocol, ProtocolOperation,
    },
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
        prot: &Protocol,
    ) -> Result<Guard<TopicBus>> {
        let topics_len = self.topics.len();
        if !self.topics.contains_key(topic_name)
            && topics_len >= (self.cfg.get().global.max_topic_num as _)
        {
            return Err(ProtError::new(E_TOPIC_EXCESS_UPPER_LIMIT).into());
        }

        if !self.topics.contains_key(topic_name) {
            let (_, topic_storage) = self
                .storage
                .get_mut()
                .get_or_create_topic(topic_name, epehemral, &covert_protocol_to_topicmeta(prot))
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
            ACTION_TOUCH | ACTION_PUBLISH | ACTION_SUBSCRIBE | ACTION_PATCH => {
                if client.get().is_need_identity() {
                    let c = prot.build_reply_err(E_NEED_IDENTITY).build();
                    let _ = out_sender.send((addr.to_string(), c)).await;
                    return;
                }
                if client.get().is_need_auth() {
                    let c = prot.build_reply_err(E_NEED_AUTH).build();
                    let _ = out_sender.send((addr.to_string(), c)).await;
                    return;
                }
            }

            ACTION_AUTH => {
                if client.get().is_need_identity() {
                    let c = prot.build_reply_err(E_NEED_IDENTITY).build();
                    let _ = out_sender.send((addr.to_string(), c)).await;
                    return;
                }
            }

            _ => {}
        };

        match prot.get_action() {
            ACTION_IDENTITY => self.identity(out_sender, client, addr, prot).await,
            ACTION_AUTH => self.auth(out_sender, client, addr, prot).await,
            ACTION_TOUCH => self.touch(out_sender, client, addr, prot).await,
            ACTION_PUBLISH => self.publish(out_sender, client, addr, prot).await,
            ACTION_SUBSCRIBE => self.subscribe(out_sender, client, addr, prot).await,
            ACTION_PATCH => self.patch(out_sender, client, addr, prot).await,
            _ => unreachable!(),
        }
    }

    //============================ Handle Action ==============================//
    pub async fn identity(
        &self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
        addr: &str,
        _prot: Protocol,
    ) {
        let mut reply = IdentityReply::default();
        // TODO: auth type determine by Config
        let auth_type = AuthType::default();
        reply.set_max_protocol_version(PROPTOCOL_V1);
        // 需要auth，则返回salt
        if !auth_type.is_no_need_auth() {
            let salt = random_str(random_num(100, 200) as usize);
            let _ = reply.set_salt(&salt);
        } else {
            client.get().set_has_auth();
        }
        reply.set_authtype(auth_type);

        client.get().set_has_identity();
        debug!("reply-identity={:?}", reply.clone().build());
        let _ = out_sender.send((addr.to_string(), reply.build())).await;
    }

    pub async fn auth(
        &self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
        addr: &str,
        msg: Protocol,
    ) {
        client.get().set_has_auth();
    }

    pub async fn touch(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
        addr: &str,
        prot: Protocol,
    ) {
        match prot.clone() {
            Protocol::V1(v1) => {
                if let Some(touch) = v1.get_touch() {
                    let topic_name = touch.topic();
                    let _ = self
                        .get_or_create_topic(topic_name, touch.topic_is_ephemeral(), &prot)
                        .await;
                }
            }
        };

        let _ = out_sender
            .send((addr.to_string(), prot.build_reply_ok().build()))
            .await;
    }

    pub async fn publish(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
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
                let _ = out_sender
                    .send((addr.to_string(), prot.build_reply_err(err.code).build()))
                    .await;
            }
        }
    }

    pub async fn subscribe(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
        addr: &str,
        prot: Protocol,
    ) {
        match prot.clone() {
            Protocol::V1(v1) => {
                let sub = v1.get_subscribe().unwrap();
                let topic_name = sub.get_topic();
                let chan_name = sub.get_channel();

                match self
                    .get_or_create_topic(topic_name, sub.is_ephemeral(), &prot)
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
                        chan.get_mut().set_client(addr, client);
                        info!(addr = addr, "sub topic: {topic_name}, channel: {chan_name}",);

                        let _ = out_sender
                            .send((addr.to_string(), prot.build_reply_ok().build()))
                            .await;
                    }
                    Err(e) => {
                        let err: ProtError = e.into();

                        let _ = out_sender
                            .send((addr.to_string(), prot.build_reply_err(err.code).build()))
                            .await;
                    }
                }
            }
        }
    }

    pub async fn patch(
        &mut self,
        out_sender: Sender<(String, Protocol)>,
        client: Guard<Client>,
        addr: &str,
        prot: Protocol,
    ) {
    }
    //============================ Handle Action ==============================//
}

pub async fn new_message_manager(opt: Guard<Config>) -> Result<Guard<MessageBus>> {
    let mm = MessageBus::new(opt).await?;
    let guard = Guard::new(mm);
    Ok(guard)
}
