use crate::client::Client;
use crate::config::Config;
use crate::message_bus::new_message_manager;
use crate::message_bus::MessageBus;
use anyhow::Result;
use common::global::Guard;
use futures::executor::block_on;
use protocol::message::Message;
use tokio::sync::mpsc::Sender;

const DEFAULT_BUFFER: u64 = 10000;
const DEFAULT_FACTOR: u16 = 100;
const DEFAULT_NUM: u64 = 10000;

pub struct Crab {
    opt: Guard<Config>,
    mb: Guard<MessageBus>,
    pub out_sender: Option<Sender<(String, Message)>>,
}

unsafe impl Sync for Crab {}
unsafe impl Send for Crab {}

impl Crab {
    pub fn new(opt: Guard<Config>) -> Result<Self> {
        let crab = Crab {
            mb: block_on(new_message_manager(opt.clone()))?,
            out_sender: None,
            opt,
        };

        // let message_dir = tsuixuq.opt.get().message_dir.as_str();
        // if !check_exist(message_dir) {
        //     return Ok(tsuixuq);
        // }
        // debug!("Tsuixuq: message_dir = {message_dir}");
        // for entry in fs::read_dir(tsuixuq.opt.get().message_dir.as_str())? {
        //     let entry = entry?;
        //     if !entry.file_type()?.is_dir() {
        //         continue;
        //     }
        //     let topic_name = entry.file_name();
        //     debug!("Tsuixuq: load topic: {}", topic_name.to_str().unwrap());
        //     tsuixuq.get_or_create_topic(topic_name.to_str().unwrap(), false)?;
        // }

        Ok(crab)
    }

    // pub fn get_or_create_topic(
    //     &mut self,
    //     topic_name: &str,
    //     ephemeral: bool,
    // ) -> Result<Guard<Topic>> {
    //     let topics_len = self.topics.len();
    //     if !self.topics.contains_key(topic_name)
    //         && topics_len >= (self.opt.get().topic_num_in_tsuixuq as _)
    //     {
    //         return Err(anyhow!("exceed upper limit of topic"));
    //     }

    //     if !self.topics.contains_key(topic_name) {
    //         let topic = new_topic(self.opt.clone(), self.mm.clone(), topic_name, ephemeral)?;
    //         self.topics.insert(topic_name.to_string(), topic.clone());
    //         return Ok(topic.clone());
    //     }

    //     Ok(self.topics.get(topic_name).unwrap().clone())
    // }

    pub async fn handle_message(
        &self,
        client: Guard<Client>,
        out_sender: Sender<(String, Message)>,
        addr: &str,
        msg: Message,
    ) {
        self.mb
            .get_mut()
            .handle_message(client, out_sender, addr, msg)
            .await;
    }

    // pub fn get_topic_channel(
    //     &mut self,
    //     topic_name: &str,
    //     chan_name: &str,
    // ) -> Result<Guard<Channel>> {
    //     let topic = self.get_or_create_topic(topic_name)?;
    //     Ok(topic.get_mut().get_mut_channel(chan_name)?)
    // }

    pub async fn delete_client_from_channel(&mut self, client_addr: &str) {
        self.mb
            .get_mut()
            .delete_client_from_channel(client_addr)
            .await;
    }
}
