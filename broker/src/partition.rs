use dashmap::DashMap;
use futures::StreamExt;
// use futures_util::stream::stream::StreamExt;
use grpcx::{
    brokercoosvc::{self, broker_coo_service_client::BrokerCooServiceClient},
    commonsvc::{TopicList, topic_list},
    smart_client::SmartClient,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct PartitionManager {
    broker_id: u32,
    // 当前节点负责的分区 (topic, partition) -> bool
    my_partitions: Arc<DashMap<(String, u32), bool>>,
    // 协调器客户端
    client: Option<SmartClient>,
}

impl PartitionManager {
    pub fn new(broker_id: u32, coo_addr: String) -> Self {
        if coo_addr.is_empty() {
            PartitionManager {
                broker_id,
                my_partitions: Arc::default(),
                client: None,
            }
        } else {
            let pm = PartitionManager {
                broker_id,
                my_partitions: Arc::default(),
                client: Some(SmartClient::new(vec![coo_addr])),
            };
            let pm_clone = pm.clone();
            tokio::spawn(async move {
                pm_clone.refresh_partition().await;
            });
            pm
        }
    }

    pub fn is_my_partition(&self, topic: &str, partition: u32) -> bool {
        self.my_partitions
            .contains_key(&(topic.to_string(), partition))
    }

    pub fn apply_topic_infos(&self, tl: TopicList) {
        match tl.list.unwrap() {
            topic_list::List::Init(topic_list_init) => {
                for info in topic_list_init.topics {
                    for part in info.partition {
                        self.my_partitions
                            .insert((info.topic.clone(), part.id), true);
                    }
                }
            }
            topic_list::List::Add(topic_list_add) => {
                for info in topic_list_add.topics {
                    for part in info.partition {
                        self.my_partitions
                            .insert((info.topic.clone(), part.id), true);
                    }
                }
            }
        }
    }

    async fn refresh_partition(&self) {
        let req = brokercoosvc::PullReq { id: self.broker_id };

        loop {
            if let Some(client) = self.client.as_ref() {
                match client
                    .open_sstream(req, |chan, request| async move {
                        BrokerCooServiceClient::new(chan).pull(request).await
                    })
                    .await
                {
                    Ok(mut strm) => {
                        while let Some(Ok(tl)) = strm.next().await {
                            self.apply_topic_infos(tl);
                        }
                    }
                    Err(_) => todo!(),
                }
            }
        }
    }
}
