use crate::{
    Config as CooConfig, broker::Broker, client::Client, consumer_group::ConsumerGroupManager,
    event_bus::EventBus, partition::PartitionManager,
};
use anyhow::Result;
use dashmap::DashMap;
use protocol::{aggregation::Event, pbv1};
use std::sync::Arc;
use tokio::sync::mpsc;
use transporter::TransportMessage;

#[derive(Clone)]
pub struct Coordinator {
    pub conf: CooConfig,

    // 向 raft_node 节点发送消息的通道
    raft_node_sender: mpsc::Sender<TransportMessage>,

    // 连接 coo 的 brokers
    // broker_id -> Broker
    brokers: Arc<DashMap<u32, Broker>>,

    // 连接 coo 的 clients
    // client_addr -> Client
    clients: Arc<DashMap<String, Client>>,

    // (Leadder)集群的 Topic-Partition 变更事件总线
    broker_event_bus: EventBus<Event>,
    client_event_bus: EventBus<Event>,

    //（Leader）集群节点变更事件总线
    // peer_change_bus: EventBus<Result<CooListResp, Status>>,

    // 分区管理
    partition_manager: Arc<PartitionManager>,

    // 消费者组管理器
    consumer_group_manager: ConsumerGroupManager,
}

impl Coordinator {
    pub fn new(
        conf: CooConfig,
        raft_node_sender: mpsc::Sender<TransportMessage>,
        partition_manager: PartitionManager,
    ) -> Result<Self> {
        conf.validate()?;

        let brokers = Arc::new(DashMap::new());
        let consumer_group_manager =
            ConsumerGroupManager::new(partition_manager.all_topics.clone());

        Ok(Self {
            raft_node_sender,
            brokers: brokers.clone(),
            clients: Arc::new(DashMap::new()),
            broker_event_bus: EventBus::new(conf.coo.event_bus_buffer_size),
            client_event_bus: EventBus::new(conf.coo.event_bus_buffer_size),
            // peer_change_bus: EventBus::new(conf.coo.event_bus_buffer_size),
            partition_manager: Arc::new(partition_manager),
            consumer_group_manager,
            conf,
        })
    }
}

// handle all message
impl Coordinator {
    pub async fn handle_raft_message(&self, msg: TransportMessage) -> Result<()> {
        match msg.version {
            1 => self.handle_raft_message_v1(msg).await,
            _ => anyhow::bail!("unsupported message version: {}", msg.version),
        }
    }

    async fn handle_raft_message_v1(&self, msg: TransportMessage) -> Result<()> {
        match msg.index {
            protocol::v1::BROKER_COO_HEARTBEAT_REQUEST_INDEX => self.handle_v1_heartbeat(msg).await,
            _ => todo!(),
        }
    }

    async fn handle_v1_heartbeat(&self, msg: TransportMessage) -> Result<()> {
        // 处理 Broker 心跳消息
        // 1. 解析消息内容，获取 Broker 信息
        // 2. 更新 brokers 中对应的 Broker 状态和信息
        // 3. 触发 broker_event_bus 发布 Broker 状态变更事件
        Ok(())
    }
}
// impl Coordinator {
//     /// qeury_topics: 获取所有的 topic
//     ///
//     /// 根据条件过滤：
//     ///
//     /// 1. 根据 `topic` 过滤
//     /// 2. 根据 `broker_id` 过滤
//     /// 3. 根据 `partition` 过滤
//     async fn qeury_topics_v1(
//         &self,
//         topics: &[String],
//         broker_ids: &[u32],
//         partition_ids: &[u32],
//         keys: &[String],
//     ) -> Vec<protocol::pbv1::Topics> {
//         let topic_assignment =
//             self.partition_manager
//                 .query_partitions(topics, broker_ids, partition_ids, keys);

//         let mut tpr = commonsvc::TopicPartitionResp {
//             term: self.get_term().await,
//             list: vec![],
//         };
//         for (topic, ta) in topic_assignment {
//             ta.iter()
//                 .for_each(|v| tpr.list.push(v.convert_to_topic_partition_meta(&topic)));
//         }

//         tpr
//     }

//     async fn propose(&self, partitions: SinglePartition, callback: Option<Callback>) -> Result<()> {
//         let unique_id = nanoid::nanoid!();
//         if let Some(cb) = callback.clone() {
//             self.raft_node.register_callback(unique_id.clone(), cb);
//         }

//         let propose_data = ProposeData::TopicPartition(TopicPartitionData {
//             topic: partitions,
//             callback,
//         });

//         let data = bincode::encode_to_vec(propose_data, config::standard())?;

//         let msg = CooRaftProposeMessage {
//             index: CooRaftProposeType::ProposeTypePartition as i32,
//             unique_id,
//             message: Bytes::from(data),
//         };

//         let transport_message = TransportMessage {
//             index: protocol::COO_RAFT_PROPOSE_MESSAGE_INDEX,
//             remote_addr: "".to_string(),
//             message: Arc::new(Box::new(msg)),
//         };

//         self.raft_node_sender.send(transport_message).await?;
//         Ok(())
//     }

//     async fn get_term(&self) -> u64 {
//         self.raft_node.get_term().await
//     }

//     async fn list_peer(
//         &self,
//         remote_addr: SocketAddr,
//         id: String,
//         is_broker: bool,
//     ) -> mpsc::Receiver<Result<CooListResp, Status>> {
//         let peer = self.raft_node.get_peer();
//         let mut list = vec![];
//         let leader_id = self.raft_node.get_leader_id().await;
//         peer.iter().for_each(|v| {
//             let id = v.id as u32;
//             list.push(commonsvc::CooInfo {
//                 act: commonsvc::CooInfoAction::Add.into(),
//                 id,
//                 coo_addr: repair_addr_with_http(v.metadata.clone()),
//                 raft_addr: repair_addr_with_http(v.raft_addr.clone()),
//                 role: if id == leader_id {
//                     commonsvc::CooRole::Leader.into()
//                 } else {
//                     commonsvc::CooRole::Follower.into()
//                 },
//             });
//         });

//         let sub_id = if is_broker {
//             format!("broker_{}_{}", id, remote_addr)
//         } else {
//             format!("client_{}_{}", id, remote_addr)
//         };
//         let (tx, rx) = self.peer_change_bus.subscribe(sub_id);

//         let raft_node = self.raft_node.clone();
//         tokio::spawn(async move {
//             let _ = tx
//                 .send(Ok(CooListResp {
//                     cluster_term: raft_node.get_term().await,
//                     list,
//                 }))
//                 .await;
//         });

//         rx
//     }
// }

// #[derive(Debug, Clone)]
// pub enum PartitionEvent {
//     NotLeader {
//         new_leader_id: u32,
//         new_coo_leader_addr: String,
//         new_raft_leader_addr: String,
//     },
//     NewTopic {
//         partitions: SinglePartition,
//     },
//     AddPartitions {
//         added: SinglePartition,
//     },
// }

// // 转换函数示例
// fn convert_to_pull_resp(ps: SinglePartition, term: u64) -> commonsvc::TopicPartitionResp {
//     let mut tp = commonsvc::TopicPartitionResp::from(ps);
//     tp.term = term;
//     tp
// }

// fn filter_single_partition(
//     sp: SinglePartition,
//     topics: &[String],
//     broker_ids: &[u32],
//     partition_ids: &[u32],
//     keys: &[String],
// ) -> Option<SinglePartition> {
//     // 1. 检查 Topic 是否匹配
//     if !topics.is_empty() && !topics.contains(&sp.topic) {
//         return None;
//     }

//     // 2. 创建新的 TopicAssignment 用于存储过滤结果
//     let mut filtered_assignment = vec![];

//     // 3. 过滤分区，broker, keys
//     for entry in sp.partitions.iter() {
//         // 检查分区 ID 是否匹配
//         let match_partition = partition_ids.is_empty() || partition_ids.contains(&entry.id);

//         // 检查 Broker 是否匹配（主副本或从副本）
//         let match_broker = broker_ids.is_empty() || broker_ids.contains(&entry.broker_leader_id);
//         // TODO： 下面的根据 broker_follower_ids 先隐藏
//         // || entry
//         //     .broker_follower_ids
//         //     .iter()
//         //     .any(|f| broker_ids.contains(f));

//         // 检查 Key 是否匹配
//         let match_key = keys.is_empty() || entry.pub_keys.iter().any(|k| keys.contains(k));

//         if match_partition && match_broker && match_key {
//             filtered_assignment.push(entry.clone());
//         }
//     }

//     // 5. 如果没有任何分区匹配，则返回 None
//     if filtered_assignment.is_empty() {
//         return None;
//     }

//     // 6. 返回过滤后的 SinglePartition
//     Some(SinglePartition {
//         unique_id: sp.unique_id,
//         topic: sp.topic,
//         partitions: filtered_assignment,
//     })
// }

// impl From<SinglePartition> for commonsvc::TopicPartitionResp {
//     fn from(sp: SinglePartition) -> Self {
//         let mut tp = commonsvc::TopicPartitionResp::default();
//         tp.list = sp
//             .partitions
//             .iter()
//             .map(|v| v.convert_to_topic_partition_meta(&sp.topic))
//             .collect();

//         tp
//     }
// }

// #[cfg(test)]
// mod test {
//     use std::path::Path;

//     use tokio::sync::mpsc;

//     use crate::coo::Coordinator;
//     use crate::default_config;
//     #[tokio::test]
//     async fn qeury_topics() {
//         let id = 1;
//         let mut conf = default_config();
//         let db_path = conf.db_path.clone();
//         conf = conf
//             .with_id(id)
//             .with_db_path(Path::new("..").join(&db_path).join(format!("coo{}", id)));
//         let (tx, rx) = mpsc::channel(1);
//         let coo = Coordinator::new("".to_string(), conf, tx);

//         let ts = coo
//             .qeury_topics(&["mytopic1".to_string()], &[], &[], &[])
//             .await;
//         println!("ts = {:?}", ts);
//     }
// }
