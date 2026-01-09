use std::{num::NonZero, sync::Arc};

use crate::{partition::SinglePartition, peer::PeerState};
use anyhow::Result;
use log::warn;
use raft::{
    eraftpb::ConfChange,
    prelude::{ConfChangeV2, Message},
};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::mpsc};
use transporter::TransporterWriter;

pub(crate) struct Mailbox {
    src_id: u32,
    dst_id: u32,
    send_timeout: NonZero<u64>,
    w: TransporterWriter,
    rx_msg: mpsc::Receiver<RaftMessage>,
    status: Arc<PeerState>, // rx_stop: mpsc::Receiver<()>,
}
impl Mailbox {
    pub(crate) fn new(
        src_id: u32,
        dst_id: u32,
        send_timeout: NonZero<u64>,
        w: TransporterWriter,
        rx_msg: mpsc::Receiver<RaftMessage>,
        status: Arc<PeerState>, // rx_stop: mpsc::Receiver<()>,
    ) -> Self {
        Self {
            src_id,
            dst_id,
            send_timeout,
            w,
            rx_msg,
            status,
        }
    }

    pub(crate) async fn start_serve(&mut self) {
        let mut failed_times = 0;
        loop {
            if self.rx_msg.is_closed() {
                warn!("Mailbox[{}->{}] has been closed", self.src_id, self.dst_id);
                break;
            }
            select! {
                msg = self.rx_msg.recv() => {
                    if msg.is_none() {
                        continue;
                    }
                    let msg = msg.unwrap();
                    let mut req = Request::new(msg);
                    trace!("Node[{}] -> Node[{}]: timeout: {}s",self.src_id,self.dst_id,self.send_timeout);
                    req.set_timeout(Duration::from_secs(self.send_timeout.get()));
                    match self.w.send(req).await {
                        Ok(_) => {
                            self.status.rotate_upgrade().await;
                            debug!("Node[{}]->Node[{}] Mailbox sent message successfully", self.src_id, self.dst_id);
                        },
                        Err(e) => {
                            self.status.rotate_downgrade().await;
                            if failed_times % 50 == 0 {
                                error!("Node[{}]->Node[{}] Mailbox send error: {:?}", self.src_id, self.dst_id, e);
                            }
                            failed_times += 1;
                        },
                    }
                }
            }
        }
    }
}

pub enum MessageType {
    RaftPropose(ProposeData),
    RaftConfChange(ConfChange),
    RaftConfChangeV2(ConfChangeV2),
    RaftMessage(Message),
}

#[derive(Serialize, Deserialize)]
pub struct TopicPartitionData {
    pub topic: SinglePartition,
    #[serde(skip)]
    pub callback: Option<mpsc::Sender<Result<String>>>,
}

pub enum ProposeData {
    // 分区布署详情
    TopicPartition(TopicPartitionData),
    // 消费者组信息详情
    ConsumerGroupDetail(),
}
