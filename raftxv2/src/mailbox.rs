use crate::peer::PeerState;
use log::{debug, error, trace, warn};
use std::{num::NonZero, sync::Arc, time::Duration};
use tokio::{select, sync::mpsc};
use transporter::{TransportMessage, TransporterWriter};

pub(crate) struct Mailbox {
    src_id: u32,
    dst_id: u32,
    send_timeout: NonZero<u64>, // milli
    w: TransporterWriter,
    rx_msg: mpsc::Receiver<TransportMessage>,
    status: Arc<PeerState>, // rx_stop: mpsc::Receiver<()>,
}
impl Mailbox {
    pub(crate) fn new(
        src_id: u32,
        dst_id: u32,
        send_timeout: NonZero<u64>,
        w: TransporterWriter,
        rx_msg: mpsc::Receiver<TransportMessage>,
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

    pub(crate) async fn start_serve(mut self) {
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
                    trace!("Node[{}] -> Node[{}]: timeout: {}s",self.src_id,self.dst_id,self.send_timeout);
                    match self.w.send_timeout(&msg,Duration::from_millis(self.send_timeout.get())).await {
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

// pub enum MessageType {
//     RaftPropose(ProposeData),
//     RaftConfChange(ConfChange),
//     RaftConfChangeV2(ConfChangeV2),
//     TransportMessage(Message),
// }

// #[derive(Serialize, Deserialize)]
// pub struct TopicPartitionData {
//     pub topic: SinglePartition,
//     #[serde(skip)]
//     pub callback: Option<mpsc::Sender<Result<String>>>,
// }

// pub enum ProposeData {
//     // 分区布署详情
//     TopicPartition(TopicPartitionData),
//     // 消费者组信息详情
//     ConsumerGroupDetail(),
// }
