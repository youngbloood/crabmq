use crate::proto::{Message, SubscribeReq, SubscribeReq_Heartbeat};
use tokio::sync::mpsc;

struct SmartConsumer {
    broker_stream: tonic::Streaming<Message>,
    request_sender: mpsc::Sender<SubscribeReq>,
    config: FlowConfig,
    current_credit: u32,
    pending_acks: HashMap<String, Message>,
    last_credit_consumed: u32,
}

impl SmartConsumer {
    async fn start(&mut self) {
        // 初始化订阅
        let _ = self
            .request_sender
            .send(SubscribeReq {
                request: Some(SubscribeReq::Subscription(self.config.clone())),
            })
            .await;

        tokio::spawn(self.receive_loop());
        tokio::spawn(self.control_loop());
    }

    async fn receive_loop(&mut self) {
        while let Some(msg) = self.broker_stream.next().await {
            if let Ok(msg) = msg {
                self.current_credit = msg.credit_remaining;
                self.last_credit_consumed = msg.credit_consumed;
                self.pending_acks.insert(msg.message_id.clone(), msg);
                self.process_message(msg).await;
            }
        }
    }

    async fn control_loop(&mut self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            interval.tick().await;

            // 请求信用
            let credit_needed = self.calculate_credit_need();
            if credit_needed > 0 {
                let _ = self
                    .request_sender
                    .send(SubscribeReq {
                        request: Some(SubscribeReq::Heartbeat(SubscribeReq_Heartbeat {
                            credit_request: credit_needed,
                        })),
                    })
                    .await;
            }

            // 发送确认
            self.send_acks().await;
        }
    }

    fn calculate_credit_need(&self) -> u32 {
        let base_need = self.config.max_bytes / 2;
        let mut factor = 1.0;

        // 基于使用率的调整
        if self.last_credit_consumed > 0 {
            let usage_rate = self.last_credit_consumed as f32 / self.config.max_bytes as f32;
            if usage_rate > 0.8 {
                factor *= 1.5;
            } else if usage_rate < 0.2 {
                factor *= 0.7;
            }
        }

        // 基于积压的调整
        if self.pending_acks.len() > (self.config.max_messages as usize * 7 / 10) {
            factor *= 0.6;
        }

        (base_need as f32 * factor) as u32
    }

    async fn send_acks(&mut self) {
        for (id, msg) in self.pending_acks.drain() {
            let return_credit = (msg.credit_consumed as f32 * 0.9) as u32;

            let _ = self
                .request_sender
                .send(SubscribeReq {
                    request: Some(SubscribeReq::Ack(SubscribeReq_Ack {
                        message_id: id,
                        commit: true,
                        credit_return: return_credit,
                    })),
                })
                .await;
        }
    }
}
