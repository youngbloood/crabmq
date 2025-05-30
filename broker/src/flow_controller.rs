use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use crate::proto::{Partition, SubscribeReq, Message};
use crate::storage::MessageStorage;

struct TokenBucket {
    capacity: u32,
    tokens: u32,
    fill_rate: u32, // tokens per second
    last_fill: std::time::Instant,
}

impl TokenBucket {
    fn new(capacity: u32, fill_rate: u32) -> Self {
        Self {
            capacity,
            tokens: capacity,
            fill_rate,
            last_fill: std::time::Instant::now(),
        }
    }
    
    fn refill(&mut self) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_fill).as_secs_f32();
        let new_tokens = (elapsed * self.fill_rate as f32) as u32;
        
        if new_tokens > 0 {
            self.tokens = std::cmp::min(self.tokens + new_tokens, self.capacity);
            self.last_fill = now;
        }
    }
    
    fn consume(&mut self, tokens: u32) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }
}

struct ClientCredit {
    max_bytes: u32,
    max_messages: u32,
    remaining_bytes: u32,
    remaining_messages: u32,
    last_updated: std::time::Instant,
    paused: bool,
}

struct FlowController {
    client_credits: Mutex<HashMap<String, Arc<Mutex<ClientCredit>>>>,
    partition_buckets: Mutex<HashMap<Partition, Arc<Mutex<TokenBucket>>>>,
    storage: Arc<dyn MessageStorage>,
}

impl FlowController {
    async fn handle_subscribe_request(
        &self,
        mut request_stream: tonic::Streaming<SubscribeReq>,
        response_stream: tonic::Response<tonic::codec::Streaming<Message>>,
    ) -> Result<(), tonic::Status> {
        let (mut tx, rx) = mpsc::channel(100);
        let response_stream = Stream::new(rx);
        
        tokio::spawn(async move {
            let mut current_credit = None;
            let mut subscription = None;
            
            while let Some(req) = request_stream.next().await {
                match req.request {
                    Some(SubscribeReq::Subscription(sub)) => {
                        subscription = Some(sub.clone());
                        let credit = self.init_client_credit(&sub).await;
                        current_credit = Some(credit);
                    }
                    Some(SubscribeReq::Ack(ack)) => {
                        if let Some(credit) = ¤t_credit {
                            self.return_credit(credit, ack.credit_return).await;
                        }
                    }
                    Some(SubscribeReq::FlowControl(flow)) => {
                        if let Some(credit) = ¤t_credit {
                            self.adjust_flow(credit, flow).await;
                        }
                    }
                    None => continue,
                }
                
                // 发送消息
                if let (Some(credit), Some(sub)) = (¤t_credit, &subscription) {
                    if let Some(messages) = self.fetch_messages(sub, credit).await {
                        for msg in messages {
                            let consumed = self.consume_credit(credit, msg.payload.len() as u32).await;
                            
                            // 添加流量控制元数据
                            let mut msg = msg;
                            msg.credit_consumed = consumed;
                            msg.credit_remaining = credit.lock().await.remaining_bytes;
                            
                            if let Err(_) = tx.send(msg).await {
                                break; // 客户端断开连接
                            }
                        }
                    }
                }
            }
        });
        
        Ok(Response::new(Box::pin(response_stream)))
    }
    
    async fn fetch_messages(
        &self,
        sub: &Subscription,
        credit: &Arc<Mutex<ClientCredit>>,
    ) -> Option<Vec<Message>> {
        let credit_guard = credit.lock().await;
        
        // 计算可用配额
        let max_bytes = std::cmp::min(
            credit_guard.remaining_bytes,
            sub.flow_config.as_ref().map(|c| c.max_bytes).unwrap_or(10_485_760) // 10MB
        );
        
        let max_messages = std::cmp::min(
            credit_guard.remaining_messages,
            sub.flow_config.as_ref().map(|c| c.max_messages).unwrap_or(500)
        );
        
        // 应用分区级流控
        let partition = Partition {
            topic: sub.topic.clone(),
            partition_id: sub.partition,
        };
        
        if let Some(bucket) = self.partition_buckets.lock().await.get(&partition) {
            let mut bucket_guard = bucket.lock().await;
            if !bucket_guard.consume(max_bytes) {
                return None; // 分区令牌不足
            }
        }
        
        // 从存储获取消息
        self.storage.fetch(
            &sub.topic,
            sub.partition,
            max_bytes,
            max_messages,
            sub.offset,
        ).await
    }
    
    async fn consume_credit(
        &self,
        credit: &Arc<Mutex<ClientCredit>>,
        size: u32,
    ) -> u32 {
        let mut credit_guard = credit.lock().await;
        let actual = std::cmp::min(size, credit_guard.remaining_bytes);
        
        credit_guard.remaining_bytes -= actual;
        credit_guard.remaining_messages -= 1;
        
        // 自动补充逻辑
        if credit_guard.remaining_bytes < credit_guard.max_bytes / 10 {
            credit_guard.remaining_bytes += credit_guard.max_bytes / 4;
        }
        
        actual
    }
}