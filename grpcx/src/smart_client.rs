use std::{
    clone,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::Stream;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Code, IntoStreamingRequest, Request, Response, Status, Streaming,
    transport::{Channel, Endpoint},
};

// 核心客户端结构
#[derive(Clone)]
pub struct SmartClient {
    endpoints: Arc<Vec<String>>,
    current_leader: Arc<Mutex<String>>,
    channels: Arc<Mutex<Vec<(Channel, String)>>>,
    retry_policy: RetryPolicy,
}

#[derive(Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff_base: Duration,
    pub backoff_factor: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_base: Duration::from_millis(50),
            backoff_factor: 2,
        }
    }
}

impl SmartClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        let initial_leader = endpoints
            .first()
            .expect("Not allow empty endpoints")
            .clone();

        Self {
            endpoints: Arc::new(endpoints),
            current_leader: Arc::new(Mutex::new(initial_leader)),
            channels: Arc::new(Mutex::new(Vec::new())),
            retry_policy: RetryPolicy::default(),
        }
    }

    pub async fn execute_unary<Req, Res, F, Fut>(
        &self,
        request: Req,
        call: F,
    ) -> Result<Response<Res>, Status>
    where
        Req: Send + Clone,
        F: Fn(Channel, Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Res>, Status>>,
    {
        let req = request.clone();
        self.retry_operation(|channel| call(channel, Request::new(req.clone())))
            .await
    }

    /// 开启客户端流（客户端发送单个请求 ，服务端响应流）
    pub async fn open_sstream<Req, Res, C, Fut>(
        &self,
        request: Req,
        call: C,
    ) -> Result<SmartStream<Res>, Status>
    where
        Req: Send + Clone,
        C: Fn(Channel, Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Streaming<Res>>, Status>>,
    {
        let req = request.clone();
        let stream = self
            .retry_operation(|channel| call(channel, Request::new(req.clone())))
            .await?;
        Ok(SmartStream::new(stream.into_inner(), self.clone()))
    }

    /// 开启客户端流（客户端发送流，服务端返回单个响应）
    pub async fn open_cstream<Req, Res, F, Fut, S>(
        &self,
        stream_factory: impl Fn() -> S + Send + Sync + 'static,
        call: F,
    ) -> Result<Response<Res>, Status>
    where
        S: Stream<Item = Req> + Send + 'static,
        Req: Send + Clone + 'static,
        F: Fn(Channel, SmartReqStream<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Res>, Status>>,
    {
        self.retry_operation(|channel| {
            let stream = stream_factory();
            call(channel, SmartReqStream::new(Box::pin(stream)))
        })
        .await
    }

    /// 开启双向流（客户端和服务端双向流）
    pub async fn open_bistream<Req, Res, C, Fut, F: Fn() -> ReceiverStream<Req>>(
        &self,
        stream_factory: F,
        call: C,
    ) -> Result<SmartStream<Res>, Status>
    where
        // S: ReceiverStream<Req>,
        Req: Send + Clone + 'static,
        C: Fn(Channel, SmartReqStream<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Streaming<Res>>, Status>>,
    {
        let strm = self
            .retry_operation(|channel| {
                let stream = stream_factory();
                call(channel, SmartReqStream::new(Box::pin(stream)))
            })
            .await?;

        Ok(SmartStream::new(strm.into_inner(), self.clone()))
    }

    async fn retry_operation<T, F, Fut>(&self, mut operation: F) -> Result<T, Status>
    where
        F: FnMut(Channel) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let mut retries = 0;
        let mut backoff = self.retry_policy.backoff_base;

        loop {
            let channel = self.get_channel().await?;
            match operation(channel).await {
                Ok(result) => return Ok(result),
                Err(status) => {
                    if retries >= self.retry_policy.max_retries {
                        return Err(status.clone());
                    }

                    self.handle_error(&status).await;
                    retries += 1;
                    tokio::time::sleep(backoff).await;
                    backoff *= self.retry_policy.backoff_factor;
                }
            }
        }
    }

    async fn get_channel(&self) -> Result<Channel, Status> {
        let leader = self.current_leader.lock().await.clone();
        if let Some((channel, _)) = self.find_channel(&leader).await {
            return Ok(channel);
        }

        // 新建连接
        let endpoint = Endpoint::from_shared(leader.clone())
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::new(Code::Unavailable, e.to_string()))?;

        self.channels.lock().await.push((channel.clone(), leader));
        Ok(channel)
    }

    async fn find_channel(&self, address: &str) -> Option<(Channel, String)> {
        self.channels
            .lock()
            .await
            .iter()
            .find(|(c, addr)| addr == address)
            .cloned()
    }

    async fn handle_error(&self, status: &Status) {
        if let Some(new_leader) = extract_leader_address(status) {
            let mut current_leader = self.current_leader.lock().await;
            *current_leader = new_leader.clone();
            self.cleanup_channels().await;
        }
    }

    async fn cleanup_channels(&self) {
        let mut channels = self.channels.lock().await;
        let current_leader = self.current_leader.lock().await;
        channels.retain(|(c, addr)| addr.eq(&*current_leader));
    }
}

// 智能流包装器
pub struct SmartStream<T> {
    inner: Streaming<T>,
    client: SmartClient,
}

impl<T> SmartStream<T> {
    fn new(inner: Streaming<T>, client: SmartClient) -> Self {
        Self { inner, client }
    }
}

impl<T> Stream for SmartStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(status))) => {
                if let Some(new_leader) = extract_leader_address(&status) {
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let mut current_leader = client.current_leader.lock().await;
                        *current_leader = new_leader;
                        client.cleanup_channels().await;
                    });
                    return Poll::Ready(Some(Err(Status::new(
                        Code::Unavailable,
                        "Leader changed, please retry",
                    ))));
                }
                Poll::Ready(Some(Err(status)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

// 工具函数
fn extract_leader_address(status: &Status) -> Option<String> {
    status
        .metadata()
        .get("x-raft-leader")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

// 智能流包装器

/// 流包装器改进
pub struct SmartReqStream<T>
where
    T: Send + Clone + 'static,
{
    inner: Pin<Box<dyn Stream<Item = T> + Send>>,
}

impl<T> SmartReqStream<T>
where
    T: Send + Clone + 'static,
{
    pub fn new(stream: impl Stream<Item = T> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> IntoStreamingRequest for SmartReqStream<T>
where
    T: Send + Clone + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = Self::Message> + Send>>;
    type Message = T;

    fn into_streaming_request(self) -> Request<Self::Stream> {
        Request::new(self.inner)
    }
}

//// 实例
//// Unary调用（键值存储服务）
// mod kv_store {
//     tonic::include_proto!("kv_store");
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let client = SmartClient::new(vec![
//         "http://node1:50051".to_string(),
//         "http://node2:50051".to_string(),
//     ]).await?;

//     // 设置值
//     let set_response = client
//         .execute_unary(
//             kv_store::SetRequest {
//                 key: "foo".into(),
//                 value: "bar".into(),
//             },
//             |chan, req| kv_store::KvClient::new(chan).set(req),
//         )
//         .await?;

//     // 获取值
//     let get_response = client
//         .execute_unary(
//             kv_store::GetRequest { key: "foo".into() },
//             |chan, req| kv_store::KvClient::new(chan).get(req),
//         )
//         .await?;

//     println!("Value: {:?}", get_response.into_inner().value);
//     Ok(())
// }

//// 使用实例
//// 双向流（聊天服务）
// mod chat {
//     tonic::include_proto!("chat");
// }

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let client = SmartClient::new(vec![
//         "http://node1:50052".to_string(),
//         "http://node2:50052".to_string(),
//     ]).await?;

//     // 打开聊天流
//     let mut stream = client
//         .open_stream(
//             chat::ConnectRequest {
//                 user: "Alice".into(),
//             },
//             |chan, req| chat::ChatClient::new(chan).join_chat(req),
//         )
//         .await?;

//     // 接收消息
//     let recv_task = tokio::spawn(async move {
//         while let Some(msg) = stream.next().await {
//             match msg {
//                 Ok(msg) => println!("Received: {}", msg.text),
//                 Err(e) => eprintln!("Stream error: {}", e),
//             }
//         }
//     });

//     // 发送消息
//     let sender = client.clone();
//     let send_task = tokio::spawn(async move {
//         let message = chat::ChatMessage {
//             text: "Hello world!".into(),
//         };
//         sender
//             .execute_unary(message, |chan, req| chat::ChatClient::new(chan).send(req))
//             .await
//             .unwrap();
//     });

//     tokio::try_join!(recv_task, send_task)?;
//     Ok(())
// }
