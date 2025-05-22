use std::{
    collections::HashSet,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, StreamExt};
use log::debug;
use tokio::{
    sync::{Mutex, RwLock},
    time,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Code, IntoStreamingRequest, Request, Response, Status, Streaming,
    transport::{Channel, Endpoint},
};

use crate::commonsvc;

#[derive(Debug, Clone)]
struct NodeState {
    addr: String,
    last_healthy: Option<time::Instant>,
    is_leader: bool,
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

#[derive(Clone)]
pub struct SmartClient {
    node_states: Arc<RwLock<Vec<NodeState>>>,
    channels: Arc<Mutex<Vec<(Channel, String)>>>,
    retry_policy: RetryPolicy,
    health_check_interval: Duration,
}

impl SmartClient {
    pub fn new(initial_endpoints: Vec<String>) -> Self {
        let node_states = initial_endpoints
            .into_iter()
            .map(|addr| NodeState {
                addr,
                last_healthy: None,
                is_leader: false,
            })
            .collect();

        Self {
            node_states: Arc::new(RwLock::new(node_states)),
            channels: Arc::new(Mutex::new(Vec::new())),
            retry_policy: RetryPolicy::default(),
            health_check_interval: Duration::from_secs(30),
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
                Ok(result) => {
                    debug!("SmartClient: operation success");
                    return Ok(result);
                }
                Err(status) => {
                    debug!("SmartClient: Status: {:?}", status);
                    if retries >= self.retry_policy.max_retries {
                        return Err(status.clone());
                    }

                    self.handle_refresh_error(status).await;
                    retries += 1;
                    tokio::time::sleep(backoff).await;
                    backoff *= self.retry_policy.backoff_factor;
                }
            }
        }
    }

    /// 核心连接获取逻辑
    async fn get_channel(&self) -> Result<Channel, Status> {
        let nodes = self.node_states.read().await;

        // 优先尝试最近健康的Leader
        if let Some(leader) = nodes
            .iter()
            .find(|n| n.is_leader && n.last_healthy.is_some())
        {
            if let Some(chan) = self.try_get_existing_channel(&leader.addr).await {
                return Ok(chan);
            }
        }

        // 降级尝试其他健康节点
        for node in nodes.iter().filter(|n| n.last_healthy.is_some()) {
            if let Some(chan) = self.try_get_existing_channel(&node.addr).await {
                return Ok(chan);
            }
        }

        // 最后尝试所有节点
        for node in nodes.iter() {
            match self.create_channel(&node.addr).await {
                Ok(chan) => return Ok(chan),
                Err(_) => continue,
            }
        }

        Err(Status::new(Code::Unavailable, "No available nodes"))
    }

    async fn try_get_existing_channel(&self, addr: &str) -> Option<Channel> {
        let channels = self.channels.lock().await;
        channels
            .iter()
            .find(|(_, a)| a == addr)
            .map(|(c, _)| c.clone())
    }

    async fn create_channel(&self, addr: &str) -> Result<Channel, Status> {
        let addr = if !(addr.starts_with("http") || addr.starts_with("https")) {
            format!("http://{}", addr)
        } else {
            addr.to_string()
        };
        let endpoint = Endpoint::from_shared(addr.clone())
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?
            .connect_timeout(Duration::from_secs(3));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Status::new(Code::Unavailable, e.to_string()))?;

        self.channels.lock().await.push((channel.clone(), addr));
        Ok(channel)
    }

    /// 动态刷新节点状态
    pub async fn refresh_coo_endpoints<F, Fut>(&self, r: F)
    where
        F: Fn(Channel) -> Fut,
        Fut: std::future::Future<
                Output = Result<Response<Streaming<commonsvc::CooListResp>>, Status>,
            >,
    {
        let mut backoff = self.retry_policy.backoff_base;

        loop {
            match self.get_channel().await {
                Ok(chan) => match r(chan).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        while let Some(Ok(resp)) = stream.next().await {
                            self.process_coo_response(resp).await;
                        }
                        break;
                    }
                    Err(e) => {
                        self.handle_refresh_error(e).await;
                    }
                },
                Err(e) => {
                    self.handle_refresh_error(e).await;
                }
            }

            time::sleep(backoff).await;
            backoff = std::cmp::min(
                backoff * self.retry_policy.backoff_factor,
                Duration::from_secs(5),
            );
        }
    }

    async fn process_coo_response(&self, resp: commonsvc::CooListResp) {
        let mut nodes = self.node_states.write().await;

        // 更新节点列表
        let mut new_addrs = HashSet::new();
        for info in resp.list {
            let addr = info.coo_addr;
            new_addrs.insert(addr.clone());

            if let Some(existing) = nodes.iter_mut().find(|n| n.addr == addr) {
                existing.is_leader = info.role == commonsvc::CooRole::Leader.into();
                existing.last_healthy = Some(time::Instant::now());
            } else {
                nodes.push(NodeState {
                    addr,
                    is_leader: info.role == commonsvc::CooRole::Leader.into(),
                    last_healthy: Some(time::Instant::now()),
                });
            }
        }

        // 清理失效节点
        nodes.retain(|n| new_addrs.contains(&n.addr));
    }

    async fn handle_refresh_error(&self, status: Status) {
        if let Some(addr) = extract_leader_address(&status) {
            if addr.is_empty() || addr == "unknown" {
                return;
            }
            self.mark_node_unhealthy(&addr).await;
        }
    }

    async fn mark_node_unhealthy(&self, addr: &str) {
        let mut nodes = self.node_states.write().await;
        if let Some(node) = nodes.iter_mut().find(|n| n.addr == addr) {
            node.last_healthy = None;
            node.is_leader = false;
        }

        // 清理失效连接
        let mut channels = self.channels.lock().await;
        channels.retain(|(_, a)| a != addr);
    }

    /// 后台健康检查任务
    pub fn start_health_check(&self) {
        let client = self.clone();
        tokio::spawn(async move {
            loop {
                client.check_nodes_health().await;
                time::sleep(client.health_check_interval).await;
            }
        });
    }

    async fn check_nodes_health(&self) {
        let nodes = self.node_states.read().await.clone();

        for node in nodes.iter() {
            match self.create_channel(&node.addr).await {
                Ok(_) => {
                    self.mark_node_healthy(&node.addr).await;
                }
                Err(_) => {
                    self.mark_node_unhealthy(&node.addr).await;
                }
            }
        }
    }

    async fn mark_node_healthy(&self, addr: &str) {
        let mut nodes = self.node_states.write().await;
        if let Some(node) = nodes.iter_mut().find(|n| n.addr == addr) {
            node.last_healthy = Some(time::Instant::now());
        }
    }
}

fn extract_leader_address(status: &Status) -> Option<String> {
    status
        .metadata()
        .get("x-raft-leader")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
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
                        client.handle_refresh_error(status).await;
                        // let mut current_leader = client.current_leader.lock().await;
                        // *current_leader = new_leader;
                        // client.cleanup_channels().await;
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
