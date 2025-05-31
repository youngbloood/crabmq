use crate::commonsvc::{self, CooInfo};
use dashmap::{
    DashMap,
    mapref::multiple::{RefMulti, RefMutMulti},
};
use futures::{Stream, StreamExt};
use log::{debug, error, info, warn};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{select, time};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, IntoStreamingRequest, Request, Response, Status, Streaming,
    transport::{Channel, Endpoint},
};
use tower::ServiceExt;

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
struct CooEndpoints {
    // term
    term: Arc<AtomicU64>,
    // 循环次数，保证endpoints下的每个endpoint都有相同几率被轮训到
    cycle_times: Arc<AtomicU64>,
    endpoints: Arc<DashMap<u32, CooEndpoint>>,
}

impl Default for CooEndpoints {
    fn default() -> Self {
        Self {
            term: Default::default(),
            cycle_times: Arc::new(AtomicU64::new(1)),
            endpoints: Default::default(),
        }
    }
}

impl CooEndpoints {
    fn apply(&self, resp: commonsvc::CooListResp) {
        if resp.cluster_term <= self.term.load(Ordering::Relaxed) {
            return;
        }
        info!("[SmartClient]: refresh the coo endpoints to local...");
        self.term.store(resp.cluster_term, Ordering::Release);
        for v in resp.list {
            self.endpoints
                .entry(v.id)
                .and_modify(|entry| {
                    entry.apply(&v);
                })
                .or_insert(CooEndpoint::from_coo_info(
                    &v,
                    self.cycle_times.load(Ordering::Relaxed),
                ));
        }
        debug!("[SmartClient]: apply the CooListResp: {:?}", self.endpoints);
    }

    fn erase_role(&self) {
        self.endpoints
            .iter_mut()
            .for_each(|mut v| v.role = commonsvc::CooRole::Follower);
    }

    /// 擦除原来的 leader 角色，并重新设置新的 Leader 角色
    fn set_leader(&self, addr: &str) {
        if let Some(mut res) = self.endpoints.iter_mut().find(|v| v.coo_addr == addr) {
            self.erase_role();
            res.role = commonsvc::CooRole::Leader;
        }
    }

    fn rorate_cycle_times(&self) {
        self.cycle_times.fetch_add(1, Ordering::Relaxed);
    }

    fn find_leader(&self) -> Option<RefMulti<'_, u32, CooEndpoint>> {
        debug!(
            "[SmartClient]: find_leader CooEndpoints: {:?}",
            self.endpoints
        );
        self.endpoints
            .iter()
            .find(|n| n.role == commonsvc::CooRole::Leader)
    }

    fn find_not_leader(&self) -> Vec<RefMutMulti<'_, u32, CooEndpoint>> {
        // 优先获取 Role != Leader
        let cycle_times = self.cycle_times.load(Ordering::Relaxed);
        let nodes_list: Vec<_> = self
            .endpoints
            .iter_mut()
            .filter(|n| n.role != commonsvc::CooRole::Leader && n.cycle_times < cycle_times)
            .collect();
        nodes_list
    }
}

#[derive(Debug)]
struct CooEndpoint {
    id: u32,
    coo_addr: String,
    raft_addr: String,
    role: commonsvc::CooRole,
    cycle_times: u64,
}

impl CooEndpoint {
    fn from_coo_info(coo_info: &CooInfo, cycle_times: u64) -> Self {
        Self {
            id: coo_info.id,
            coo_addr: coo_info.coo_addr.clone(),
            raft_addr: coo_info.raft_addr.clone(),
            role: commonsvc::CooRole::from_i32(coo_info.role).unwrap(),
            cycle_times: cycle_times - 1,
        }
    }

    fn apply(&mut self, coo_info: &CooInfo) -> bool {
        if self.id != coo_info.id {
            return false;
        }
        self.coo_addr = coo_info.coo_addr.clone();
        self.raft_addr = coo_info.raft_addr.clone();
        self.role = commonsvc::CooRole::from_i32(coo_info.role).unwrap();

        true
    }

    fn rorate_cycle_times(&mut self) {
        self.cycle_times += 1;
    }
}

#[derive(Clone)]
pub struct SmartClient {
    initial_endpoints: Vec<String>,
    endpoints: CooEndpoints,
    channels: Arc<DashMap<String, Channel>>,
    retry_policy: RetryPolicy,
}

impl SmartClient {
    pub fn new(initial_endpoints: Vec<String>) -> Self {
        Self {
            initial_endpoints,
            endpoints: CooEndpoints::default(),
            channels: Arc::new(DashMap::new()),
            retry_policy: RetryPolicy::default(),
        }
    }

    /// 根据 R 动态获取目标集群节点列表
    pub async fn refresh_coo_endpoints<R, Fut>(&self, r: R)
    where
        R: Fn(Channel) -> Fut,
        Fut: std::future::Future<
                Output = Result<Response<Streaming<commonsvc::CooListResp>>, Status>,
            >,
    {
        let mut backoff = self.retry_policy.backoff_base;
        loop {
            // 步骤1：获取初始连接
            let (chan, addr) = match self.get_channel().await {
                Ok((chan, addr)) => (chan, addr),
                Err(e) => {
                    self.handle_refresh_error(e).await;
                    time::sleep(backoff).await;
                    backoff = std::cmp::min(
                        backoff * self.retry_policy.backoff_factor,
                        Duration::from_secs(5),
                    );
                    continue;
                }
            };

            // 步骤2：建立流式连接并持久监听
            match r(chan).await {
                Ok(response) => {
                    let mut strm = response.into_inner();
                    let mut heartbeat_timeout = time::interval(Duration::from_secs(30));

                    // 持久监听循环
                    loop {
                        tokio::select! {
                            // 监听服务端数据
                            msg = strm.next() => {
                                match msg {
                                    Some(Ok(resp)) => {
                                        // 成功收到数据：重置退避时间
                                        backoff = self.retry_policy.backoff_base;
                                        // 处理响应（若返回 false 表示需要重连）
                                        if !self.process_coo_response(resp, addr.clone()).await {
                                            warn!("[SmartClient]->[{}]: target is not leader, switch to leader...", addr);
                                            // self.erase_role_and_channel(&addr);
                                            break; // 退出内层循环触发重连
                                        }
                                    }
                                    Some(Err(e)) => {
                                        // 流错误：记录并重连
                                        error!("[SmartClient]->[{}]: strm error: {:?}", addr, e);
                                        self.erase_role();
                                        self.handle_refresh_error(e).await;
                                        break;
                                    }
                                    None => {
                                        error!("[SmartClient]->[{}]: strm has been closed by server", addr);
                                        self.erase_role();
                                        // 流正常关闭：主动重连
                                        break;
                                    }
                                }
                            }
                            // 心跳超时检测
                            _ = heartbeat_timeout.tick() => {
                                warn!("[SmartClient]->[{}]: Stream heartbeat timeout, monitor CooListResp continue...", addr);
                                // break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("[SmartClient]->[{}]: invoke closure r error: {:?}", addr, e);
                    self.erase_channel(&addr);
                    self.handle_refresh_error(e).await;
                }
            }

            // 步骤3：连接中断后等待退避时间
            time::sleep(backoff).await;
            backoff = std::cmp::min(
                backoff * self.retry_policy.backoff_factor,
                Duration::from_secs(5),
            );
        }
    }

    /// 单次调用
    pub async fn execute_unary<Req, Res, F, Fut>(
        &self,
        request: Req,
        call: F,
    ) -> Result<Response<Res>, Status>
    where
        Req: Send + Clone,
        F: Fn(Channel, Request<Req>, String) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Res>, Status>>,
    {
        let req = request.clone();
        self.retry_operation(|channel, addr| call(channel, Request::new(req.clone()), addr))
            .await
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
        F: Fn(Channel, SmartReqStream<Req>, String) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Res>, Status>>,
    {
        self.retry_operation(|channel, addr| {
            let stream = stream_factory();
            call(channel, SmartReqStream::new(Box::pin(stream)), addr)
        })
        .await
    }

    /// 开启服务端流（客户端发送单个请求 ，服务端响应流）
    pub async fn open_sstream<Req, Res, C, Fut, HR, HRFut>(
        &self,
        request: Req,
        call: C,
        handle_resp: HR,
        stop: CancellationToken,
    ) -> Result<(), Status>
    where
        Req: Send + Clone,
        C: Fn(Channel, Request<Req>, String) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Streaming<Res>>, Status>>,
        HR: Fn(Res) -> HRFut,
        HRFut: std::future::Future<Output = Result<(), Status>>,
    {
        loop {
            let req = request.clone();
            let resp = self
                .retry_operation(|channel, addr| call(channel, Request::new(req.clone()), addr))
                .await?;

            let mut resp_strm = SmartStream::new(resp.into_inner(), self.clone());
            loop {
                select! {
                    resp = resp_strm.next() => {
                        if resp.is_none() {
                            continue;
                        }
                        match resp.unwrap(){
                            Ok(res) =>  handle_resp(res).await?,
                            Err(status) => {
                                self.handle_refresh_error(status).await;
                                break;
                            },
                        }
                    }

                    _ = stop.cancelled() => {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// 开启双向流（客户端和服务端双向流）
    pub async fn open_bistream<Req, Res, C, Fut, F, HR, HRFut>(
        &self,
        stream_factory: F,
        call: C,
        handle_resp: HR,
        stop: CancellationToken,
    ) -> Result<(), Status>
    where
        Req: Send + Clone + 'static,
        C: Fn(Channel, SmartReqStream<Req>, String) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Streaming<Res>>, Status>>,
        F: Fn() -> ReceiverStream<Req>,
        HR: Fn(Res) -> HRFut,
        HRFut: std::future::Future<Output = Result<(), Status>>,
    {
        loop {
            let resp = self
                .retry_operation(|channel, addr| {
                    let stream = stream_factory();
                    call(channel, SmartReqStream::new(Box::pin(stream)), addr)
                })
                .await?;

            let mut resp_strm = SmartStream::new(resp.into_inner(), self.clone());
            loop {
                select! {
                    resp = resp_strm.next() => {
                        if resp.is_none() {
                            continue;
                        }
                        match resp.unwrap(){
                            Ok(res) =>  handle_resp(res).await?,
                            Err(status) => {
                                self.handle_refresh_error(status).await;
                                break;
                            },
                        }
                    }

                    _ = stop.cancelled() => {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn retry_operation<T, F, Fut>(&self, mut operation: F) -> Result<T, Status>
    where
        F: FnMut(Channel, String) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let mut retries = 0;
        let mut backoff = self.retry_policy.backoff_base;

        loop {
            match self.must_leader_channel().await {
                Ok((chan, addr)) => match operation(chan, addr).await {
                    Ok(result) => {
                        // info!("[SmartClient]: connect to Leader and operation success");
                        return Ok(result);
                    }
                    Err(e) => {
                        error!("[SmartClient]: failed connect to Leader, status: {:?}", e);
                        if retries >= self.retry_policy.max_retries {
                            return Err(e.clone());
                        }

                        self.handle_refresh_error(e).await;
                        retries += 1;
                        tokio::time::sleep(backoff).await;
                        backoff *= self.retry_policy.backoff_factor;
                    }
                },
                Err(e) => {
                    warn!("[SmartClient]: get_leader_channel failed: {:?}", e);
                    retries += 1;
                    tokio::time::sleep(backoff).await;
                    backoff *= self.retry_policy.backoff_factor;
                }
            }
        }
    }

    async fn must_leader_channel(&self) -> Result<(Channel, String), Status> {
        // 优先获取 Role = Leader
        if let Some(leader) = self.endpoints.find_leader() {
            if let Some(chan) = self.try_get_existing_channel(&leader.coo_addr).await {
                return Ok((chan, leader.coo_addr.clone()));
            } else {
                return self.create_channel(&leader.coo_addr).await;
            }
        }

        Err(Status::aborted("Not Leader"))
    }

    /// 核心连接获取逻辑
    async fn get_channel(&self) -> Result<(Channel, String), Status> {
        if self.endpoints.endpoints.is_empty() {
            return self.get_channel_from_endpoints().await;
        }
        // 优先获取 Role = Leader
        if let Some(leader) = self.endpoints.find_leader() {
            if let Some(chan) = self.try_get_existing_channel(&leader.coo_addr).await {
                return Ok((chan, leader.coo_addr.clone()));
            } else {
                return self.create_channel(&leader.coo_addr).await;
            }
        }

        // 公平调用：获取未被轮训过的非 Leader 节点
        let mut nodes = self.endpoints.find_not_leader();
        if nodes.is_empty() {
            self.endpoints.rorate_cycle_times();
            nodes = self.endpoints.find_not_leader();
        }

        // 降级尝试其他节点
        // 遍历所有非 Leader 节点，尝试获取或创建通道
        for mut node in nodes {
            // 尝试获取现有通道
            if let Some(chan) = self.try_get_existing_channel(&node.coo_addr).await {
                node.rorate_cycle_times();
                return Ok((chan, node.coo_addr.clone()));
            }
            // 尝试创建新通道，成功则返回，否则继续下一个节点
            match self.create_channel(&node.coo_addr).await {
                Ok(res) => {
                    node.rorate_cycle_times();
                    return Ok(res);
                }
                Err(e) => {
                    error!(
                        "[SmartClient]: create coo endpoint[{}] channel failed: {:?}",
                        &node.coo_addr, e
                    );
                    // 及时该节点创建失败，也标记一次调用，下次轮到下一个创建
                    node.rorate_cycle_times();
                    continue;
                }
            }
        }

        Err(Status::new(Code::Unavailable, "No available nodes"))
    }

    async fn get_channel_from_endpoints(&self) -> Result<(Channel, String), Status> {
        for v in &self.initial_endpoints {
            if let Ok(res) = self.create_channel(v).await {
                return Ok(res);
            }
        }

        Err(Status::invalid_argument("all endpoints is unavailable"))
    }

    async fn try_get_existing_channel(&self, addr: &str) -> Option<Channel> {
        let addr = repair_addr_with_http(addr.to_string());
        if let Some(mut chan) = self.channels.get_mut(&addr) {
            // 检查通道是否处于可用状态
            if chan.ready().await.is_ok() {
                return Some(chan.value().clone());
            } else {
                // 自动清理无效连接
                self.channels.remove(&addr);
                return None;
            }
        }
        None
    }

    async fn create_channel(&self, addr: &str) -> Result<(Channel, String), Status> {
        let addr = repair_addr_with_http(addr.to_string());
        let endpoint = Endpoint::from_shared(addr.clone())
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?
            .connect_timeout(Duration::from_secs(3))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_timeout(Duration::from_secs(60))
            .tcp_keepalive(Some(Duration::from_secs(60)));

        let chan = endpoint
            .connect()
            .await
            .map_err(|e| Status::new(Code::Unavailable, e.to_string()))?;

        self.channels.insert(addr.to_string(), chan.clone());
        Ok((chan, addr))
    }

    async fn process_coo_response(&self, resp: commonsvc::CooListResp, addr: String) -> bool {
        if !has_leader(&resp) {
            // NOTE: 返回当中有 Leader 时才应用。
            // 网络分区场景：abcde组成集群，先abc形成一个分区，de形成一个分区
            // abc中返回的会有 Leader, 而de中返回无 Leader。但是de不断选举，导致term会比abc中的大。
            // 此处可能是 de 节点返回的信息，无 leader 直接忽略掉，继续进行下一个节点的链接
            return false;
        }
        let is_leader = now_endpoint_is_leader(&resp, &addr);
        self.endpoints.apply(resp);
        is_leader
    }

    async fn handle_refresh_error(&self, status: Status) {
        if let Some(addr) = extract_leader_address(&status) {
            if addr.is_empty() || addr == "unknown" {
                return;
            }
            self.endpoints.set_leader(&addr);
        }
    }

    fn erase_role(&self) {
        self.endpoints.erase_role()
    }

    fn erase_channel(&self, addr: &str) {
        self.channels.remove(addr);
    }
}

pub fn extract_leader_address(status: &Status) -> Option<String> {
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

fn has_leader(resp: &commonsvc::CooListResp) -> bool {
    resp.list
        .iter()
        .any(|ci| ci.role == commonsvc::CooRole::Leader.into())
}

fn now_endpoint_is_leader(resp: &commonsvc::CooListResp, addr: &str) -> bool {
    resp.list.iter().any(|ci| {
        ci.role == commonsvc::CooRole::Leader.into()
            && ci.coo_addr == repair_addr_with_http(addr.to_string())
    })
}

pub fn repair_addr_with_http(addr: String) -> String {
    if !(addr.starts_with("http") || addr.starts_with("https")) {
        return format!("http://{}", addr);
    }
    addr
}
