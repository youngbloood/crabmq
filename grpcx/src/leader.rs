use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::client::GrpcService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Interceptor, Request, Response, Status};
// 假设的 gRPC 服务定义

// 自定义错误类型
#[derive(Debug)]
pub enum ClientError {
    Tonic(Status),
    Transport(tonic::transport::Error),
    NoLeader,
    MaxRetriesExceeded,
}

impl From<tonic::Status> for ClientError {
    fn from(status: Status) -> Self {
        ClientError::Tonic(status)
    }
}

impl From<tonic::transport::Error> for ClientError {
    fn from(err: tonic::transport::Error) -> Self {
        ClientError::Transport(err)
    }
}

// 拦截器：捕获 PermissionDenial 错误并提取 leader 地址
fn leader_interceptor() -> Interceptor {
    Interceptor::new(
        move |mut req: Request<()>| {
            // 这里可以添加请求头等逻辑（可选）
            Ok(req)
        },
        move |resp: Result<Response<()>, Status>| {
            match resp {
                Ok(response) => Ok(response),
                Err(status) if status.code() == Code::PermissionDenied => {
                    // 从元数据中提取 leader 地址
                    let leader_address = status
                        .metadata()
                        .get("leader_address")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());

                    match leader_address {
                        Some(addr) => {
                            // 将 leader 地址附加到错误中，供上层处理
                            let mut new_status =
                                Status::new(Code::PermissionDenied, status.message());
                            new_status
                                .metadata_mut()
                                .insert("leader_address", addr.parse().unwrap());
                            Err(new_status)
                        }
                        None => Err(Status::new(
                            Code::PermissionDenied,
                            "No leader address provided",
                        )),
                    }
                }
                Err(status) => Err(status),
            }
        },
    )
}

// 智能切换客户端
pub struct SmartCooClient {
    current_channel: Arc<RwLock<Channel>>,
    addresses: Vec<String>, // coo 集群的所有节点地址
    max_retries: usize,
}

impl SmartCooClient {
    pub async fn new(addresses: Vec<String>, max_retries: usize) -> Result<Self, ClientError> {
        let initial_address = addresses.get(0).ok_or(ClientError::NoLeader)?;
        let channel = Endpoint::from_shared(initial_address.clone())?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;

        Ok(SmartCooClient {
            current_channel: Arc::new(RwLock::new(channel)),
            addresses,
            max_retries,
        })
    }

    // 切换到新的 leader 地址
    async fn switch_leader(&self, leader_address: &str) -> Result<(), ClientError> {
        let new_channel = Endpoint::from_shared(leader_address.to_string())?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;

        let mut channel = self.current_channel.write().await;
        *channel = new_channel;
        Ok(())
    }

    // 执行 gRPC 请求，支持自动切换
    pub async fn call<Req, Resp>(
        &self,
        request: Request<Req>,
        mut call: impl FnMut(Request<Req>, Channel) -> Result<Response<Resp>, Status> + Send,
    ) -> Result<Response<Resp>, ClientError> {
        let mut retries = 0;
        loop {
            let channel = self.current_channel.read().await.clone();
            let mut request = request.clone();
            request = leader_interceptor().call(request)?;

            match call(request, channel).await {
                Ok(response) => return Ok(response),
                Err(status) if status.code() == Code::PermissionDenied => {
                    if retries >= self.max_retries {
                        return Err(ClientError::MaxRetriesExceeded);
                    }

                    // 提取 leader 地址
                    let leader_address = status
                        .metadata()
                        .get("leader_address")
                        .and_then(|v| v.to_str().ok())
                        .ok_or_else(|| {
                            ClientError::Tonic(Status::new(
                                Code::PermissionDenied,
                                "No leader address",
                            ))
                        })?;

                    // 切换到新的 leader
                    self.switch_leader(leader_address).await?;
                    retries += 1;
                }
                Err(status) => return Err(status.into()),
            }
        }
    }

    // 示例方法：调用 CooService 的 say_hello
    // pub async fn say_hello(
    //     &self,
    //     req: HelloRequest,
    // ) -> Result<Response<HelloResponse>, ClientError> {
    //     self.call(Request::new(req), |req, channel| {
    //         let mut client = CooServiceClient::new(channel);
    //         async move { client.say_hello(req).await }.boxed()
    //     })
    //     .await
    // }
}

// // 示例主函数
// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // coo 集群的节点地址
//     let addresses = vec![
//         "http://coo1:50051".to_string(),
//         "http://coo2:50051".to_string(),
//         "http://coo3:50051".to_string(),
//     ];

//     // 创建智能客户端
//     let client = SmartCooClient::new(addresses, 3).await?;

//     // 发送请求
//     let request = HelloRequest {
//         name: "World".to_string(),
//     };
//     let response = client.say_hello(request).await?;
//     println!("Response: {:?}", response.into_inner().message);

//     Ok(())
// }
