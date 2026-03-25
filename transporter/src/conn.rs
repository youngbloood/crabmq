use crate::TransportMessage;
#[cfg(feature = "service")]
use crate::TransporterWriter;
use anyhow::Result;
use std::{net::SocketAddr, net::ToSocketAddrs, time::Duration};

/**
 * ProtocolGetRemoteAddr 定义了获取远端地址的接口，get_remote_addr 方法根据连接 ID 获取远端地址
 */
pub trait ProtocolGetRemoteAddr {
    fn get_remote_addr(&self, conn_id: u64) -> Option<SocketAddr>;
}

/**
 * ProtocolTransporterShutdown 定义了关闭服务的接口，shutdown 方法用于停止服务
 */
#[async_trait::async_trait]
pub trait ProtocolTransporterShutdown {
    // 停止服务
    async fn shutdown(&self);
}

/**
 * ProtocolTransporterCloser 定义了连接关闭功能，close 方法用于关闭连接，closed 方法用于检查连接是否已关闭
 */
#[async_trait::async_trait]
pub trait ProtocolTransporterCloser {
    // 关闭指定连接
    async fn close(&self);
    // 检查连接是否已关闭
    async fn closed(&self) -> bool;
}

/**
 * ProtocolTransporterWriter 定义了发送消息的接口，send 方法将消息发送到远端地址
 */
#[async_trait::async_trait]
pub trait ProtocolTransporterWriter: Send + Sync + 'static {
    async fn send(&self, cmd: &TransportMessage, t: Option<Duration>) -> Result<()>;
}

/**
 * ProtocolTransporterService 定义了本地监听服务的接口，启动服务并从 channel 中获取消息
 */
#[cfg(feature = "service")]
#[async_trait::async_trait]
pub trait ProtocolTransporterService:
    ProtocolTransporterWriter + ProtocolTransporterShutdown + ProtocolGetRemoteAddr + Send + Sync
{
    // 启动本地监听服务
    async fn run(&self) -> Result<()>;

    async fn split_writer(&self, conn_id: u64) -> Option<TransporterWriter>;
    // 广播
    async fn broadcast(&self, cmd: &TransportMessage) -> Result<()>;

    async fn close(&self, conn_id: u64) -> Result<()>;
}

/**
 * ProtocolTransporterClient 定义了连接到远端地址的接口，建立连接并加入到管理器中
 */
#[cfg(feature = "client")]
#[async_trait::async_trait]
pub trait ProtocolTransporterClient:
    ProtocolTransporterShutdown + ProtocolTransporterCloser
{
    async fn connect(
        &self,
        remote_addr: SocketAddr,
        timeout: Duration,
    ) -> Result<TransporterWriter>;
}

static CONN_ID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
// 生成一个全局唯一的连接 ID，可以使用 UUID 或者其他方法来实现
pub(crate) fn get_conn_id() -> u64 {
    CONN_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
