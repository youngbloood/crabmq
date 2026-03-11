use crate::TransportMessage;
#[cfg(feature = "service")]
use crate::TransporterWriter;
use anyhow::Result;
use std::time::Duration;

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
 * ProtocolTransporterReader 定义了接收消息的接口，recv 方法从链接中获取消息
 */
#[async_trait::async_trait]
pub trait ProtocolTransporterReader: Send + Sync + 'static {
    async fn recv(&mut self, t: Option<Duration>) -> Option<TransportMessage>;
}

/**
 * ProtocolTransporterService 定义了本地监听服务的接口，启动服务并从 channel 中获取消息
 */
#[cfg(feature = "service")]
#[async_trait::async_trait]
pub trait ProtocolTransporterService:
    ProtocolTransporterReader + ProtocolTransporterWriter + ProtocolTransporterShutdown + Send + Sync
{
    // 启动本地监听服务
    async fn run(&self) -> Result<()>;

    async fn split_writer(&self, remote: &str) -> Option<TransporterWriter>;
    // 广播
    async fn broadcast(&self, cmd: &TransportMessage) -> Result<()>;

    async fn close(&self, remote: &str) -> Result<()>;
}

/**
 * ProtocolTransporterClient 定义了连接到远端地址的接口，建立连接并加入到管理器中
 */
#[cfg(feature = "client")]
#[async_trait::async_trait]
pub trait ProtocolTransporterClient:
    ProtocolTransporterWriter
    + ProtocolTransporterReader
    + ProtocolTransporterShutdown
    + ProtocolTransporterCloser
{
    async fn connect(&self, remote_addr: &str) -> Result<()>;
}
