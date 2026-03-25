use crate::{
    TransportMessage,
    codec::TransportCodec,
    decode_to_message,
    err::{ErrorCode, TransporterError},
};
use log::{error, warn};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::AsyncWriteExt as _,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    select,
    sync::mpsc::{Receiver, Sender},
    time::timeout,
};
use tokio_stream::StreamExt as _;
use tokio_util::{bytes::Bytes, codec::FramedRead, sync::CancellationToken};

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub(crate) use client::*;

#[cfg(feature = "service")]
mod service;
#[cfg(feature = "service")]
pub(crate) use service::*;

pub(crate) struct TcpReadHalf {
    tx: Sender<TransportMessage>,
    conn_id: u64,
    remote_addr: SocketAddr,
    shutdown: CancellationToken,

    max_frame_body_size: usize,
    idle_timeout: Duration,
}

impl Drop for TcpReadHalf {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TcpReadHalf {
    async fn loop_handle(self, rh: OwnedReadHalf) {
        let mut framed = FramedRead::new(rh, TransportCodec::new(self.max_frame_body_size));
        let conn_id = self.conn_id;
        let tx = self.tx.clone();
        let idle_timeout = self.idle_timeout;
        loop {
            select! {
                frame_res = framed.next() => {
                    if frame_res.is_none() {
                        // 网络断开了，或者说对端主动断开了连接
                        return;
                    }
                    match frame_res.unwrap() {
                        Ok((version, index, body)) => {
                            let msg = decode_to_message(version, index, conn_id, &body).map_err(|e| -> anyhow::Error {
                                TransporterError::new(ErrorCode::DecodeError, e.to_string()).into()
                            });

                            if let Err(e) = msg {
                                error!("[Transporter]: Failed to decode message: {}", e);
                                // 解析爆错了，直接断开连接
                                return;
                            }

                            let msg = msg.unwrap();
                            if tx.send(msg).await.is_err() {
                                // 上层 channel 已关闭，无需再读
                                return;
                            }
                        }

                        Err(e) => {
                            error!("[Transporter]: Failed to read frame: {}", e);
                            // 网络断开或解析爆错了，直接断开连接
                            return;
                        }
                    }
                }

                _ = self.shutdown.cancelled() => {
                    return;
                }

                // 真正的空闲超时：用独立的 sleep，每次收到包后重新进入 select!
                // 在没有收到任何包的情况下，sleep 会在 idle_timeout 后触发
                _ = tokio::time::sleep(idle_timeout) => {
                    warn!("[Transporter]: Idle timeout for conn_id: {}, remote_addr: {}", conn_id, self.remote_addr);
                    return;
                }
            }
        }
    }
}

pub struct TcpWriteHalf {
    w: OwnedWriteHalf,

    send_timeout: Duration,

    conn_id: u64,
    remote_addr: SocketAddr,

    shutdown: CancellationToken,
}

impl TcpWriteHalf {
    fn new(
        w: OwnedWriteHalf,
        send_timeout: Duration,
        conn_id: u64,
        remote_addr: SocketAddr,
        shutdown: CancellationToken,
    ) -> Self {
        TcpWriteHalf {
            w,
            send_timeout,
            conn_id,
            remote_addr,
            shutdown,
        }
    }

    async fn loop_handle(mut self, mut rx: Receiver<Bytes>) {
        let send_timeout = self.send_timeout;
        loop {
            select! {
                msg = rx.recv() => {
                    if msg.is_none() {
                        // channel 断了，说明不再有消息要发送了，可以关闭连接了
                        self.shutdown.cancel();
                        let _ = self.w.shutdown().await;
                        return;
                    }
                    let msg = msg.unwrap();

                    // 写超时
                    match tokio::time::timeout(send_timeout, self.w.write_all(&msg)).await {
                        Err(_elapsed) => {
                            warn!("[Transporter]: Send timeout for conn_id: {}, remote_addr: {}", self.conn_id, self.remote_addr);
                            self.shutdown.cancel();
                            let _ = self.w.shutdown().await;
                            return;
                        }

                        Ok(Err(e)) => {
                            error!("[Transporter]: Failed to send message: {}", e);
                            self.shutdown.cancel();
                            return;
                        }

                        Ok(Ok(())) => {} // 发送成功，继续
                    }
                }

                _ = self.shutdown.cancelled() => {
                    let _ = self.w.shutdown().await;
                    return;
                }
            }
        }
    }
}
