use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use log::error;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    time,
};
use tokio_util::sync::CancellationToken;

use crate::{disk::partition::PartitionWriterHandle, metrics::StorageWriterMetrics};

type PartitionFlushFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
const CHANNEL_BUFFER_SIZE: usize = 1;

#[derive(Clone)]
pub struct Flusher {
    stop: CancellationToken,
    flush_interval: Duration,

    waits: Arc<DashMap<(String, u32), Arc<PartitionWriterHandle>>>,

    // 1st: Vec<Arc<PartitionWriterHandle>>: Wait flush PartitionWriterHandle Vec
    // 2nd: bool: pop all
    // 3rd: bool: fsync
    tasks_channel: Arc<Vec<Sender<(Vec<Arc<PartitionWriterHandle>>, bool, bool)>>>,

    with_metrics: bool,
    metrics: StorageWriterMetrics,
}

impl Flusher {
    pub fn new(
        stop: CancellationToken,
        tasks_num: u32,
        flush_interval: Duration,
        waits: Arc<DashMap<(String, u32), Arc<PartitionWriterHandle>>>,
        with_metrics: bool,
    ) -> Self {
        let mut tasks_channel = Vec::with_capacity(tasks_num as _);
        for _ in 0..tasks_num {
            let _stop = stop.child_token();
            let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
            tasks_channel.push(tx);
            tokio::spawn(async move {
                loop {
                    select! {
                        _ = _stop.cancelled() => {
                            return;
                        }

                        chunk = rx.recv() => {
                            if chunk.is_none() {
                                continue;
                            }
                            let (chunk, all, fsync): (
                                Vec<Arc<PartitionWriterHandle>>,
                                bool,
                                bool,
                            ) = chunk.unwrap();
                            for c in chunk {
                                select! {
                                    _ = _stop.cancelled() => {
                                        return;
                                    }
                                    res = c.flush(all, fsync) => {
                                        if let Err(e) = res{
                                            error!("{}", e.to_string())
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

        Self {
            stop,
            flush_interval,
            waits,
            tasks_channel: Arc::new(tasks_channel),
            with_metrics,
            metrics: StorageWriterMetrics::default(),
        }
    }

    pub async fn run(self, mut flush_signal: Receiver<(bool, bool)>) {
        let mut hot_ticker = time::interval(self.flush_interval);
        let mut warm_ticker = time::interval(self.flush_interval * 2);
        let mut cold_ticker = time::interval(self.flush_interval * 3);
        let _stop = self.stop.clone();

        loop {
            select! {
                _ = _stop.cancelled() => {
                    return;
                }

                _ = hot_ticker.tick() => {
                    self.flush_hot().await;
                }

                _ = warm_ticker.tick() => {
                    self.flush_warm().await;
                }

                _ = cold_ticker.tick() => {
                    self.flush_cold().await;
                }

                s = flush_signal.recv() => {
                    if s.is_none(){
                        continue;
                    }
                    let (all, fsync) = s.unwrap();
                    self.flush_interval(0, all, fsync).await;
                }
            }
        }
    }

    async fn flush_hot(&self) {
        self.flush_interval(self.flush_interval.as_secs(), true, false)
            .await;
    }

    async fn flush_warm(&self) {
        self.flush_interval(self.flush_interval.as_secs() * 2, true, false)
            .await;
    }

    async fn flush_cold(&self) {
        self.flush_interval(self.flush_interval.as_secs() * 4, true, false)
            .await;
    }

    async fn flush_interval(&self, i: u64, all: bool, fsync: bool) {
        let now = Utc::now().timestamp() as u64;
        let chunk: Vec<Arc<PartitionWriterHandle>> = self
            .waits
            .iter()
            .filter(|x| {
                if i == 0 {
                    return true;
                }
                now - x.latest_write_timestamp.load(Ordering::Relaxed) > i
            })
            .map(|v| v.value().clone())
            .collect();

        for c in chunk {
            let fut: PartitionFlushFuture<'_> =
                Box::pin(PartitionWriterHandle::flush(c.as_ref(), all, fsync));
            select! {
                _ = self.stop.cancelled() => {
                    return;
                }
                res = fut => {
                    let _ = res;
                }
            }
        }
    }
}

// metrics
impl Flusher {
    // 添加获取分区指标的方法
    pub(crate) fn get_metrics(&self) -> StorageWriterMetrics {
        self.metrics.clone()
    }

    pub(crate) fn reset_metrics(&self) {
        self.metrics.reset();
    }
}
