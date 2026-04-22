use anyhow::Result;
use dashmap::DashMap;
use log::{error, info};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
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

    tasks_num: u32,

    global_size_limit: Arc<AtomicU64>,

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
        global_size_limit: Arc<AtomicU64>,
        waits: Arc<DashMap<(String, u32), Arc<PartitionWriterHandle>>>,
        with_metrics: bool,
    ) -> Self {
        let metrics = StorageWriterMetrics::default();
        let mut tasks_channel = Vec::with_capacity(tasks_num as _);
        for _ in 0..tasks_num {
            let _stop = stop.child_token();
            let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
            tasks_channel.push(tx);
            let _metrics = metrics.clone();
            let _global_size_limit = global_size_limit.clone();
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
                                        match res {
                                            Ok((sim,l,s)) => {
                                                _global_size_limit.fetch_sub(sim as u64, Ordering::Relaxed);
                                                _metrics.update_data_min_start_timestamp();
                                                _metrics.inc_data_flush_count(l, 0);
                                                _metrics.inc_flush_bytes(s, 0);
                                                _metrics.update_data_max_end_timestamp();
                                            }
                                            Err(e) => {
                                                _metrics.inc_data_flush_count(0, 1);
                                                error!("{:?}", e);
                                            }
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
            tasks_num,
            global_size_limit,
            waits,
            tasks_channel: Arc::new(tasks_channel),
            with_metrics,
            metrics,
        }
    }

    pub async fn run(self, mut flush_signal: Receiver<(bool, bool)>) {
        info!("[STORAGE]: start flusher task...");
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
        self.flush_interval(self.flush_interval.as_millis(), true, false)
            .await;
    }

    async fn flush_warm(&self) {
        self.flush_interval(self.flush_interval.as_millis() * 2, true, false)
            .await;
    }

    async fn flush_cold(&self) {
        self.flush_interval(self.flush_interval.as_millis() * 4, true, false)
            .await;
    }

    async fn flush_interval(&self, i: u128, all: bool, fsync: bool) {
        let now1 = Instant::now();
        let need_flush: Vec<Arc<PartitionWriterHandle>> = self
            .waits
            .iter()
            .filter(|x| {
                if i == 0 {
                    return true;
                }

                now1.duration_since(x.latest_write_timestamp.load())
                    .as_millis() as u128
                    > i
            })
            .map(|v| v.value().clone())
            .collect();

        if need_flush.is_empty() || self.tasks_channel.is_empty() {
            return;
        }

        let worker_num = self.tasks_channel.len();
        let chunk_size = need_flush.len().div_ceil(worker_num);
        for (i, c) in need_flush.chunks(chunk_size).enumerate() {
            let _ = self.tasks_channel[i].send((c.to_vec(), all, fsync)).await;
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
