use super::buffer::PartitionWriterBuffer;
use crate::disk::{
    StorageError,
    fd_cache::FdWriterCacheAync,
    meta::{TopicMeta, WriterPositionPtr},
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use log::{error, warn};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc, time};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct PartitionState {
    pub last_write_time: Instant,
    pub write_count: u32,
    pub flush_state: FlushState,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum FlushState {
    Hot,   // 高频写入分区
    Warm,  // 近期写入分区
    Cold,  // 空闲分区
    Stale, // 需要强制刷盘的分区
}

#[derive(Clone)]
pub struct Flusher {
    stop: CancellationToken,
    fd_cache: Arc<FdWriterCacheAync>,

    partition_writer_buffers: Arc<DashMap<PathBuf, Arc<PartitionWriterBuffer>>>,
    pub partition_states: Arc<DashMap<PathBuf, PartitionState>>,

    topic_metas: Arc<DashMap<PathBuf, TopicMeta>>,
    partition_writer_ptrs: Arc<DashMap<PathBuf, Arc<WriterPositionPtr>>>,
    interval: Duration,

    partition_writer_buffer_tasks_num: usize,
    // 存储 PartitionWriterBuffer 异步刷盘任务的 sender
    partition_writer_buffer_tasks: Arc<Vec<mpsc::Sender<(bool, Arc<PartitionWriterBuffer>)>>>,

    partition_writer_ptr_tasks_num: usize,
    // 存储 PartitionWriterPtr 异步刷盘任务的 sender
    partition_writer_ptr_tasks: Arc<Vec<mpsc::Sender<(bool, PathBuf, Arc<WriterPositionPtr>)>>>,
}

impl Drop for Flusher {
    fn drop(&mut self) {
        for tx in self.partition_writer_buffer_tasks.iter() {
            let _ = tx;
        }
        for tx in self.partition_writer_ptr_tasks.iter() {
            let _ = tx;
        }
    }
}

impl Flusher {
    pub fn new(
        stop: CancellationToken,
        partition_writer_buffer_tasks_num: usize,
        partition_writer_ptr_tasks_num: usize,
        interval: Duration,
        fd_cache: Arc<FdWriterCacheAync>,
    ) -> Self {
        let mut buffer_tasks = vec![];
        for _ in 0..partition_writer_buffer_tasks_num {
            let _stop = stop.clone();
            let (tx, mut rx) = mpsc::channel::<(bool, Arc<PartitionWriterBuffer>)>(1);
            tokio::spawn(async move {
                loop {
                    if rx.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            warn!("Flusher: partition_writer_ptr_tasks receive stop signal, exit.");
                            break;
                        }

                        res = rx.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (should_sync, pwb) = res.unwrap();
                            if let Err(e) = pwb.flush(should_sync).await {
                                error!("Flusher: partition_writer_buffer_tasks flush err: {e:?}")
                            }
                        }
                    }
                }
            });
            buffer_tasks.push(tx);
        }

        let mut ptr_tasks = vec![];
        for _ in 0..partition_writer_ptr_tasks_num {
            let _stop = stop.clone();
            let (tx, mut rx) = mpsc::channel::<(bool, PathBuf, Arc<WriterPositionPtr>)>(1);
            let _fd_cache = fd_cache.clone();

            tokio::spawn(async move {
                loop {
                    if rx.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            warn!("Flusher: partition_writer_ptr_tasks receive stop signal, exit.");
                            break;
                        }

                        res = rx.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (should_sync, _p, pwp) = res.unwrap();
                            if let Err(e) = pwp.save_to(should_sync).await {
                                error!("pwp.save_to err: {e:?}");
                            }
                        }
                    }
                }
            });
            ptr_tasks.push(tx);
        }

        Self {
            stop,
            fd_cache,
            partition_writer_buffers: Arc::new(DashMap::new()),
            partition_states: Arc::default(),
            interval,
            topic_metas: Arc::default(),
            partition_writer_ptrs: Arc::default(),
            partition_writer_buffer_tasks_num,
            partition_writer_buffer_tasks: Arc::new(buffer_tasks),
            partition_writer_ptr_tasks_num,
            partition_writer_ptr_tasks: Arc::new(ptr_tasks),
            // partition_writer_ptr_tasks: Arc::default(),
        }
    }

    pub async fn run(&mut self, mut flush_signal: mpsc::Receiver<()>) {
        let mut state_updater = tokio::time::interval(Duration::from_secs(5));
        let mut hot_ticker = time::interval(self.interval);
        let mut warm_ticker = time::interval(self.interval * 10 * 10);
        let mut cold_ticker = time::interval(self.interval * 10 * 10 * 10);
        loop {
            select! {
                _ = hot_ticker.tick() => {
                    self.flush_by_state(FlushState::Hot, false);
                }

                _ = warm_ticker.tick() => {
                    self.flush_by_state(FlushState::Warm, false);
                }

                _ = cold_ticker.tick() => {
                    self.flush_by_state(FlushState::Cold, false);
                    self.flush_topic_meta_and_partition_writer_ptr(false).await;
                }

                _ = flush_signal.recv() => {
                    self.graceful_flush_all(false).await;
                }

                // 状态更新
                _ = state_updater.tick() => {
                    self.update_partition_states();
                }

                _ = self.stop.cancelled() => {
                    warn!("Flusher: receive stop signal, exit.");
                    break;
                }
            }
        }
    }

    #[inline]
    pub fn add_partition_writer(&self, key: PathBuf, writer: PartitionWriterBuffer) {
        self.partition_writer_buffers
            .insert(key.clone(), Arc::new(writer));
        self.partition_states.insert(
            key,
            PartitionState {
                last_write_time: Instant::now(),
                write_count: 0,
                flush_state: FlushState::Stale,
            },
        );
    }

    #[inline]
    pub fn add_topic_meta(&self, topic: PathBuf, meta: TopicMeta) {
        self.topic_metas.insert(topic, meta);
    }

    #[inline]
    pub fn add_partition_writer_ptr(&self, partition: PathBuf, ptr: Arc<WriterPositionPtr>) {
        self.partition_writer_ptrs.insert(partition, ptr);
    }

    // 更新所有的 partition_states
    fn update_partition_states(&self) {
        for mut entry in self.partition_states.iter_mut() {
            entry.flush_state = Self::calculate_flush_state(entry.last_write_time);
            // 重置计数
            if entry.write_count > 0 {
                entry.write_count = 0;
            }
        }
    }

    // 更新指定的 partition_states 的所有信息
    pub fn update_partition_write_count(&self, p: &PathBuf, write_count: u32) {
        if let Some(mut state) = self.partition_states.get_mut(p) {
            state.last_write_time = Instant::now();
            state.write_count += write_count;
            state.flush_state = Self::calculate_flush_state(state.last_write_time);
        }
    }

    pub fn calculate_flush_state(last_write_time: Instant) -> FlushState {
        let now = Instant::now();
        if now.duration_since(last_write_time) < Duration::from_secs(10) {
            FlushState::Hot
        } else if now.duration_since(last_write_time) < Duration::from_secs(60) {
            FlushState::Warm
        } else if now.duration_since(last_write_time) > Duration::from_secs(60) {
            FlushState::Stale
        } else {
            FlushState::Cold
        }
    }

    pub async fn flush_topic_partition(&self, p: &PathBuf, should_sync: bool) -> Result<()> {
        let pwb = self.partition_writer_buffers.get(p);
        if pwb.is_none() {
            return Err(anyhow!(
                StorageError::PartitionNotFound("Flusher".to_string()).to_string()
            ));
        }
        let pwb = pwb.unwrap();
        let idx = rand::random::<u32>() as usize;
        self.partition_writer_buffer_tasks[idx % self.partition_writer_buffer_tasks_num]
            .send((should_sync, pwb.value().clone()))
            .await?;
        Ok(())
    }

    // should_sync = false 时，交给操作系统刷脏页落盘
    async fn flush_topic_meta_and_partition_writer_ptr(&self, should_sync: bool) {
        let topic_metas: Vec<(PathBuf, TopicMeta)> = self
            .topic_metas
            .iter_mut()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (filename, meta) in topic_metas {
            if let Err(e) = meta.save().await {
                error!("Flush topic_metas [{filename:?}] failed: {e:?}");
            }
        }

        // 处理 PartitionWriterPtr
        let partition_writer_ptrs: Vec<(PathBuf, Arc<WriterPositionPtr>)> = self
            .partition_writer_ptrs
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (idx, (filename, pwp)) in partition_writer_ptrs.iter().enumerate() {
            if let Err(e) = self.partition_writer_ptr_tasks
                [idx % self.partition_writer_ptr_tasks_num]
                .try_send((should_sync, filename.clone(), pwp.clone()))
            {
                error!("Flusher: flush_all send PartitionWriterPtr err: {e:?}")
            }
        }
    }

    // should_sync = false 时，交给操作系统刷脏页落盘
    async fn graceful_flush_all(&self, should_sync: bool) {
        // 第一级：刷热点分区 (最近10秒内有写入)
        self.flush_by_state(FlushState::Hot, should_sync);

        // 第二级：刷温分区 (最近60秒内有写入)
        self.flush_by_state(FlushState::Warm, should_sync);

        // 第三级：刷过期分区 (超过60秒未刷)
        self.flush_by_state(FlushState::Stale, should_sync);
    }

    // should_sync = false 时，交给操作系统刷脏页落盘
    fn flush_by_state(&self, state: FlushState, should_sync: bool) {
        let writers: Vec<_> = self
            .partition_writer_buffers
            .iter()
            .filter(|entry| {
                let state_entry = self.partition_states.get(entry.key()).unwrap();
                state_entry.flush_state == state
            })
            .map(|entry| entry.value().clone())
            .collect();

        for (idx, writer) in writers.iter().enumerate() {
            // 使用现有任务分发机制
            // let idx = /* 计算任务索引 */;
            if let Err(e) = self.partition_writer_buffer_tasks
                [idx % self.partition_writer_buffer_tasks_num]
                .try_send((should_sync, writer.clone()))
            // 发送不成功就进入下次循环发送，不阻塞
            {
                error!("Flush error: {:?}", e);
            }
        }
    }
}

// metrics
impl Flusher {
    // 添加获取分区指标的方法
    pub fn get_partition_metrics(&self) -> Vec<PartitionMetrics> {
        // if let Some(ts) = self.topics.get(topic) {
        let mut result = vec![];
        for pwb in self.partition_writer_buffers.iter() {
            let metrics = pwb.get_metrics();
            result.push(PartitionMetrics {
                partition_path: pwb.key().clone(),
                flush_count: Arc::new(AtomicU64::new(metrics.flush_count.load(Ordering::Relaxed))),
                flush_bytes: Arc::new(AtomicU64::new(metrics.flush_bytes.load(Ordering::Relaxed))),
                flush_latency_us: Arc::new(AtomicU64::new(
                    metrics.flush_latency_us.load(Ordering::Relaxed),
                )),
                min_start_timestamp: Arc::new(AtomicU64::new(
                    metrics.min_start_timestamp.load(Ordering::Relaxed),
                )),
                max_end_timestamp: Arc::new(AtomicU64::new(
                    metrics.max_end_timestamp.load(Ordering::Relaxed),
                )),
            });
        }
        result
    }

    pub fn reset_metrics(&self) {
        for entry in self.partition_writer_buffers.iter() {
            let pwb = entry.value();
            pwb.reset_metrics();
        }
    }
}

// 添加监控结构体
#[derive(Default, Clone, Debug)]
pub struct PartitionMetrics {
    pub partition_path: PathBuf,
    pub flush_count: Arc<AtomicU64>,         // 刷盘次数
    pub flush_bytes: Arc<AtomicU64>,         // 刷盘字节数
    pub flush_latency_us: Arc<AtomicU64>,    // 刷盘耗时微秒数
    pub min_start_timestamp: Arc<AtomicU64>, // 刷盘期间最早开始时间戳（微秒）
    pub max_end_timestamp: Arc<AtomicU64>,   // 刷盘期间最晚结束时间戳（微秒）
}
