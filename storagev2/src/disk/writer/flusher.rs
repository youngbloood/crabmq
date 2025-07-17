use super::buffer::PartitionWriterBuffer;
use crate::{
    MessageMeta, StorageError,
    disk::{PartitionIndexManager, meta::WriterPositionPtr},
    metrics::StorageWriterMetrics,
};
use anyhow::{Result, anyhow};
use dashmap::DashMap;
use log::{error, warn};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::mpsc, time};
use tokio_util::sync::CancellationToken;

const FLUSH_CHUNK_SIZE: usize = 50;
const CHANNEL_BUFFER_SIZE: usize = 1;

#[derive(Clone, Debug)]
pub(crate) struct PartitionState {
    pub last_write_time: Instant,
    pub write_count: u32,
    pub flush_state: FlushState,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum FlushState {
    Hot,   // 高频写入分区
    Warm,  // 近期写入分区
    Cold,  // 空闲分区
    Stale, // 需要强制刷盘的分区
}

type FlushUnitPartitionWriterBuffer = (bool, Vec<Arc<PartitionWriterBuffer>>);
type FlushUnitWriterPositionPtr = (bool, Vec<Arc<WriterPositionPtr>>);

#[derive(Clone)]
pub(crate) struct Flusher {
    stop: CancellationToken,

    pub(crate) metrics: StorageWriterMetrics,
    pub(crate) partition_states: Arc<DashMap<PathBuf, PartitionState>>,
    pub(crate) partition_index_manager: PartitionIndexManager,

    partition_writer_buffers: Arc<DashMap<PathBuf, Arc<PartitionWriterBuffer>>>,
    partition_writer_ptrs: Arc<DashMap<PathBuf, Arc<WriterPositionPtr>>>,
    interval: Duration,

    // tasks 任务池
    partition_writer_buffer_tasks_num: usize,
    // 存储 PartitionWriterBuffer 异步刷盘任务的 sender
    partition_writer_buffer_tasks: Arc<Vec<mpsc::Sender<FlushUnitPartitionWriterBuffer>>>,

    partition_writer_ptr_tasks_num: usize,
    // 存储 PartitionWriterPtr 异步刷盘任务的 sender
    partition_writer_ptr_tasks: Arc<Vec<mpsc::Sender<FlushUnitWriterPositionPtr>>>,

    partition_meta_tasks_num: usize,
    // 存储 MessageMetas 异步刷盘任务的 sender
    partition_meta_tasks: Arc<Vec<mpsc::Sender<(bool, String, u32, Vec<MessageMeta>)>>>,
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
    pub(crate) fn new(
        stop: CancellationToken,
        partition_index_manager: PartitionIndexManager,
        partition_writer_buffer_tasks_num: usize,
        partition_writer_ptr_tasks_num: usize,
        partition_meta_tasks_num: usize,
        interval: Duration,
        with_metrics: bool,
    ) -> Self {
        let mut ptr_tasks = vec![];
        for i in 0..partition_writer_ptr_tasks_num {
            let _stop = stop.clone();
            let (tx, mut rx) =
                mpsc::channel::<(bool, Vec<Arc<WriterPositionPtr>>)>(CHANNEL_BUFFER_SIZE);
            tokio::spawn(async move {
                loop {
                    if rx.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            warn!("Flusher: partition_writer_ptr_tasks[{i}] receive stop signal, exit.");
                            break;
                        }

                        res = rx.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (fsync, pwps) = res.unwrap();
                            for pwp in pwps {
                                if let Err(e) = pwp.save(fsync).await {
                                    error!("pwp[{:?}].save_to err: {e:?}", pwp.get_filename().await);
                                }
                            }
                        }
                    }
                }
            });
            ptr_tasks.push(tx);
        }

        let mut meta_tasks = vec![];
        for i in 0..partition_meta_tasks_num {
            let _stop = stop.clone();
            let _partition_index_manager = partition_index_manager.clone();
            let (tx, mut rx) =
                mpsc::channel::<(bool, String, u32, Vec<MessageMeta>)>(CHANNEL_BUFFER_SIZE);
            tokio::spawn(async move {
                loop {
                    if rx.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            warn!("Flusher: partition_meta_tasks[{i}] receive stop signal, exit.");
                            break;
                        }

                        res = rx.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (fsync, topic, partition_id, mms) = res.unwrap();
                            if let Ok(index) = _partition_index_manager
                                .get_or_create(&topic, partition_id)
                                .await{
                                if let Err(e) = index.batch_put(partition_id, &mms){
                                    error!("partition_index[{}-{}].batch_put err: {e:?}", topic, partition_id);
                                }
                            }
                        }
                    }
                }
            });
            meta_tasks.push(tx);
        }

        let meta_tasks = Arc::new(meta_tasks);

        let metrics = StorageWriterMetrics::default();
        let mut buffer_tasks = vec![];
        for i in 0..partition_writer_buffer_tasks_num {
            let _stop = stop.clone();
            let _meta_tasks = meta_tasks.clone();
            let _metrics = metrics.clone();
            let (tx, mut rx) =
                mpsc::channel::<(bool, Vec<Arc<PartitionWriterBuffer>>)>(CHANNEL_BUFFER_SIZE);
            tokio::spawn(async move {
                loop {
                    if rx.is_closed() {
                        break;
                    }
                    select! {
                        _ = _stop.cancelled() => {
                            warn!("Flusher: partition_writer_buffer_tasks[{i}] receive stop signal, exit.");
                            break;
                        }

                        res = rx.recv() => {
                            if res.is_none(){
                                continue;
                            }
                            let (fsync, pwbs) = res.unwrap();
                            let mut flush_count = 0;
                            if with_metrics {
                                _metrics.update_min_start_timestamp();
                            }
                            for pwb in pwbs {
                                match pwb.flush(false, fsync).await {
                                    Ok((num, mms)) => {
                                        flush_count += 1;
                                        if with_metrics {
                                            _metrics.inc_flush_bytes(num);
                                        }

                                        // 将 mms 随机分发至后端 task 进行处理
                                        let idx = rand::random::<u32>() as usize;
                                        if let Err(e) = _meta_tasks[idx % partition_meta_tasks_num]
                                            .send((fsync, pwb.topic.clone(), pwb.partition_id, mms))
                                            .await{
                                                error!("Flusher: partition_meta[{}] send err: {e:?}", idx % partition_meta_tasks_num);
                                            }
                                    },
                                    Err(e) => {
                                        error!("Flusher: partition_writer_buffer_tasks[{i}] flush err: {e:?}");
                                    }
                                }
                            }
                            if with_metrics {
                                _metrics.inc_flush_count(flush_count);
                                _metrics.update_max_end_timestamp();
                            }
                        }
                    }
                }
            });
            buffer_tasks.push(tx);
        }

        Self {
            stop,
            partition_writer_buffers: Arc::new(DashMap::new()),
            partition_states: Arc::default(),
            interval,
            partition_index_manager,
            partition_writer_ptrs: Arc::default(),
            partition_writer_buffer_tasks_num,
            partition_writer_buffer_tasks: Arc::new(buffer_tasks),
            partition_writer_ptr_tasks_num,
            partition_writer_ptr_tasks: Arc::new(ptr_tasks),
            partition_meta_tasks_num,
            partition_meta_tasks: meta_tasks,
            metrics,
            // partition_writer_ptr_tasks: Arc::default(),
        }
    }

    pub(crate) async fn run(&self, mut flush_signal: mpsc::Receiver<()>) {
        let mut state_updater = tokio::time::interval(Duration::from_secs(5));
        let mut hot_ticker = time::interval(self.interval);
        let mut warm_ticker = time::interval(self.interval * 10 * 10);
        let mut cold_ticker = time::interval(self.interval * 10 * 10 * 2);
        loop {
            select! {
                _ = hot_ticker.tick() => {
                    self.flush_by_state(FlushState::Hot, false);
                }

                _ = warm_ticker.tick() => {
                    self.flush_by_state(FlushState::Warm, false);
                    self.flush_partition_writer_ptr(false).await;
                }

                _ = cold_ticker.tick() => {
                    self.flush_by_state(FlushState::Cold, false);
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

    // 添加分区移除方法
    pub(crate) fn remove_partition_writer(&self, path: &PathBuf) {
        // 从所有相关集合中移除分区
        self.partition_writer_buffers.remove(path);
        self.partition_states.remove(path);
        self.partition_writer_ptrs.remove(path);

        log::debug!("Removed partition from flusher: {:?}", path);
    }

    #[inline]
    pub(crate) fn add_partition_writer(&self, key: PathBuf, writer: PartitionWriterBuffer) {
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
    pub(crate) fn add_partition_writer_ptr(&self, partition: PathBuf, ptr: Arc<WriterPositionPtr>) {
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
    pub(crate) fn update_partition_write_count(&self, p: &PathBuf, write_count: u32) {
        if let Some(mut state) = self.partition_states.get_mut(p) {
            state.last_write_time = Instant::now();
            state.write_count += write_count;
            state.flush_state = Self::calculate_flush_state(state.last_write_time);
        }
    }

    pub(crate) fn calculate_flush_state(last_write_time: Instant) -> FlushState {
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

    // 刷指定的 topic-parition 消息文件
    pub(crate) async fn flush_topic_partition(&self, p: &PathBuf, fsync: bool) -> Result<()> {
        let pwb = self.partition_writer_buffers.get(p);
        if pwb.is_none() {
            return Err(anyhow!(
                StorageError::PartitionNotFound("Flusher".to_string()).to_string()
            ));
        }
        let pwb = pwb.unwrap();
        let idx = rand::random::<u32>() as usize;
        self.partition_writer_buffer_tasks[idx % self.partition_writer_buffer_tasks_num]
            .send((fsync, vec![pwb.value().clone()]))
            .await?;
        Ok(())
    }

    // 刷指定的 topic-parition 消息文件的写指针文件
    pub(crate) async fn flush_topic_partition_writer_ptr(
        &self,
        p: &PathBuf,
        fsync: bool,
    ) -> Result<()> {
        let wpp = self.partition_writer_ptrs.get(p);
        if wpp.is_none() {
            return Err(anyhow!(
                StorageError::PartitionNotFound("Flusher".to_string()).to_string()
            ));
        }

        let wpp = wpp.unwrap();
        let idx = rand::random::<u32>() as usize;
        self.partition_writer_ptr_tasks[idx % self.partition_writer_buffer_tasks_num]
            .send((fsync, vec![wpp.value().clone()]))
            .await?;
        Ok(())
    }

    // fsync = false 时，交给操作系统刷脏页落盘
    async fn flush_partition_writer_ptr(&self, fsync: bool) {
        // 处理 PartitionWriterPtr
        let partition_writer_ptrs: Vec<Arc<WriterPositionPtr>> = self
            .partition_writer_ptrs
            .iter()
            .map(|e| e.value().clone())
            .collect();

        let chunks = partition_writer_ptrs.chunks(FLUSH_CHUNK_SIZE);
        for chunk in chunks {
            let idx = rand::random::<u32>() as usize;
            if let Err(e) = self.partition_writer_ptr_tasks
                [idx % self.partition_writer_ptr_tasks_num]
                .try_send((fsync, chunk.to_vec()))
            {
                error!("Flusher: flush_all send PartitionWriterPtr err: {e:?}")
            }
        }
    }

    // fsync = false 时，交给操作系统刷脏页落盘
    async fn graceful_flush_all(&self, fsync: bool) {
        // 第一级：刷热点分区 (最近10秒内有写入)
        self.flush_by_state(FlushState::Hot, fsync);

        // 第二级：刷温分区 (最近60秒内有写入)
        self.flush_by_state(FlushState::Warm, fsync);

        // 第三级：刷过期分区 (超过60秒未刷)
        self.flush_by_state(FlushState::Stale, fsync);
    }

    // fsync = false 时，交给操作系统刷脏页落盘
    fn flush_by_state(&self, state: FlushState, fsync: bool) {
        let writers: Vec<Arc<PartitionWriterBuffer>> = self
            .partition_writer_buffers
            .iter()
            .filter(|entry| {
                if let Some(state_entry) = self.partition_states.get(entry.key()) {
                    return state_entry.flush_state == state;
                }
                false
            })
            .map(|entry| entry.value().clone())
            .collect();

        // 每个task最大批次处理 FLUSH_CHUNK_SIZE 大小的刷盘任务
        let chunks = writers.chunks(FLUSH_CHUNK_SIZE);
        for chunk in chunks {
            /* 计算任务索引 */
            let idx = rand::random::<u32>() as usize;
            if let Err(e) = self.partition_writer_buffer_tasks
                [idx % self.partition_writer_buffer_tasks_num]
                .try_send((fsync, chunk.to_vec()))
            // 发送不成功就进入下次循环发送，不阻塞
            {
                error!("Flush error: {:?}", e);
            }
        }
    }

    pub(crate) async fn flush_metas(
        &self,
        fsync: bool,
        topic: String,
        partition_id: u32,
        mms: Vec<MessageMeta>,
    ) -> Result<()> {
        let idx = rand::random::<u32>() as usize;
        self.partition_meta_tasks[idx % self.partition_meta_tasks_num]
            .send((fsync, topic.to_string(), partition_id, mms))
            .await?;
        Ok(())
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
