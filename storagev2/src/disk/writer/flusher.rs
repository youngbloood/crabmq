use super::buffer::PartitionWriterBuffer;
use crate::disk::{
    fd_cache::FdWriterCacheAync,
    meta::{TopicMeta, WriterPositionPtr},
};
use dashmap::DashMap;
use log::error;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{select, sync::mpsc, time};

#[derive(Clone)]
pub struct Flusher {
    fd_cache: Arc<FdWriterCacheAync>,
    partition_writer_buffers: Arc<DashMap<PathBuf, Arc<PartitionWriterBuffer>>>,
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
        partition_writer_buffer_tasks_num: usize,
        partition_writer_ptr_tasks_num: usize,
        interval: Duration,
        fd_cache: Arc<FdWriterCacheAync>,
    ) -> Self {
        let mut buffer_tasks = vec![];
        for _ in 0..partition_writer_buffer_tasks_num {
            let (tx, mut rx) = mpsc::channel::<(bool, Arc<PartitionWriterBuffer>)>(1);
            tokio::spawn(async move {
                while let Some((should_sync, pwb)) = rx.recv().await {
                    if let Err(e) = pwb.flush(should_sync).await {
                        error!("Flusher: partition_writer_buffer_tasks flush err: {e:?}")
                    }
                }
            });
            buffer_tasks.push(tx);
        }

        let mut ptr_tasks = vec![];
        for _ in 0..partition_writer_ptr_tasks_num {
            let (tx, mut rx) = mpsc::channel::<(bool, PathBuf, Arc<WriterPositionPtr>)>(1);
            let _fd_cache = fd_cache.clone();
            tokio::spawn(async move {
                while let Some((should_sync, p, pwp)) = rx.recv().await {
                    if let Err(e) = pwp.save_to(should_sync).await {
                        error!("pwp.save_to err: {e:?}");
                    }
                }
            });
            ptr_tasks.push(tx);
        }

        Self {
            fd_cache,
            partition_writer_buffers: Arc::new(DashMap::new()),
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
        let mut ticker = time::interval(self.interval);
        let mut tt = 0_u64;
        loop {
            select! {
                _ = ticker.tick() => {
                    // 每 5s 必须 sync 一次
                    self.flush_all(tt%100==0).await;
                    tt+=1;
                }

                _ = flush_signal.recv() => {
                    self.flush_all(true).await;
                }
            }
        }
    }

    pub fn add_partition_writer(&self, key: PathBuf, writer: PartitionWriterBuffer) {
        self.partition_writer_buffers.insert(key, Arc::new(writer));
    }

    pub fn add_topic_meta(&self, topic: PathBuf, meta: TopicMeta) {
        self.topic_metas.insert(topic, meta);
    }

    pub fn add_partition_meta(&self, partition: PathBuf, meta: Arc<WriterPositionPtr>) {
        self.partition_writer_ptrs.insert(partition, meta);
    }

    async fn flush_all(&self, should_sync: bool) {
        // 获取所有writer的副本，避免长时间持有锁
        let writers: Vec<Arc<PartitionWriterBuffer>> = self
            .partition_writer_buffers
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for (idx, writer) in writers.iter().enumerate() {
            if let Err(e) = self.partition_writer_buffer_tasks
                [idx % self.partition_writer_buffer_tasks_num]
                .send((should_sync, writer.clone()))
                .await
            {
                error!("Flusher: flush_all send PartitionWriterBuffer err: {e:?}")
            }
        }

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
                .send((should_sync, filename.clone(), pwp.clone()))
                .await
            {
                error!("Flusher: flush_all send PartitionWriterPtr err: {e:?}")
            }
        }
    }

    // 添加获取分区指标的方法
    pub fn get_partition_metrics(&self) -> Vec<PartitionMetrics> {
        // if let Some(ts) = self.topics.get(topic) {
        let mut result = vec![];
        for pwb in self.partition_writer_buffers.iter() {
            let metrics = pwb.get_metrics();
            result.push(PartitionMetrics {
                partition_path: pwb.key().clone(),
                flush_offset: Arc::new(AtomicU64::new(
                    metrics.flush_offset.load(Ordering::Relaxed),
                )),
                flush_count: Arc::new(AtomicU64::new(metrics.flush_count.load(Ordering::Relaxed))),
                flush_bytes: Arc::new(AtomicU64::new(metrics.flush_bytes.load(Ordering::Relaxed))),
                flush_latency_us: Arc::new(AtomicU64::new(
                    metrics.flush_latency_us.load(Ordering::Relaxed),
                )),
            });
        }
        result
    }
}

// 添加监控结构体
#[derive(Default, Clone)]
pub struct PartitionMetrics {
    pub partition_path: PathBuf,
    pub flush_offset: Arc<AtomicU64>,
    pub flush_count: Arc<AtomicU64>,
    pub flush_bytes: Arc<AtomicU64>,
    pub flush_latency_us: Arc<AtomicU64>,
}
