use super::buffer::PartitionWriterBuffer;
use crate::disk::{
    fd_cache::FdWriterCacheAync,
    meta::{PartitionWriterPtr, TopicMeta},
};
use dashmap::DashMap;
use log::error;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{select, sync::mpsc, time};

#[derive(Clone)]
pub struct Flusher {
    fd_cache: Arc<FdWriterCacheAync>,
    partition_writer_buffers: Arc<DashMap<PathBuf, PartitionWriterBuffer>>,
    topic_metas: Arc<DashMap<PathBuf, TopicMeta>>,
    partition_writer_ptrs: Arc<DashMap<PathBuf, PartitionWriterPtr>>,
    interval: Duration,

    partition_writer_buffer_tasks_num: usize,
    // 存储 PartitionWriterBuffer 异步刷盘任务的 sender
    partition_writer_buffer_tasks: Arc<Vec<mpsc::Sender<PartitionWriterBuffer>>>,

    partition_writer_ptr_tasks_num: usize,
    // 存储 PartitionWriterPtr 异步刷盘任务的 sender
    partition_writer_ptr_tasks: Arc<Vec<mpsc::Sender<(PathBuf, PartitionWriterPtr)>>>,
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
            let (tx, mut rx) = mpsc::channel::<PartitionWriterBuffer>(1);
            tokio::spawn(async move {
                while let Some(pwb) = rx.recv().await {
                    if let Err(e) = pwb.flush().await {
                        error!("Flusher: partition_writer_buffer_tasks flush err: {e:?}")
                    }
                }
            });
            buffer_tasks.push(tx);
        }

        let mut ptr_tasks = vec![];
        for _ in 0..partition_writer_ptr_tasks_num {
            let (tx, mut rx) = mpsc::channel::<(PathBuf, PartitionWriterPtr)>(1);
            let _fd_cache = fd_cache.clone();
            tokio::spawn(async move {
                while let Some((p, pwp)) = rx.recv().await {
                    match _fd_cache.get_or_create(&p, false) {
                        Ok(fd) => {
                            if let Err(e) = pwp.save_to(fd).await {
                                error!("pwp.save_to err: {e:?}");
                            }
                        }
                        Err(e) => error!("Flusher:  _fd_cache.get_or_create err: {e:?}"),
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
        }
    }

    pub async fn run(&mut self, mut flush_signal: mpsc::Receiver<()>) {
        let mut ticker = time::interval(self.interval);
        loop {
            select! {
                _ = ticker.tick() => {
                    self.flush_all().await;
                }

                _ = flush_signal.recv() => {
                    self.flush_all().await;
                }
            }
        }
    }

    pub fn add_partition_writer(&self, key: PathBuf, writer: PartitionWriterBuffer) {
        self.partition_writer_buffers.insert(key, writer);
    }

    pub fn add_topic_meta(&self, topic: PathBuf, meta: TopicMeta) {
        self.topic_metas.insert(topic, meta);
    }

    pub fn add_partition_meta(&self, partition: PathBuf, meta: PartitionWriterPtr) {
        self.partition_writer_ptrs.insert(partition, meta);
    }

    async fn flush_all(&self) {
        // 获取所有writer的副本，避免长时间持有锁
        let writers: Vec<PartitionWriterBuffer> = self
            .partition_writer_buffers
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for (idx, writer) in writers.iter().enumerate() {
            if let Err(e) = self.partition_writer_buffer_tasks
                [idx % self.partition_writer_buffer_tasks_num]
                .send(writer.clone())
                .await
            {
                error!("Flusher: flush_all send PartitionWriterBuffer err: {e:?}")
            }
        }

        let topic_metas: Vec<(PathBuf, TopicMeta)> = self
            .topic_metas
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (filename, meta) in topic_metas {
            match self.fd_cache.get_or_create(&filename, false) {
                Ok(fd) => {
                    if let Err(e) = meta.save_to(fd).await {
                        error!("Flush topic_metas [{filename:?}] failed: {e:?}");
                    }
                }
                Err(e) => {
                    error!("Flusher: fd_cache.get_or_create err: {e:?}");
                }
            }
        }

        // 处理partition_metas
        let partition_writer_ptrs: Vec<(PathBuf, PartitionWriterPtr)> = self
            .partition_writer_ptrs
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (idx, (filename, pwp)) in partition_writer_ptrs.iter().enumerate() {
            if let Err(e) = self.partition_writer_ptr_tasks
                [idx % self.partition_writer_ptr_tasks_num]
                .send((filename.clone(), pwp.clone()))
                .await
            {
                error!("Flusher: flush_all send PartitionWriterPtr err: {e:?}")
            }
        }
    }
}
