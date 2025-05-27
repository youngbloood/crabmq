use super::{
    meta::{PartitionMeta, TopicMeta},
    writer::PartitionWriter,
};
use dashmap::DashMap;
use log::error;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{RwLock, mpsc},
    time,
};

#[derive(Clone)]
pub struct Flusher {
    writers: Arc<DashMap<PathBuf, Arc<PartitionWriter>>>,
    topic_metas: Arc<DashMap<PathBuf, TopicMeta>>,
    partition_metas: Arc<DashMap<PathBuf, PartitionMeta>>,
    interval: Duration,
}

impl Flusher {
    pub fn new(interval: Duration) -> Self {
        Self {
            writers: Arc::new(DashMap::new()),
            interval,
            topic_metas: Arc::default(),
            partition_metas: Arc::default(),
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

    pub fn add_partition_writer(&self, key: PathBuf, writer: Arc<PartitionWriter>) {
        self.writers.insert(key, writer);
    }

    pub fn add_topic_meta(&self, topic: PathBuf, meta: TopicMeta) {
        self.topic_metas.insert(topic, meta);
    }

    pub fn add_partition_meta(&self, partition: PathBuf, meta: PartitionMeta) {
        self.partition_metas.insert(partition, meta);
    }

    async fn flush_all(&self) {
        for entry in self.writers.iter() {
            let writer = entry.value();
            if let Err(e) = writer.flush().await {
                error!("Flush failed: {}", e);
            }
        }

        for entry in self.topic_metas.iter() {
            let meta = entry.value();
            let filename = entry.key();
            if let Err(e) = meta.save(filename).await {
                error!("Flush topic_metas [{filename:?}] failed: {e:?}");
            }
        }

        for entry in self.partition_metas.iter() {
            let meta = entry.value();
            let filename = entry.key();
            if let Err(e) = meta.save(filename).await {
                error!("Flush partition_metas [{filename:?}] failed: {e:?}");
            }
        }
    }
}
