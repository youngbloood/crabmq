use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct StorageWriterMetrics {
    pub flush_count: Arc<AtomicU64>,         // 刷盘次数
    pub flush_bytes: Arc<AtomicU64>,         // 刷盘字节数
    pub min_start_timestamp: Arc<AtomicU64>, // 刷盘期间最早开始时间戳（微秒）
    pub max_end_timestamp: Arc<AtomicU64>,   // 刷盘期间最晚结束时间戳（微秒）
}

impl Default for StorageWriterMetrics {
    fn default() -> Self {
        Self {
            flush_count: Default::default(),
            flush_bytes: Default::default(),
            min_start_timestamp: Arc::new(AtomicU64::new(u64::MAX)),
            max_end_timestamp: Default::default(),
        }
    }
}

impl StorageWriterMetrics {
    pub fn reset(&self) {
        self.flush_count.store(0, Ordering::Relaxed);
        self.flush_bytes.store(0, Ordering::Relaxed);
        self.min_start_timestamp.store(u64::MAX, Ordering::Relaxed);
        self.max_end_timestamp.store(0, Ordering::Relaxed);
    }

    pub(crate) fn inc_flush_count(&self, count: u64) {
        self.flush_count.fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn inc_flush_bytes(&self, num: u64) {
        self.flush_bytes.fetch_add(num, Ordering::Relaxed);
    }

    pub(crate) fn update_min_start_timestamp(&self) {
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        // 记录最早的刷盘时间
        self.min_start_timestamp
            .fetch_min(start_timestamp, Ordering::Relaxed);
    }

    pub(crate) fn update_max_end_timestamp(&self) {
        // 记录最晚的刷盘时间
        let end_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        self.max_end_timestamp
            .fetch_max(end_timestamp, Ordering::Relaxed);
    }
}
