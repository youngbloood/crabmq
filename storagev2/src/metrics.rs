use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct StorageWriterMetrics {
    pub data_flush_count: Arc<AtomicU64>,         // 刷盘次数
    pub data_flush_err_count: Arc<AtomicU64>,     // 刷盘错误次数
    pub data_flush_bytes: Arc<AtomicU64>,         // 刷盘字节数
    pub data_min_start_timestamp: Arc<AtomicU64>, // 刷盘期间最早开始时间戳（微秒）
    pub data_max_end_timestamp: Arc<AtomicU64>,   // 刷盘期间最晚结束时间戳（微秒）

    pub index_flush_count: Arc<AtomicU64>,         // 刷盘次数
    pub index_flush_err_count: Arc<AtomicU64>,     // 刷盘错误次数
    pub index_flush_bytes: Arc<AtomicU64>,         // 刷盘字节数
    pub index_min_start_timestamp: Arc<AtomicU64>, // 刷盘期间最早开始时间戳（微秒）
    pub index_max_end_timestamp: Arc<AtomicU64>,   // 刷盘期间最晚结束时间戳（微秒）
}

impl Default for StorageWriterMetrics {
    fn default() -> Self {
        Self {
            data_flush_count: Default::default(),
            data_flush_err_count: Default::default(),
            data_flush_bytes: Default::default(),
            data_min_start_timestamp: Arc::new(AtomicU64::new(u64::MAX)),
            data_max_end_timestamp: Default::default(),

            index_flush_count: Default::default(),
            index_flush_err_count: Default::default(),
            index_flush_bytes: Default::default(),
            index_min_start_timestamp: Arc::new(AtomicU64::new(u64::MAX)),
            index_max_end_timestamp: Default::default(),
        }
    }
}

impl StorageWriterMetrics {
    pub fn reset(&self) {
        self.data_flush_count.store(0, Ordering::Relaxed);
        self.data_flush_err_count.store(0, Ordering::Relaxed);
        self.data_flush_bytes.store(0, Ordering::Relaxed);
        self.data_min_start_timestamp
            .store(u64::MAX, Ordering::Relaxed);
        self.data_max_end_timestamp.store(0, Ordering::Relaxed);

        self.index_flush_count.store(0, Ordering::Relaxed);
        self.index_flush_err_count.store(0, Ordering::Relaxed);
        self.index_flush_bytes.store(0, Ordering::Relaxed);
        self.index_min_start_timestamp
            .store(u64::MAX, Ordering::Relaxed);
        self.index_max_end_timestamp.store(0, Ordering::Relaxed);
    }

    pub(crate) fn inc_data_flush_count(&self, count: u64, err_count: u64) {
        if count != 0 {
            self.data_flush_count.fetch_add(count, Ordering::Relaxed);
        }
        if err_count != 0 {
            self.data_flush_err_count
                .fetch_add(err_count, Ordering::Relaxed);
        }
    }

    pub(crate) fn inc_index_flush_count(&self, count: u64, err_count: u64) {
        if count != 0 {
            self.index_flush_count.fetch_add(count, Ordering::Relaxed);
        }
        if err_count != 0 {
            self.index_flush_err_count
                .fetch_add(err_count, Ordering::Relaxed);
        }
    }

    pub(crate) fn inc_flush_bytes(&self, data_num: u64, index_num: u64) {
        if data_num != 0 {
            self.data_flush_bytes.fetch_add(data_num, Ordering::Relaxed);
        }
        if index_num != 0 {
            self.index_flush_bytes
                .fetch_add(index_num, Ordering::Relaxed);
        }
    }

    pub(crate) fn update_data_min_start_timestamp(&self) {
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        // 记录最早的刷盘时间
        self.data_min_start_timestamp
            .fetch_min(start_timestamp, Ordering::Relaxed);
    }

    pub(crate) fn update_index_min_start_timestamp(&self) {
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        // 记录最早的刷盘时间
        self.index_min_start_timestamp
            .fetch_min(start_timestamp, Ordering::Relaxed);
    }

    pub(crate) fn update_data_max_end_timestamp(&self) {
        // 记录最晚的刷盘时间
        let end_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        self.data_max_end_timestamp
            .fetch_max(end_timestamp, Ordering::Relaxed);
    }

    pub(crate) fn update_index_max_end_timestamp(&self) {
        // 记录最晚的刷盘时间
        let end_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        self.index_max_end_timestamp
            .fetch_max(end_timestamp, Ordering::Relaxed);
    }
}
