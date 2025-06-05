use crate::disk::{
    Config,
    fd_cache::{FileWriterHandlerAsync, create_writer_fd_with_prealloc},
    meta::{WriterPositionPtr, gen_record_filename},
    writer::flusher::Flusher,
};
use anyhow::Result;
use bytes::Bytes;
use common::dir_recursive;
use crossbeam::queue::SegQueue;
use log::error;
use std::{
    ffi::OsString,
    io::SeekFrom,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt as _};

#[derive(Clone)]
pub struct PartitionWriterBuffer {
    dir: PathBuf,
    conf: Config,

    // 当前写的文件因子：用于构成写入的目标文件
    current_factor: Arc<AtomicU64>,

    current_fd: FileWriterHandlerAsync,

    // 先将数据存入内存缓冲区
    buffer: Arc<SegQueue<Bytes>>,

    // 写相关的位置指针
    write_ptr: Arc<WriterPositionPtr>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,

    flusher: Flusher,

    // 监控刷盘指标
    metrics: Arc<BufferFlushMetrics>,
}

impl PartitionWriterBuffer {
    pub async fn new(
        dir: PathBuf,
        conf: &Config,
        write_ptr: Arc<WriterPositionPtr>,
        flusher: Flusher,
    ) -> Result<Self> {
        let record_files = dir_recursive(dir.clone(), &[OsString::from("record")])?;
        // 只获取最大编号的文件
        let max_file = record_files
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.split('.').next())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max();
        let (factor, mut max_file) = if let Some(max) = max_file {
            (AtomicU64::new(max), max)
        } else {
            (AtomicU64::new(0), 0)
        };

        let max_record_filename = dir.join(gen_record_filename(max_file));
        // 判断 WriterPositionPtr.filename 与最大 record文件 不一致的情况
        // 此时表示文件可能为外界条件破坏。以下一个 record 文件重新开始
        if !write_ptr.get_filename().await.eq(&max_record_filename) {
            max_file += 1;
            write_ptr
                .reset_with_filename(dir.join(gen_record_filename(max_file)))
                .await;
            factor.fetch_add(1, Ordering::Relaxed);
        }

        Ok(Self {
            dir: dir.clone(),
            current_factor: Arc::new(factor),
            current_fd: FileWriterHandlerAsync::new(create_writer_fd_with_prealloc(
                &dir.join(gen_record_filename(max_file)),
                conf.max_size_per_file,
            )?),
            conf: conf.clone(),
            buffer: Arc::new(SegQueue::new()),
            write_ptr,
            has_create_next_record_file: Arc::default(),
            flusher,
            metrics: Arc::new(BufferFlushMetrics {
                min_start_timestamp: Arc::new(AtomicU64::new(u64::MAX)),
                ..Default::default()
            }),
        })
    }

    async fn rotate_file(&self) {
        self.current_factor.fetch_add(1, Ordering::Relaxed);
        let next_filename = self.dir.join(gen_record_filename(
            self.current_factor.load(Ordering::Relaxed),
        ));

        match create_writer_fd_with_prealloc(&next_filename, self.conf.max_size_per_file) {
            Ok(async_file) => {
                self.current_fd.reset(async_file).await;
                self.write_ptr.reset_with_filename(next_filename).await;
            }
            Err(e) => {
                error!("create_writer_fd err: {e:?}");
            }
        }

        // writer_ptr_wl.next_filename = PathBuf::new();
        // writer_ptr_wl.next_offset = 0;
        self.has_create_next_record_file
            .store(false, Ordering::Relaxed);
    }

    #[inline]
    async fn get_next_filename(&self) -> PathBuf {
        self.dir.join(gen_record_filename(
            self.current_factor.load(Ordering::Relaxed) + 1,
        ))
    }

    // 写入到 buffer 中
    pub async fn write_batch(&self, batch: &[Bytes]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.update_partition_state(batch.len());
        let count = batch.len();
        // 要加入每个消息的 8 bytes 长度头
        let total_size: u64 = batch.iter().map(|v| v.len() as u64 + 8).sum();
        // 检查是否需要滚动文件
        let should_rotate = {
            self.write_ptr.get_offset() + total_size > self.conf.max_size_per_file
                || self.write_ptr.get_current_count() >= self.conf.max_msg_num_per_file
        };
        if should_rotate {
            self.flush(true).await?;
            self.rotate_file().await;
        }

        for data in batch {
            self.buffer.push(data.clone());
        }

        {
            // 更新指针（减少锁次数）
            self.write_ptr.rotate_offset(total_size);
            self.write_ptr.rotate_current_count(count as u64);
        }

        // 检查是否需要立即刷盘
        if self.conf.flusher_factor != 0 && total_size > self.conf.flusher_factor {
            self.flush(false).await?;
        }

        // 当前文件的使用率已经达到设置阈值，预创建下一个
        // let should_pre_create = {
        //     ((self.write_ptr.get_offset() as f64 / self.conf.max_size_per_file as f64) * 100.0)
        //         as u64
        //         >= self.conf.create_next_record_file_threshold as u64
        //         && self
        //             .has_create_next_record_file
        //             .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        //             .is_ok()
        // };

        // if should_pre_create {
        //     let next_filename = self.get_next_filename().await;
        //     let writer_ptr = self.write_ptr.clone();
        //     if self.fd_cache.get(&next_filename).is_none() {
        //         let _fd_cache = self.fd_cache.clone();
        //         tokio::spawn(async move {
        //             if _fd_cache.get_or_create(&next_filename, true).is_ok() {
        //                 // let mut wl = writer_ptr.write().await;
        //                 // wl.next_filename = next_filename;
        //                 // wl.offset = 0;
        //             }
        //         });
        //     }
        // }
        Ok(())
    }

    // 仅 Flusher::new 中的 tasks 调用
    pub async fn flush(&self, should_sync: bool) -> Result<()> {
        let start = std::time::Instant::now();
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut fd_wl = self.current_fd.write().await;
        // 直接使用队列数据，避免拷贝
        let mut position = self.write_ptr.get_flush_offset();
        fd_wl.seek(SeekFrom::Start(position)).await?;
        let mut flushed_bytes = 0_u64;
        let mut flush_count = 0;
        while let Some(data) = self.buffer.pop() {
            let len = data.len();
            flush_count += 1;
            // 先写入长度头
            fd_wl.write_u64(len as u64).await?;
            position += 8;

            // 直接写入数据（零拷贝）
            fd_wl.write_all(&data).await?;
            position += len as u64;
            flushed_bytes += len as u64 + 8;
        }
        if flushed_bytes == 0 || flush_count == 0 {
            return Ok(());
        }
        // 更新刷盘位置
        self.write_ptr.rotate_flush_offset(flushed_bytes);

        // 批量同步
        if should_sync {
            let fd = self.current_fd.clone();
            tokio::spawn(async move {
                let _ = fd.write().await.sync_data().await;
            });
        }

        // 更新刷盘指标
        if self.conf.with_metrics {
            let elapsed = start.elapsed().as_micros() as u64;
            let end_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;

            self.metrics.flush_count.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .flush_bytes
                .fetch_add(flushed_bytes, Ordering::Relaxed);
            self.metrics
                .flush_latency_us
                .fetch_add(elapsed, Ordering::Relaxed);

            // 记录最早的刷盘时间
            let current_min = self.metrics.min_start_timestamp.load(Ordering::Relaxed);
            if start_timestamp < current_min {
                self.metrics
                    .min_start_timestamp
                    .compare_exchange(
                        current_min,
                        start_timestamp,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .ok();
            }

            // 记录最晚的刷盘时间
            let current_max = self.metrics.max_end_timestamp.load(Ordering::Relaxed);
            if end_timestamp > current_max {
                self.metrics
                    .max_end_timestamp
                    .compare_exchange(
                        current_max,
                        end_timestamp,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .ok();
            }
        }

        Ok(())
    }

    fn update_partition_state(&self, batch_size: usize) {
        self.flusher
            .update_partition_write_count(&self.dir, batch_size as _);
    }
}

impl PartitionWriterBuffer {
    // 添加获取刷盘指标的方法
    pub fn get_metrics(&self) -> Arc<BufferFlushMetrics> {
        self.metrics.clone()
    }

    pub fn reset_metrics(&self) {
        self.metrics.flush_count.store(0, Ordering::Relaxed);
        self.metrics.flush_bytes.store(0, Ordering::Relaxed);
        self.metrics.flush_latency_us.store(0, Ordering::Relaxed);
        self.metrics
            .min_start_timestamp
            .store(u64::MAX, Ordering::Relaxed);
        self.metrics.max_end_timestamp.store(0, Ordering::Relaxed);
    }
}

// 添加监控指标
#[derive(Default)]
pub struct BufferFlushMetrics {
    pub flush_count: AtomicU64,              // 刷盘次数
    pub flush_bytes: AtomicU64,              // 刷盘消息字节数
    pub flush_latency_us: AtomicU64,         // 刷盘耗时微秒数
    pub min_start_timestamp: Arc<AtomicU64>, // 刷盘期间最早开始时间戳（微秒）
    pub max_end_timestamp: Arc<AtomicU64>,   // 刷盘期间最晚结束时间戳（微秒）
}
