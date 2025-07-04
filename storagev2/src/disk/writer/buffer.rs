use crate::disk::{
    Config as DiskConfig,
    fd_cache::{FileHandlerWriterAsync, create_writer_fd, create_writer_fd_with_prealloc},
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
    io::{IoSlice, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncSeekExt as _, AsyncWriteExt};
// use tokio_uring::fs::File as UringFile;

const BATCH_POP_SIZE: usize = 36;

struct SwitchQueue {
    switcher: Arc<AtomicBool>,
    queue_a: Arc<SegQueue<(Bytes, Bytes)>>,
    queue_b: Arc<SegQueue<(Bytes, Bytes)>>,
}

impl SwitchQueue {
    const ACQUIRE_ORDER: Ordering = Ordering::Acquire;
    const RELEASE_ORDER: Ordering = Ordering::Release;
    const RELAXED_ORDER: Ordering = Ordering::Relaxed;

    fn new() -> Self {
        Self {
            switcher: Arc::new(AtomicBool::new(false)),
            queue_a: Arc::new(SegQueue::new()),
            queue_b: Arc::new(SegQueue::new()),
        }
    }

    #[inline(always)]
    fn push(&self, data: (Bytes, Bytes)) {
        let current = self.switcher.load(Self::RELAXED_ORDER);
        let queue = if current {
            &self.queue_b
        } else {
            &self.queue_a
        };
        queue.push(data);
    }

    // 高性能批量弹出
    fn pop_batch(&self, batch_size: usize) -> Vec<(Bytes, Bytes)> {
        let mut results = Vec::with_capacity(batch_size);
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 优先处理活跃队列
        while let Some(item) = active_queue.pop() {
            results.push(item);
            if results.len() >= batch_size {
                return results;
            }
        }

        // 活跃队列空时检查非活跃队列
        if !inactive_queue.is_empty() {
            // 原子切换队列
            self.switcher
                .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER)
                .ok(); // 不关心是否切换成功

            // 处理新活跃队列
            let new_active = if current {
                &self.queue_a
            } else {
                &self.queue_b
            };
            while let Some(item) = new_active.pop() {
                results.push(item);
                if results.len() >= batch_size {
                    break;
                }
            }
        }

        results
    }

    // 弹出全部元素
    fn pop_all(&self) -> Vec<(Bytes, Bytes)> {
        let mut results = Vec::new();
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 清空活跃队列
        while let Some(item) = active_queue.pop() {
            results.push(item);
        }

        // 尝试切换并处理新队列
        if self
            .switcher
            .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER)
            .is_ok()
        {
            let new_active = if current {
                &self.queue_a
            } else {
                &self.queue_b
            };
            while let Some(item) = new_active.pop() {
                results.push(item);
            }
        }

        results
    }
}

#[derive(Clone)]
pub(crate) struct PartitionWriterBuffer {
    pub(crate) dir: PathBuf,
    conf: Arc<DiskConfig>,

    // 当前写的文件因子：用于构成写入的目标文件
    current_factor: Arc<AtomicU64>,

    #[cfg(not(target_os = "linux"))]
    pub(crate) current_fd: FileHandlerWriterAsync,

    #[cfg(target_os = "linux")]
    pub(crate) current_fd: Arc<RwLock<UringFile>>,

    // 先将数据存入内存缓冲区
    pub(crate) buffer: Arc<SwitchQueue>,

    // 写相关的位置指针
    pub(crate) write_ptr: Arc<WriterPositionPtr>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,

    flusher: Arc<Flusher>,
}

impl PartitionWriterBuffer {
    pub(crate) async fn new(
        dir: PathBuf,
        conf: Arc<DiskConfig>,
        write_ptr: Arc<WriterPositionPtr>,
        flusher: Arc<Flusher>,
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

        let max_size_per_file = if conf.partition_writer_prealloc {
            conf.max_size_per_file
        } else {
            0
        };

        let current_fd = Self::get_current_fd(&dir, max_file, write_ptr.get_flush_offset()).await?;

        Ok(Self {
            dir: dir.clone(),
            current_factor: Arc::new(factor),
            #[cfg(not(target_os = "linux"))]
            current_fd,
            conf: conf.clone(),
            buffer: Arc::new(SwitchQueue::new()),
            write_ptr,
            has_create_next_record_file: Arc::default(),
            flusher,
            // metrics: Arc::new(BufferFlushMetrics {
            //     min_start_timestamp: Arc::new(AtomicU64::new(u64::MAX)),
            //     ..Default::default()
            // }),
        })
    }

    #[cfg(not(target_os = "linux"))]
    async fn get_current_fd(
        dir: &PathBuf,
        max_file: u64,
        offset: u64,
    ) -> Result<FileHandlerWriterAsync> {
        let current_fd = FileHandlerWriterAsync::new(create_writer_fd(
            &dir.join(gen_record_filename(max_file)),
        )?);

        // PartitionWriterBuffer 在初始化时就设置好 current_fd 的写位置
        current_fd
            .write()
            .await
            .seek(SeekFrom::Start(offset))
            .await?;

        Ok(current_fd)
    }

    #[cfg(target_os = "linux")]
    async fn get_current_fd(dir: &PathBuf, max_file: u64, offset: u64) -> Result<UringFile> {
        let mut std_fd = create_writer_fd(&dir.join(gen_record_filename(max_file)))?
            .into_std()
            .await;
        std_fd.seek(SeekFrom::Start(offset))?;
        let uring_fd = UringFile::from_std(std_fd);
        Ok(Arc::new(RwLock::new(uring_fd)))
    }

    async fn rotate_file(&self) {
        self.current_factor.fetch_add(1, Ordering::Relaxed);
        let next_filename = self.dir.join(gen_record_filename(
            self.current_factor.load(Ordering::Relaxed),
        ));

        let max_size_per_file = if self.conf.partition_writer_prealloc {
            self.conf.max_size_per_file
        } else {
            0
        };

        match create_writer_fd_with_prealloc(&next_filename, max_size_per_file) {
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
    pub(crate) async fn write_batch(&self, batch: &[Bytes]) -> Result<u64> {
        if batch.is_empty() {
            return Ok(0);
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
            if self.conf.with_metrics {
                self.flusher.metrics.update_min_start_timestamp();
            }
            let flush_bytes = self.flush(true).await?;
            if self.conf.with_metrics {
                self.flusher.metrics.inc_flush_count(1);
                self.flusher.metrics.inc_flush_bytes(flush_bytes);
                self.flusher.metrics.update_max_end_timestamp();
            }
            self.rotate_file().await;
        }

        for data in batch {
            // 这里将 (消息头,消息体) 单次调用 push 写入，否则有并发问题
            // 创建消息头
            let header = Bytes::from_owner(data.len().to_be_bytes());
            // 存储消息头和消息体（零拷贝）
            self.buffer.push((header, data.clone())); // data.clone() 是引用计数增量，非内存拷贝
        }
        {
            // 更新指针（减少锁次数）
            self.write_ptr.rotate_offset(total_size);
            self.write_ptr.rotate_current_count(count as u64);
        }

        // 检查是否需要立即刷盘
        if self.conf.flusher_factor != 0 && total_size > self.conf.flusher_factor {
            if self.conf.with_metrics {
                self.flusher.metrics.update_min_start_timestamp();
            }
            let flush_bytes = self.flush(false).await?;
            if self.conf.with_metrics {
                self.flusher.metrics.inc_flush_count(1);
                self.flusher.metrics.inc_flush_bytes(flush_bytes);
                self.flusher.metrics.update_max_end_timestamp();
            }
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
        Ok(total_size)
    }

    // 将数据从内存刷盘
    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn flush(&self, fsync: bool) -> Result<u64> {
        // 批量获取数据
        let batch = self.buffer.pop_batch(BATCH_POP_SIZE);
        if batch.is_empty() {
            return Ok(0);
        }

        // 准备IO向量
        let mut iovecs = Vec::with_capacity(batch.len() * 2);
        for (header, data) in batch {
            iovecs.extend_from_slice(&[header, data]);
        }
        // 执行批量写入
        let slices: Vec<IoSlice> = iovecs.iter().map(|v| IoSlice::new(v)).collect();
        let mut fd_wl = self.current_fd.write().await;
        let flushed_bytes = fd_wl.write_vectored(&slices).await? as u64;

        // 更新刷盘位置
        self.write_ptr.rotate_flush_offset(flushed_bytes);
        // 批量同步
        if fsync {
            let fd = self.current_fd.clone();
            tokio::spawn(async move {
                let _ = fd.write().await.sync_data().await;
            });
        }

        Ok(flushed_bytes)
    }

    // 将数据从内存刷盘
    #[cfg(target_os = "linux")]
    pub(crate) async fn flush(&self, fsync: bool) -> Result<()> {
        let start = std::time::Instant::now();
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut flushed_bytes = 0_u64;
        let mut datas = Vec::with_capacity(self.buffer.len());
        // 数据零拷贝
        while let Some(data) = self.buffer.pop() {
            flushed_bytes += data.len() as u64;
            datas.push(data);
        }
        if flushed_bytes == 0 {
            return Ok(());
        }

        let slices: Vec<IoSlice> = datas.iter().map(|v| IoSlice::new(v.as_ref())).collect();

        let pos: u64 = self.write_ptr.get_flush_offset();
        self.current_fd
            .write()
            .await
            .writev_at_all(slices, Some(pos))
            .await;
        // 更新刷盘位置
        self.write_ptr.rotate_flush_offset(flushed_bytes);
        // 批量同步
        if fsync {
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
