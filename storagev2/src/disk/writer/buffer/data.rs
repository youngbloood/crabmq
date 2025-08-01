/*!
 * 消息数据缓冲区: 每个 partition 对应一个 PartitionWriterBuffer
 */

use crate::{
    MessageMeta, MessagePayload,
    disk::{
        Config as DiskConfig,
        fd_cache::{FileHandlerWriterAsync, create_writer_fd, create_writer_fd_with_prealloc},
        meta::{WriterPositionPtr, gen_record_filename},
        writer::{
            buffer::{BufferFlushable, parse_topic_partition_from_dir, switch_queue::SwitchQueue},
            flusher::Flusher,
        },
    },
};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use common::dir_recursive;
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

#[derive(Clone)]
pub(crate) struct PartitionWriterBuffer {
    pub(crate) dir: PathBuf,
    pub(crate) topic: String,
    pub(crate) partition_id: u32,
    conf: Arc<DiskConfig>,

    // 当前写的文件因子：用于构成写入的目标文件
    current_segment: Arc<AtomicU64>,

    #[cfg(not(target_os = "linux"))]
    pub(crate) current_fd: FileHandlerWriterAsync,

    #[cfg(target_os = "linux")]
    pub(crate) current_fd: Arc<RwLock<UringFile>>,

    // 先将数据存入内存缓冲区
    pub(crate) queue: Arc<SwitchQueue<(Bytes, Bytes)>>,

    // 写相关的位置指针
    pub(crate) write_ptr: Arc<WriterPositionPtr>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,

    flusher: Arc<Flusher>,
}

#[async_trait::async_trait]
impl BufferFlushable for PartitionWriterBuffer {
    // 将数据从内存刷盘
    #[cfg(not(target_os = "linux"))]
    async fn flush(&self, all: bool, fsync: bool) -> Result<u64> {
        // 批量获取数据
        let batch;
        if all {
            batch = self.queue.pop_all();
        } else {
            batch = self
                .queue
                .pop_batch(self.conf.batch_pop_size_from_buffer as usize);
        }

        if batch.is_empty() {
            return Ok(0);
        }

        // 准备IO向量
        let mut iovecs = Vec::with_capacity(batch.len() * 2);
        // let mut mms = Vec::with_capacity(batch.len());
        for (header, data) in batch {
            iovecs.extend_from_slice(&[header, data]);
            // mms.push(mm);
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

        // let (topic, partition_id) = self.parse_topic_partition_from_dir().unwrap();
        // let index = get_global_partition_index()
        //     .get_or_create(&topic, partition_id)
        //     .await?;
        // index.batch_put(partition_id, &mms)?;

        Ok(flushed_bytes)
    }

    async fn is_dirty(&self) -> bool {
        self.queue.is_dirty()
    }
}

impl PartitionWriterBuffer {
    pub(crate) async fn new(
        dir: PathBuf,
        conf: Arc<DiskConfig>,
        write_ptr: Arc<WriterPositionPtr>,
        flusher: Arc<Flusher>,
    ) -> Result<Self> {
        let tp = parse_topic_partition_from_dir(&dir);
        if tp.is_none() {
            return Err(anyhow!("not found topic and partition_id in dir"));
        }
        let tp = tp.unwrap();

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
            topic: tp.0,
            partition_id: tp.1,
            current_segment: Arc::new(factor),
            #[cfg(not(target_os = "linux"))]
            current_fd,
            conf: conf.clone(),
            queue: Arc::new(SwitchQueue::new()),
            write_ptr,
            has_create_next_record_file: Arc::default(),
            flusher,
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
        self.current_segment.fetch_add(1, Ordering::Relaxed);
        let next_filename = self.dir.join(gen_record_filename(
            self.current_segment.load(Ordering::Relaxed),
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
            self.current_segment.load(Ordering::Relaxed) + 1,
        ))
    }

    pub(crate) async fn write_batch_with_metas_and_rotation(
        &self,
        batch: Vec<MessagePayload>,
    ) -> Result<(u64, Vec<MessageMeta>, bool)> {
        if batch.is_empty() {
            return Ok((0, Vec::new(), false));
        }

        let batch_len = batch.len();
        self.update_partition_state(batch_len);

        let mut now_ptr_offset = self.write_ptr.get_offset();
        let mut now_batch_total_size = 0;
        let mut now_count = self.write_ptr.get_current_count();
        let mut message_metas = Vec::with_capacity(batch_len);
        let mut did_rotate = false;

        for data in batch {
            let bts = data.to_bytes()?;
            let should_rotate = {
                now_ptr_offset + (bts.len() as u64) > self.conf.max_size_per_file
                    || now_count + 1 >= self.conf.max_msg_num_per_file
            };

            if should_rotate {
                did_rotate = true;
                if self.conf.with_metrics {
                    self.flusher.metrics.update_data_min_start_timestamp();
                }
                let flush_bytes = self.flush(true, false).await?;
                if self.conf.with_metrics {
                    self.flusher.metrics.inc_data_flush_count(1, 0);
                    self.flusher.metrics.inc_flush_bytes(flush_bytes, 0);
                    self.flusher.metrics.update_data_max_end_timestamp();
                }

                self.rotate_file().await;
                // 重置位置信息
                now_ptr_offset = self.write_ptr.get_offset();
                now_count = self.write_ptr.get_current_count();
            }

            // 生成消息元数据（包含文件段、偏移量等信息）
            let mm = data.gen_meta(self.current_segment.load(Ordering::Relaxed), now_ptr_offset);
            message_metas.push(mm);

            now_batch_total_size += 8 + bts.len() as u64;
            now_ptr_offset += 8 + bts.len() as u64;
            now_count += 1;

            // 创建消息头并存储到缓冲区
            let header = Bytes::from_owner(bts.len().to_be_bytes());
            self.queue.push((header, bts));
        }

        {
            // 更新指针（减少锁次数）
            self.write_ptr.rotate_offset(now_batch_total_size);
            self.write_ptr.rotate_current_count(batch_len as u64);
        }

        // 检查是否需要立即刷盘
        if self.conf.flusher_factor != 0 && now_batch_total_size > self.conf.flusher_factor {
            if self.conf.with_metrics {
                self.flusher.metrics.update_data_min_start_timestamp();
            }
            let flush_bytes = self.flush(true, false).await?;
            if self.conf.with_metrics {
                self.flusher.metrics.inc_data_flush_count(1, 0);
                self.flusher.metrics.inc_flush_bytes(flush_bytes, 0);
                self.flusher.metrics.update_data_max_end_timestamp();
            }
            self.flusher.flush_metas(false, &self.dir).await?;
        }

        Ok((now_batch_total_size, message_metas, did_rotate))
    }

    pub(crate) async fn write_batch_with_metas(
        &self,
        batch: Vec<MessagePayload>,
    ) -> Result<(u64, Vec<MessageMeta>)> {
        let (bytes, metas, _) = self.write_batch_with_metas_and_rotation(batch).await?;
        Ok((bytes, metas))
    }

    pub(crate) async fn write_batch(&self, batch: Vec<MessagePayload>) -> Result<u64> {
        let (bytes, _) = self.write_batch_with_metas(batch).await?;
        Ok(bytes)
    }

    // 将数据从内存刷盘
    #[cfg(not(target_os = "linux"))]
    pub(crate) async fn flush(&self, all: bool, fsync: bool) -> Result<u64> {
        // 批量获取数据
        let batch = if all {
            self.queue.pop_all()
        } else {
            self.queue
                .pop_batch(self.conf.batch_pop_size_from_buffer as usize)
        };

        if batch.is_empty() {
            return Ok(0);
        }

        // 准备IO向量
        let mut iovecs = Vec::with_capacity(batch.len() * 2);
        // let mut mms = Vec::with_capacity(batch.len());
        for (header, data) in batch {
            iovecs.extend_from_slice(&[header, data]);
            // mms.push(mm);
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

        // let (topic, partition_id) = self.parse_topic_partition_from_dir().unwrap();
        // let index = get_global_partition_index()
        //     .get_or_create(&topic, partition_id)
        //     .await?;
        // index.batch_put(partition_id, &mms)?;

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
