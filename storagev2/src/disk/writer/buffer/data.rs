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
    serializer::{MessageSerializer, SgIoSerializer},
};
use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
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
    pub(crate) current_fd: Arc<ArcSwap<FileHandlerWriterAsync>>,

    #[cfg(target_os = "linux")]
    pub(crate) current_fd: Arc<RwLock<UringFile>>,

    // 先将消息存入内存缓冲区 - 在 flush 时才序列化，保持零拷贝
    pub(crate) queue: Arc<SwitchQueue<MessagePayload>>,

    // 写相关的位置指针
    pub(crate) write_ptr: Arc<WriterPositionPtr>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,

    flusher: Arc<Flusher>,

    // 消息序列化器（支持切换rkyv/S-G IO等）
    serializer: Arc<dyn MessageSerializer>,
}

#[async_trait::async_trait]
impl BufferFlushable for PartitionWriterBuffer {
    // 将数据从内存刷盘
    #[cfg(not(target_os = "linux"))]
    async fn flush(&self, all: bool, fsync: bool) -> Result<u64> {
        // 批量获取消息
        let batch = if all {
            self.queue.pop_all()
        } else {
            self.queue
                .pop_batch(self.conf.batch_pop_size_from_buffer as usize)
        };

        if batch.is_empty() {
            return Ok(0);
        }

        // 零拷贝批量写入（真正的单次遍历实现）
        // 核心思路：
        //   1. 预分配稳定存储（all_headers, length_headers）
        //   2. 单次遍历batch，序列化每条消息，保存所有 SerializedMessage
        //   3. 从保存的 SerializedMessage 中提取 IoSlice 引用
        //   4. 一次 write_vectored 批量写入
        // 关键：SerializedMessage 的生命周期依赖 batch 和 all_headers，
        //       只要它们在 write_vectored 前保持存活，IoSlice 引用就有效

        // 1. 预分配稳定存储（不会被移动或释放）
        let mut all_headers: Vec<Vec<Vec<u8>>> = vec![Vec::new(); batch.len()];
        let mut length_headers: Vec<[u8; 8]> = vec![[0u8; 8]; batch.len()];
        let mut all_serialized = Vec::with_capacity(batch.len());
        let mut total_bytes = 0u64;

        // 2. 单次循环：序列化每条消息，立即组装 IoSlice，完成所有准备工作
        // 使用 zip + iter_mut 让 Rust 理解每次借用的是不同元素
        let mut iovecs = Vec::with_capacity(batch.len() * 7);

        for ((msg, headers), length_header) in batch
            .iter()
            .zip(all_headers.iter_mut())
            .zip(length_headers.iter_mut())
        {
            let serialized = self
                .serializer
                .serialize(msg, headers)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;

            // 计算并存储长度头
            *length_header = (serialized.total_len as u64).to_le_bytes();
            total_bytes += 8 + serialized.total_len;

            // 立即组装 IoSlice：先添加长度头，再添加消息数据
            iovecs.push(IoSlice::new(length_header));
            iovecs.extend_from_slice(&serialized.iovecs);

            // 保存 SerializedMessage 以保持生命周期（batch、all_headers、length_headers 都存活）
            all_serialized.push(serialized);
        }

        // 3. 批量写入（根据配置选择写入方式）
        let mut flushed_bytes = 0u64;
        let write_mode = self.conf.disk_write_mode;

        match write_mode {
            crate::disk::DiskWriteMode::WriteVectored => {
                // 零拷贝写入：使用 write_vectored
                // 注意：writev 系统调用受 IOV_MAX 限制（每次调用的最大 IoSlice 数量）
                let iov_max = self.conf.iov_max;

                if iovecs.len() <= iov_max {
                    // 不超过限制，一次写入
                    flushed_bytes = self.current_fd.load().write(&iovecs, write_mode).await? as u64;
                } else {
                    // 超过限制，分批写入
                    for chunk in iovecs.chunks(iov_max) {
                        flushed_bytes +=
                            self.current_fd.load().write(chunk, write_mode).await? as u64;
                    }
                }
            }
            crate::disk::DiskWriteMode::Mmap => {
                // Mmap 写入：将数据拷贝到连续内存后写入
                // 优点：不受 IOV_MAX 限制，可以一次性写入大批量数据
                // 缺点：需要一次内存拷贝
                flushed_bytes = self.current_fd.load().write(&iovecs, write_mode).await? as u64;
            }
        }

        // 5. 验证写入完整性
        if flushed_bytes != total_bytes {
            return Err(anyhow!(
                "write_vectored incomplete: wrote {} bytes, expected {} (iovecs: {})",
                flushed_bytes,
                total_bytes,
                iovecs.len()
            ));
        }

        // 6. 更新刷盘位置
        self.write_ptr.rotate_flush_offset(flushed_bytes);

        // 7. 批量同步
        if fsync {
            let fd = self.current_fd.load_full();
            tokio::spawn(async move {
                let _ = fd.sync_data().await;
            });
        }

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
        serializer: Arc<dyn MessageSerializer>,
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
        if write_ptr.get_filename().ne(&max_record_filename) {
            max_file += 1;
            write_ptr.reset_with_filename(dir.join(gen_record_filename(max_file)));
            factor.fetch_add(1, Ordering::Relaxed);
        }

        let _max_size_per_file = if conf.partition_writer_prealloc {
            conf.max_size_per_file
        } else {
            0
        };

        let current_fd = Self::get_current_fd(
            &dir,
            max_file,
            write_ptr.get_flush_offset(),
            conf.disk_write_mode,
        )
        .await?;

        Ok(Self {
            dir: dir.clone(),
            topic: tp.0,
            partition_id: tp.1,
            current_segment: Arc::new(factor),
            #[cfg(not(target_os = "linux"))]
            current_fd: Arc::new(ArcSwap::from_pointee(current_fd)),
            conf: conf.clone(),
            queue: Arc::new(SwitchQueue::new()),
            write_ptr,
            has_create_next_record_file: Arc::default(),
            flusher,
            serializer,
        })
    }

    #[cfg(not(target_os = "linux"))]
    async fn get_current_fd(
        dir: &PathBuf,
        max_file: u64,
        offset: u64,
        mode: crate::disk::DiskWriteMode,
    ) -> Result<FileHandlerWriterAsync> {
        let current_fd = create_writer_fd(&dir.join(gen_record_filename(max_file)), mode).await?;

        // 注意：fd::Writer 内部已经追踪位置，不需要手动 seek
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

        match create_writer_fd_with_prealloc(
            &next_filename,
            max_size_per_file,
            self.conf.disk_write_mode,
        )
        .await
        {
            Ok(new_writer) => {
                // 使用 ArcSwap::store 原子替换底层 Writer
                self.current_fd.store(Arc::new(new_writer));
                self.write_ptr.reset_with_filename(next_filename);
            }
            Err(e) => {
                error!("create_writer_fd err: {e:?}");
            }
        }

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
            // 估算序列化后的大小（用于判断是否需要 rotation）
            // 格式：[8:total_len] + [1:msg_id_len] + msg_id + [8:timestamp] + [4:metadata_len] + metadata + [4:payload_len] + payload
            let estimated_metadata_size: usize = data
                .metadata
                .iter()
                .map(|(k, v)| 4 + k.len() + 4 + v.len())
                .sum();
            let estimated_size = 8  // total_len header
                + 1 + data.msg_id.len()  // msg_id
                + 8  // timestamp
                + 4 + 4 + estimated_metadata_size  // metadata_len + entry_count + entries
                + 4 + data.payload.len(); // payload_len + payload

            let msg_total_len = estimated_size as u64;

            let should_rotate = {
                now_ptr_offset + msg_total_len > self.conf.max_size_per_file
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

            // ✅ 直接将 MessagePayload 入队，延迟到 flush 时才序列化（保持零拷贝）
            self.queue.push(data);

            now_batch_total_size += msg_total_len;
            now_ptr_offset += msg_total_len;
            now_count += 1;
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
