use crate::disk::{
    Config,
    fd_cache::FdWriterCacheAync,
    meta::{WriterPositionPtr, gen_record_filename},
};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use common::dir_recursive;
use std::{
    ffi::OsString,
    io::Read,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncSeekExt as _, AsyncWriteExt as _},
    sync::RwLock,
};

#[derive(Clone)]
pub struct PartitionWriterBuffer {
    dir: PathBuf,
    conf: Config,

    // 当前写的文件因子：用于构成写入的目标文件
    current_factor: Arc<AtomicU64>,

    // 定期刷盘至磁盘(该分区下的磁盘文件)
    fd_cache: Arc<FdWriterCacheAync>,

    // 先将数据存入内存缓冲区
    buffer: Arc<RwLock<BytesMut>>,

    // 写相关的位置指针
    write_ptr: Arc<RwLock<WriterPositionPtr>>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,
}

impl PartitionWriterBuffer {
    pub fn new(
        dir: PathBuf,
        config: &Config,
        fd_cache: Arc<FdWriterCacheAync>,
        write_ptr: Arc<RwLock<WriterPositionPtr>>,
    ) -> Result<Self> {
        let mut files = dir_recursive(dir.clone(), &[OsString::from("record")])?;

        // 只获取最大编号的文件
        let max_file = files
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| s.split('.').next())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max();
        let factor = if let Some(max) = max_file {
            AtomicU64::new(max)
        } else {
            AtomicU64::new(0)
        };

        Ok(Self {
            dir,
            current_factor: Arc::new(factor),
            fd_cache,
            conf: config.clone(),
            buffer: Arc::new(RwLock::new(BytesMut::with_capacity(
                (config.flusher_factor as f64 * 1.5) as usize,
            ))),
            write_ptr,
            has_create_next_record_file: Arc::default(),
        })
    }

    async fn rotate_file(&self) {
        self.current_factor.fetch_add(1, Ordering::Relaxed);
        let mut writer_ptr_wl = self.write_ptr.write().await;
        writer_ptr_wl.filename = self.dir.join(gen_record_filename(
            self.current_factor.load(Ordering::Relaxed),
        ));
        writer_ptr_wl.offset = 0;
        writer_ptr_wl.current_count = 0;
        writer_ptr_wl.flush_offset = 0;

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
    pub async fn write(&self, data: Bytes) -> Result<()> {
        // 检查是否需要滚动文件
        let should_rotate = {
            let writer_ptr_rl = self.write_ptr.read().await;
            writer_ptr_rl.offset + data.len() as u64 + 8 > self.conf.max_size_per_file
                || writer_ptr_rl.current_count >= self.conf.max_msg_num_per_file
        };

        if should_rotate {
            self.flush().await?;
            self.rotate_file().await;
        }
        let data_len = data.len();
        let total_size = data_len + 8;

        // 快速写入缓冲区
        {
            let mut buffer_wl = self.buffer.write().await;
            buffer_wl.extend((data_len as u64).to_le_bytes());
            buffer_wl.extend_from_slice(&data[..]);
        }
        // 更新写入指针 - 尽量减少写锁持有时间
        {
            let mut writer_ptr_wl = self.write_ptr.write().await;
            writer_ptr_wl.current_count += 1;
            writer_ptr_wl.offset += total_size as u64;
        }
        // 检查是否需要立即刷盘
        let should_flush = {
            let buffer_rl = self.buffer.read().await;
            self.conf.flusher_factor != 0 && buffer_rl.len() as u64 > self.conf.flusher_factor
        };

        if should_flush {
            self.flush().await?;
        }

        // 当前文件的使用率已经达到设置阈值，预创建下一个
        let should_pre_create = {
            let writer_ptr_rl = self.write_ptr.read().await;
            ((writer_ptr_rl.offset as f64 / self.conf.max_size_per_file as f64) * 100.0) as u64
                >= self.conf.create_next_record_file_threshold as u64
                && self
                    .has_create_next_record_file
                    .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
        };
        if should_pre_create {
            let next_filename = self.get_next_filename().await;
            let writer_ptr = self.write_ptr.clone();
            if self.fd_cache.get(&next_filename).is_none() {
                let _fd_cache = self.fd_cache.clone();
                tokio::spawn(async move {
                    if _fd_cache.get_or_create(&next_filename, true).is_ok() {
                        // let mut wl = writer_ptr.write().await;
                        // wl.next_filename = next_filename;
                        // wl.offset = 0;
                    }
                });
            }
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let buffer_len = { self.buffer.read().await.len() };
        if buffer_len == 0 {
            return Ok(());
        }

        let fd = self.fd_cache.get_or_create(
            &self.dir.join(gen_record_filename(
                self.current_factor.load(Ordering::Relaxed),
            )),
            true,
        )?;

        // 直接使用缓冲区数据，避免拷贝
        let data = {
            let mut buffer_wl = self.buffer.write().await;
            buffer_wl.split().freeze() // 直接获取Bytes，避免拷贝
        };

        let data_len = data.len();

        {
            let mut fd_wl = fd.write().await;
            let mut writer_ptr_wl = self.write_ptr.write().await;

            fd_wl
                .seek(std::io::SeekFrom::Start(writer_ptr_wl.flush_offset))
                .await?;

            fd_wl.write_all(&data).await?;
            fd_wl.sync_data().await?;

            writer_ptr_wl.flush_offset += data_len as u64;
        }

        Ok(())
    }
}
