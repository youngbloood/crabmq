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

pub struct PartitionWriterBuffer {
    dir: PathBuf,
    config: Config,

    // 当前写的文件因子：用于构成写入的目标文件
    current_factor: Arc<AtomicU64>,

    // 定期刷盘至磁盘
    fd_cache: Arc<FdWriterCacheAync>,

    // 先将数据存入内存缓冲区
    buffer: Arc<RwLock<BytesMut>>,

    // 写相关的位置指针
    write_ptr: Arc<RwLock<WriterPositionPtr>>,

    // 是否已经预创建了下一个 record 文件
    has_create_next_record_file: Arc<AtomicBool>,
}

impl PartitionWriterBuffer {
    pub async fn new(
        dir: PathBuf,
        config: &Config,
        fd_cache: Arc<FdWriterCacheAync>,
        write_ptr: Arc<RwLock<WriterPositionPtr>>,
    ) -> Result<Self> {
        let mut files = dir_recursive(dir.clone(), &[OsString::from("record")])?;
        let factor = if !files.is_empty() {
            files.sort();
            files.reverse();
            let filename = files.first().unwrap().file_name().unwrap();
            let filename_factor = PathBuf::from(filename)
                .with_extension("")
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();

            AtomicU64::new(filename_factor)
        } else {
            AtomicU64::new(0)
        };

        Ok(Self {
            dir,
            current_factor: Arc::new(factor),
            fd_cache,
            config: config.clone(),
            buffer: Arc::new(RwLock::new(BytesMut::with_capacity(1000))),
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
        {
            let res = {
                let writer_ptr_rl = self.write_ptr.read().await;
                writer_ptr_rl.offset + data.len() as u64 + 8 > self.config.max_size_per_file
                    || writer_ptr_rl.current_count >= self.config.max_msg_num_per_file
            };
            if res {
                self.flush().await?;
                self.rotate_file().await;
            }

            {
                let mut buffer_wl = self.buffer.write().await;
                buffer_wl.extend((data.len() as u64).to_le_bytes());
                buffer_wl.extend_from_slice(&data);
            }

            let buffer_rl = self.buffer.read().await;
            // 缓冲区超过4MB立即刷盘
            if buffer_rl.len() as u64 > self.config.flusher_factor {
                self.flush().await?;
            }
        };

        let mut writer_ptr_rl = self.write_ptr.write().await;
        writer_ptr_rl.current_count += 1;
        writer_ptr_rl.offset += data.len() as u64 + 8;

        // 当前文件的使用率已经达到设置阈值，预创建下一个
        if (writer_ptr_rl.offset as f64 / self.config.max_size_per_file as f64) * 100.0
            >= self.config.create_next_record_file_threshold
            && self
                .has_create_next_record_file
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let next_filename = self.get_next_filename().await;
            if self.fd_cache.get(&next_filename).is_none() {
                let _fd_cache = self.fd_cache.clone();
                tokio::spawn(async move { _fd_cache.get_or_create(&next_filename) });
            }
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let buffer_len = { self.buffer.read().await.len() };
        if buffer_len == 0 {
            return Ok(());
        }

        let fd = self
            .fd_cache
            .get_or_create(&self.dir.join(gen_record_filename(
                self.current_factor.load(Ordering::Relaxed),
            )))?;

        let mut buffer_wl = self.buffer.write().await;
        {
            let mut fd_wl = fd.write().await;
            let buf_len = buffer_wl.to_vec().len();
            let mut writer_ptr_wl = self.write_ptr.write().await;
            fd_wl
                .seek(std::io::SeekFrom::Start(writer_ptr_wl.flush_offset))
                .await?;
            fd_wl.write_all(&buffer_wl.to_vec()).await?;
            fd_wl.sync_data().await?; // 强制刷盘
            writer_ptr_wl.flush_offset += buf_len as u64;
        }
        buffer_wl.clear();

        Ok(())
    }
}
