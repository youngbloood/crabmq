use super::{
    FdCacheAync,
    config::DiskConfig,
    gen_record_filename,
    meta::{PartitionMeta, PositionPtr, WriterPositionPtr},
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
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::RwLock,
};

pub struct PartitionWriter {
    dir: PathBuf,
    config: DiskConfig,

    // 当前写的文件因子：用于构成写入的目标文件
    current_factor: Arc<AtomicU64>,

    // 定期刷盘至磁盘
    fd_cache: Arc<FdCacheAync>,

    // 先将数据存入内存缓冲区
    buffer: Arc<RwLock<BytesMut>>,

    // 写相关的位置指针
    write_ptr: Arc<RwLock<WriterPositionPtr>>,
}

impl PartitionWriter {
    pub async fn new(
        dir: PathBuf,
        config: &DiskConfig,
        fd_cache: Arc<FdCacheAync>,
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
        let buffer_len = {
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
            if buffer_rl.len() > 4 * 1024 * 1024 {
                self.flush().await?;
            }
            // 已包含8
            buffer_rl.len() as u64
        };

        let mut writer_ptr_rl = self.write_ptr.write().await;
        writer_ptr_rl.current_count += 1;
        writer_ptr_rl.offset += data.len() as u64 + 8;

        if (writer_ptr_rl.offset as f64 / self.config.max_size_per_file as f64) * 100.0 >= 80.0 {
            // 当前文件的使用率已经达到 80，预创建下一个
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
            let mut fd_wl = fd.writer().write().await;
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
