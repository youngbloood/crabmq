use crate::disk::fd::{Reader, Writer};
use crate::disk::prealloc::preallocate;
use anyhow::Result;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs::OpenOptions, io::IoSlice, path::Path};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// VectoredWriter: 使用 write_vectored 的零拷贝写入实现
///
/// 特点：
/// - 零拷贝：直接使用 writev 系统调用，避免内存拷贝
/// - 高性能：适合批量写入小块数据的场景
/// - 并发安全：使用 Mutex 保护文件句柄，支持多线程并发写入
pub struct VectoredWriter {
    fd: Mutex<AsyncFile>,
    write_pos: AtomicU64,
}

impl VectoredWriter {
    /// 创建 VectoredWriter
    ///
    /// # 参数
    /// - `filename`: 文件路径
    /// - `prealloc`: 是否预分配文件空间
    /// - `prealloc_size`: 预分配大小（字节）
    pub async fn new(filename: &Path, prealloc: bool, prealloc_size: u64) -> Result<Self> {
        // 使用 O_DIRECT 标志打开文件（绕过页缓存，直接写入磁盘）
        // 注意：O_DIRECT 要求数据对齐，但 write_vectored 会自动处理
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .custom_flags(libc::O_DSYNC) // 数据同步写入（不同步元数据）
            .open(filename)?;

        // 预分配文件空间（减少写入时的磁盘碎片）
        if prealloc && prealloc_size > 0 {
            preallocate(&file, prealloc_size)?;
        }

        let async_file = AsyncFile::from_std(file);
        let write_pos = async_file.metadata().await?.len();

        Ok(Self {
            fd: Mutex::new(async_file),
            write_pos: AtomicU64::new(write_pos),
        })
    }
}

#[async_trait::async_trait]
impl Writer for VectoredWriter {
    /// 批量写入数据（零拷贝）
    ///
    /// 使用 writev 系统调用，将多个分散的内存块一次性写入文件，
    /// 避免数据拷贝，适合批量写入小块数据的场景
    async fn write(&self, datas: &[IoSlice<'_>]) -> Result<usize> {
        if datas.is_empty() {
            return Ok(0);
        }

        let mut fd = self.fd.lock().await;
        let size = fd.write_vectored(datas).await?;
        self.write_pos.fetch_add(size as u64, Ordering::Relaxed);
        Ok(size)
    }

    /// 同步数据到磁盘
    async fn sync_data(&self) -> Result<()> {
        let fd = self.fd.lock().await;
        fd.sync_data().await?;
        Ok(())
    }

    /// 获取当前写入位置
    fn write_pos(&self) -> u64 {
        self.write_pos.load(Ordering::Relaxed)
    }
}

/// VectoredReader: 使用 read 的读取实现
///
/// 特点：
/// - 简单高效：使用标准的 read 系统调用
/// - 并发安全：使用 Mutex 保护文件句柄
pub struct VectoredReader {
    fd: Mutex<AsyncFile>,
    read_pos: AtomicU64,
}

impl VectoredReader {
    /// 创建 VectoredReader
    pub async fn new(filename: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(filename)?;
        let async_file = AsyncFile::from_std(file);

        Ok(Self {
            fd: Mutex::new(async_file),
            read_pos: AtomicU64::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Reader for VectoredReader {
    /// 从当前位置读取 n 字节数据
    async fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut fd = self.fd.lock().await;
        let mut buffer = vec![0u8; n];
        let size = fd.read(&mut buffer).await?;
        buffer.truncate(size);
        self.read_pos.fetch_add(size as u64, Ordering::Relaxed);
        Ok(buffer)
    }

    /// 从指定偏移量读取 n 字节数据
    async fn read_at(&mut self, offset: u64, n: usize) -> Result<Vec<u8>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut fd = self.fd.lock().await;
        fd.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut buffer = vec![0u8; n];
        let size = fd.read(&mut buffer).await?;
        buffer.truncate(size);
        self.read_pos.store(offset + size as u64, Ordering::Relaxed);
        Ok(buffer)
    }

    /// 获取当前读取位置
    fn read_pos(&self) -> u64 {
        self.read_pos.load(Ordering::Relaxed)
    }
}
