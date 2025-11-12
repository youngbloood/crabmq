use crate::disk::fd::{Reader, Writer};
use crate::disk::prealloc::preallocate;
use anyhow::{anyhow, Result};
use memmap2::{MmapMut, MmapOptions};
use once_cell::sync::Lazy;
use std::fs::{File, OpenOptions};
use std::io::IoSlice;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Semaphore};

// 全局信号量：限制并发 mmap 操作数量（避免耗尽线程池）
static MMAP_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(2048));

/// MmapWriter: 使用 mmap 的高性能写入实现
///
/// 特点：
/// - 高性能：通过内存映射减少用户态/内核态切换
/// - 自动扩容：文件空间不足时自动扩展并重新映射
/// - 并发安全：使用 Mutex 保护映射区域
/// - 适合大文件顺序写入场景
pub struct MmapWriter {
    fd: Mutex<File>,
    mmap: Mutex<MmapMut>,
    write_pos: AtomicU64,
    file_len: AtomicU64, // 当前文件长度（用于判断是否需要扩容）
}

impl MmapWriter {
    /// 创建 MmapWriter
    ///
    /// # 参数
    /// - `filename`: 文件路径
    /// - `prealloc`: 是否预分配文件空间
    /// - `prealloc_size`: 预分配大小（字节）
    pub async fn new(filename: &Path, prealloc: bool, prealloc_size: u64) -> Result<Self> {
        // 在 spawn_blocking 中执行同步文件操作
        let filename = filename.to_path_buf();
        let (fd, mmap, file_len) = tokio::task::spawn_blocking(move || -> Result<(File, MmapMut, u64)> {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .custom_flags(libc::O_DSYNC)
                .open(&filename)?;

            // 预分配文件空间
            let initial_size = if prealloc && prealloc_size > 0 {
                preallocate(&file, prealloc_size)?;
                prealloc_size
            } else {
                let len = file.metadata()?.len();
                if len == 0 {
                    // 文件为空，初始分配 1MB
                    let initial = 1024 * 1024;
                    file.set_len(initial)?;
                    initial
                } else {
                    len
                }
            };

            // 创建内存映射
            let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

            Ok((file, mmap, initial_size))
        })
        .await??;

        Ok(Self {
            fd: Mutex::new(fd),
            mmap: Mutex::new(mmap),
            write_pos: AtomicU64::new(0),
            file_len: AtomicU64::new(file_len),
        })
    }

    /// 扩展文件并重新映射（内部方法）
    async fn expand_and_remap(&self, required_size: u64) -> Result<()> {
        let current_len = self.file_len.load(Ordering::Relaxed);
        let new_len = (required_size.max(current_len * 2)).next_power_of_two(); // 指数增长

        // 在 spawn_blocking 中执行扩容和重新映射
        let fd_mutex = self.fd.lock().await;
        let mmap_mutex = self.mmap.lock().await;

        // 扩展文件
        fd_mutex.set_len(new_len)?;

        // 重新映射
        let new_mmap = unsafe { MmapOptions::new().map_mut(&*fd_mutex)? };
        drop(mmap_mutex);

        let mut mmap = self.mmap.lock().await;
        *mmap = new_mmap;
        self.file_len.store(new_len, Ordering::Relaxed);

        Ok(())
    }
}

#[async_trait::async_trait]
impl Writer for MmapWriter {
    /// 批量写入数据（通过内存映射）
    ///
    /// 将 IoSlice 数据拷贝到 mmap 映射的内存区域，
    /// 操作系统负责将脏页异步刷新到磁盘
    async fn write(&self, datas: &[IoSlice<'_>]) -> Result<usize> {
        if datas.is_empty() {
            return Ok(0);
        }

        // 1. 计算总大小
        let total_size: usize = datas.iter().map(|slice| slice.len()).sum();
        if total_size == 0 {
            return Ok(0);
        }

        let current_pos = self.write_pos.load(Ordering::Relaxed);
        let required_size = current_pos + total_size as u64;

        // 2. 检查是否需要扩容
        if required_size > self.file_len.load(Ordering::Relaxed) {
            self.expand_and_remap(required_size).await?;
        }

        // 3. 拷贝数据到连续内存
        let mut buffer = Vec::with_capacity(total_size);
        for slice in datas {
            buffer.extend_from_slice(slice);
        }

        let _permit = MMAP_SEMAPHORE.acquire().await.unwrap();

        // 4. 直接拷贝到映射区域（无需 spawn_blocking，内存拷贝很快）
        let mut mmap = self.mmap.lock().await;
        let write_offset = current_pos as usize;

        if write_offset + total_size > mmap.len() {
            return Err(anyhow!("写入位置超出映射区域"));
        }

        // 使用 unsafe 高效拷贝数据到映射区域
        unsafe {
            let dst_ptr = mmap.as_mut_ptr().add(write_offset);
            std::ptr::copy_nonoverlapping(buffer.as_ptr(), dst_ptr, total_size);
        }

        drop(mmap);

        // 5. 更新写入位置
        self.write_pos.fetch_add(total_size as u64, Ordering::Relaxed);

        Ok(total_size)
    }

    /// 同步数据到磁盘
    async fn sync_data(&self) -> Result<()> {
        let _permit = MMAP_SEMAPHORE.acquire().await.unwrap();
        let mmap = self.mmap.lock().await;

        // 在 spawn_blocking 中执行同步 flush
        // 注意：这里使用 flush_async 会更好，但 memmap2 不支持
        // 所以我们直接调用 flush（它是快速的系统调用）
        mmap.flush()?;

        Ok(())
    }

    /// 获取当前写入位置
    fn write_pos(&self) -> u64 {
        self.write_pos.load(Ordering::Relaxed)
    }
}

/// MmapReader: 使用 mmap 的读取实现
pub struct MmapReader {
    mmap: Mutex<memmap2::Mmap>,
    read_pos: AtomicU64,
}

impl MmapReader {
    /// 创建 MmapReader
    pub async fn new(filename: &Path) -> Result<Self> {
        let filename = filename.to_path_buf();
        let mmap = tokio::task::spawn_blocking(move || -> Result<memmap2::Mmap> {
            let file = OpenOptions::new().read(true).open(&filename)?;
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
            Ok(mmap)
        })
        .await??;

        Ok(Self {
            mmap: Mutex::new(mmap),
            read_pos: AtomicU64::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Reader for MmapReader {
    /// 从当前位置读取 n 字节数据
    async fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let current_pos = self.read_pos.load(Ordering::Relaxed) as usize;
        let mmap = self.mmap.lock().await;

        if current_pos + n > mmap.len() {
            return Err(anyhow!("读取位置超出文件范围"));
        }

        let data = mmap[current_pos..current_pos + n].to_vec();
        self.read_pos.fetch_add(n as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// 从指定偏移量读取 n 字节数据
    async fn read_at(&mut self, offset: u64, n: usize) -> Result<Vec<u8>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let offset = offset as usize;
        let mmap = self.mmap.lock().await;

        if offset + n > mmap.len() {
            return Err(anyhow!("读取位置超出文件范围"));
        }

        let data = mmap[offset..offset + n].to_vec();
        self.read_pos.store((offset + n) as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// 获取当前读取位置
    fn read_pos(&self) -> u64 {
        self.read_pos.load(Ordering::Relaxed)
    }
}
