use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio_uring::buf::IoBufMut;
use tokio_uring::fs as tokio_fs;

const BLOCK_SIZE: usize = 1024 * 1024; // 1MB
const SIZES_GB: [u64; 3] = [10, 20, 50]; // GB

// 预分配文件空间 (使用 fallocate)
fn prealloc(filename: &str, size: u64) -> io::Result<()> {
    let file = File::create(filename)?;
    file.set_len(size)?;
    file.sync_all()?; // 确保元数据落盘
    Ok(())
}

// 测试用例1: 顺序写入
fn test_sequential_write(size_gb: u64) -> io::Result<Duration> {
    let size = size_gb * 1024 * 1024 * 1024;
    let filename = format!("seq_{}g.bin", size_gb);
    let mut file = File::create(&filename)?;

    let buffer = vec![0u8; BLOCK_SIZE];
    let start = Instant::now();

    let mut written = 0;
    while written < size {
        let write_size = (size - written).min(BLOCK_SIZE as u64) as usize;
        file.write_all(&buffer[..write_size])?;
        written += write_size as u64;
    }

    file.sync_all()?; // 确保数据落盘
    let duration = start.elapsed();
    std::fs::remove_file(filename)?;
    Ok(duration)
}

// 测试用例2: 预分配 + 顺序写入
fn test_prealloc_write(size_gb: u64) -> io::Result<Duration> {
    let size = size_gb * 1024 * 1024 * 1024;
    let filename = format!("prealloc_{}g.bin", size_gb);
    prealloc(&filename, size)?;

    let mut file = File::options().write(true).open(&filename)?;
    let buffer = vec![0u8; BLOCK_SIZE];
    let start = Instant::now();

    let mut written = 0;
    while written < size {
        let write_size = (size - written).min(BLOCK_SIZE as u64) as usize;
        file.write_all(&buffer[..write_size])?;
        written += write_size as u64;
    }

    file.sync_all()?;
    let duration = start.elapsed();
    std::fs::remove_file(filename)?;
    Ok(duration)
}

// 测试用例3: io_uring 顺序写入
async fn test_uring_sequential_write(size_gb: u64) -> io::Result<Duration> {
    let size = size_gb * 1024 * 1024 * 1024;
    let filename = format!("uring_seq_{}g.bin", size_gb);
    let file = tokio_fs::File::create(filename).await?;

    let buffer = vec![0u8; BLOCK_SIZE].into_boxed_slice();
    let start = Instant::now();

    let mut written = 0;
    while written < size {
        let write_size = (size - written).min(BLOCK_SIZE as u64) as usize;
        let slice = &buffer[..write_size];

        let (res, _) = file.write_at(slice, written).await;
        written += res? as u64;
    }

    file.sync_all().await?;
    let duration = start.elapsed();
    tokio_uring::start(async move {
        drop(file);
        tokio_fs::remove_file(&format!("uring_seq_{}g.bin", size_gb)).await
    });
    Ok(duration)
}

// 测试用例4: io_uring + 预分配写入
async fn test_uring_prealloc_write(size_gb: u64) -> io::Result<Duration> {
    let size = size_gb * 1024 * 1024 * 1024;
    let filename = format!("uring_prealloc_{}g.bin", size_gb);
    prealloc(&filename, size)?;

    let file = tokio_fs::OpenOptions::new()
        .write(true)
        .open(&filename)
        .await?;

    let buffer = vec![0u8; BLOCK_SIZE].into_boxed_slice();
    let start = Instant::now();

    let mut written = 0;
    while written < size {
        let write_size = (size - written).min(BLOCK_SIZE as u64) as usize;
        let slice = &buffer[..write_size];

        let (res, _) = file.write_at(slice, written).await;
        written += res? as u64;
    }

    file.sync_all().await?;
    let duration = start.elapsed();
    tokio_uring::start(async move {
        drop(file);
        tokio_fs::remove_file(filename).await
    });
    Ok(duration)
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn test_disk_bench() -> Result<()> {
        println!(
            "{:<15} | {:<15} | {:<15} | {:<15} | {:<15}",
            "Size (GB)", "Sequential", "Prealloc", "io_uring", "io_uring+prealloc"
        );
        println!("{}", "-".repeat(80));

        for &size in &SIZES_GB {
            // 同步测试
            let dur_seq = test_sequential_write(size).unwrap_or_else(|e| {
                eprintln!("Seq {}GB error: {}", size, e);
                Duration::MAX
            });

            let dur_prealloc = test_prealloc_write(size).unwrap_or_else(|e| {
                eprintln!("Prealloc {}GB error: {}", size, e);
                Duration::MAX
            });

            // 异步测试 (在io_uring运行时中执行)
            let (dur_uring, dur_uring_prealloc) = tokio_uring::start(async {
                let dur_uring = test_uring_sequential_write(size).await.unwrap_or_else(|e| {
                    eprintln!("io_uring {}GB error: {}", size, e);
                    Duration::MAX
                });

                let dur_uring_prealloc =
                    test_uring_prealloc_write(size).await.unwrap_or_else(|e| {
                        eprintln!("io_uring+prealloc {}GB error: {}", size, e);
                        Duration::MAX
                    });

                (dur_uring, dur_uring_prealloc)
            });

            // 打印结果
            println!(
                "{:<15} | {:<13.2?} | {:<13.2?} | {:<13.2?} | {:<13.2?}",
                size, dur_seq, dur_prealloc, dur_uring, dur_uring_prealloc
            );
        }

        Ok(())
    }
}
