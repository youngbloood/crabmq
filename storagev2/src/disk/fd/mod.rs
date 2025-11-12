mod mmap;
mod vectored;

use anyhow::Result;
use std::io::IoSlice;
use std::path::Path;

pub use mmap::MmapWriter;
pub use vectored::VectoredWriter;

use super::DiskWriteMode;

/// 写入 trait：统一抽象不同的磁盘写入方式
///
/// 注意：所有方法使用 &self（非 &mut self），内部通过 Mutex 保证并发安全
#[async_trait::async_trait]
pub trait Writer: Send + Sync {
    /// 批量写入数据（支持零拷贝的 IoSlice）
    ///
    /// 使用 &self 而不是 &mut self，内部通过 Mutex 保证并发安全
    async fn write(&self, datas: &[IoSlice<'_>]) -> Result<usize>;

    /// 同步数据到磁盘（fsync）
    async fn sync_data(&self) -> Result<()>;

    /// 获取当前写入位置
    fn write_pos(&self) -> u64;
}

/// 读取 trait：统一抽象不同的磁盘读取方式
#[async_trait::async_trait]
pub trait Reader: Send + Sync {
    /// 从当前位置读取 n 字节数据
    async fn read(&mut self, n: usize) -> Result<Vec<u8>>;

    /// 从指定偏移量读取 n 字节数据
    async fn read_at(&mut self, offset: u64, n: usize) -> Result<Vec<u8>>;

    /// 获取当前读取位置
    fn read_pos(&self) -> u64;
}

/// 根据配置创建对应的 Writer
pub async fn create_writer(
    filename: &Path,
    mode: DiskWriteMode,
    prealloc: bool,
    prealloc_size: u64,
) -> Result<Box<dyn Writer>> {
    match mode {
        DiskWriteMode::WriteVectored => {
            let writer = VectoredWriter::new(filename, prealloc, prealloc_size).await?;
            Ok(Box::new(writer))
        }
        DiskWriteMode::Mmap => {
            let writer = MmapWriter::new(filename, prealloc, prealloc_size).await?;
            Ok(Box::new(writer))
        }
    }
}

/// 根据配置创建对应的 Reader（未来扩展）
pub async fn create_reader(
    filename: &Path,
    mode: DiskWriteMode,
) -> Result<Box<dyn Reader>> {
    match mode {
        DiskWriteMode::WriteVectored => {
            let reader = vectored::VectoredReader::new(filename).await?;
            Ok(Box::new(reader))
        }
        DiskWriteMode::Mmap => {
            let reader = mmap::MmapReader::new(filename).await?;
            Ok(Box::new(reader))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::IoSlice;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_vectored_write_read() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_vectored.dat");

        // 测试写入
        let mut writer = create_writer(
            &file_path,
            DiskWriteMode::WriteVectored,
            false,
            0,
        ).await?;

        let data1 = b"Hello ";
        let data2 = b"World!";
        let iovecs = [IoSlice::new(data1), IoSlice::new(data2)];

        let written = writer.write(&iovecs).await?;
        assert_eq!(written, 12);
        assert_eq!(writer.write_pos(), 12);

        writer.sync_data().await?;
        drop(writer);

        // 测试读取
        let mut reader = create_reader(&file_path, DiskWriteMode::WriteVectored).await?;
        let data = reader.read(12).await?;
        assert_eq!(data, b"Hello World!");

        Ok(())
    }

    #[tokio::test]
    async fn test_mmap_write_read() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_mmap.dat");

        // 测试写入
        let mut writer = create_writer(
            &file_path,
            DiskWriteMode::Mmap,
            true,
            1024 * 1024, // 1MB 预分配
        ).await?;

        let data1 = b"Mmap ";
        let data2 = b"Test!";
        let iovecs = [IoSlice::new(data1), IoSlice::new(data2)];

        let written = writer.write(&iovecs).await?;
        assert_eq!(written, 10);
        assert_eq!(writer.write_pos(), 10);

        writer.sync_data().await?;
        drop(writer);

        // 测试读取
        let mut reader = create_reader(&file_path, DiskWriteMode::Mmap).await?;
        let data = reader.read(10).await?;
        assert_eq!(data, b"Mmap Test!");

        Ok(())
    }

    #[tokio::test]
    async fn test_large_batch_write() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_large.dat");

        let mut writer = create_writer(
            &file_path,
            DiskWriteMode::WriteVectored,
            false,
            0,
        ).await?;

        // 创建 100 个 IoSlice，每个 8 字节
        let chunks: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("chunk{:02} ", i).into_bytes())
            .collect();

        let iovecs: Vec<IoSlice> = chunks.iter().map(|c| IoSlice::new(c)).collect();

        let written = writer.write(&iovecs).await?;
        assert_eq!(written, 100 * 8); // 每个 chunk 8 字节 ("chunk00 " = 8 bytes)

        writer.sync_data().await?;

        Ok(())
    }
}
