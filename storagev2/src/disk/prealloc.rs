//! 文件空间预分配模块
//! 提供跨平台的文件预分配能力，优化顺序写入性能

use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{self},
};

#[cfg(target_os = "linux")]
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    linux::fallocate(file, len).context("Failed to preallocate file space")
}

#[cfg(target_os = "macos")]
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    macos::fcntl_preallocate(file, len).context("Failed to preallocate file space")
}

#[cfg(windows)]
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    windows::set_valid_data(file, len).context("Failed to preallocate file space")
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    generic::prealloc_fill_zero(file, len).context("Failed to preallocate file space")
}

/// 获取文件当前物理大小
pub fn get_physical_size(file: &File) -> Result<u64> {
    let metadata = file.metadata()?;
    Ok(metadata.len())
}

// --------------------------
// 平台特定实现
// --------------------------

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use libc::{FALLOC_FL_KEEP_SIZE, fallocate};
    use std::os::unix::prelude::*;

    pub fn fallocate(file: &File, len: u64) -> Result<()> {
        let fd = file.as_raw_fd();
        let ret = unsafe {
            fallocate(
                fd,
                FALLOC_FL_KEEP_SIZE, // 保持逻辑大小不变
                0,                   // 起始偏移
                len as libc::off_t,  // 分配长度
            )
        };

        if ret != 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOSPC) {
                return Err(err).context("Disk space不足");
            }
            return Err(err.into());
        }
        file.set_len(len)?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use libc::{F_ALLOCATECONTIG, F_PREALLOCATE, fcntl};
    use std::os::unix::prelude::*;

    pub fn fcntl_preallocate(file: &File, len: u64) -> Result<()> {
        let fd = file.as_raw_fd();
        let mut fstore = libc::fstore_t {
            fst_flags: F_ALLOCATECONTIG,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as i64,
            fst_bytesalloc: 0,
        };

        let ret = unsafe { fcntl(fd, F_PREALLOCATE, &mut fstore) };
        if ret == -1 {
            // 回退到非连续分配
            fstore.fst_flags = 0;
            let ret = unsafe { fcntl(fd, F_PREALLOCATE, &mut fstore) };
            if ret == -1 {
                return Err(io::Error::last_os_error().into());
            }
        }

        // 实际扩展文件大小
        file.set_len(len)?;
        Ok(())
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::os::windows::prelude::*;
    use winapi::um::fileapi::SetFileValidData;

    pub fn set_valid_data(file: &File, len: u64) -> Result<()> {
        let handle = file.as_raw_handle() as *mut _;

        // 需要 SE_MANAGE_VOLUME_NAME 权限
        unsafe {
            if SetFileValidData(handle, len) == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(5) {
                    // ERROR_ACCESS_DENIED
                    log::warn!("需要管理员权限进行文件预分配，回退到标准方法");
                    return prealloc_fill_zero(file, len);
                }
                return Err(err.into());
            }
        }
        Ok(())
    }
}

/// 通用实现（非高效）
mod generic {
    use super::*;
    use std::os::unix::fs::FileExt as _;

    const BLOCK_SIZE: usize = 1 << 20; // 1MB

    pub fn prealloc_fill_zero(file: &File, len: u64) -> Result<()> {
        let current_len = file.metadata()?.len();
        if current_len >= len {
            return Ok(());
        }

        let mut buf = vec![0u8; BLOCK_SIZE];
        let mut pos = current_len;

        while pos < len {
            let write_size = std::cmp::min(BLOCK_SIZE as u64, len - pos) as usize;
            file.write_all_at(&buf[..write_size], pos)?;
            pos += write_size as u64;
        }

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tempfile::tempfile;

//     #[test]
//     fn test_linux_fallocate() {
//         #[cfg(target_os = "linux")]
//         {
//             let file = tempfile().unwrap();
//             preallocate(&file, 1024).unwrap();
//             assert!(get_physical_size(&file).unwrap() >= 1024);
//         }
//     }

//     #[test]
//     fn test_cross_platform() {
//         let file = tempfile().unwrap();
//         let result = preallocate(&file, 1024);

//         // 根据当前平台验证结果
//         if cfg!(target_os = "linux") {
//             assert!(result.is_ok());
//         } else if cfg!(windows) {
//             assert!(result.is_ok() || result.unwrap_err().to_string().contains("需要管理员权限"));
//         } else {
//             assert!(result.is_ok());
//         }
//     }

//     #[bench]
//     fn bench_preallocate(b: &mut test::Bencher) {
//         let file = tempfile().unwrap();
//         b.iter(|| {
//             preallocate(&file, 1_000_000).unwrap();
//             file.set_len(0).unwrap();
//         });
//     }
// }
