use anyhow::{Context as _, Result, anyhow};
use futures::Future;
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::{
    env::var,
    fs::{self, File},
    path::Path,
    time::Duration,
};
use tokio::fs as async_fs;
use tokio::time::Interval;
use tokio::{select, time::interval as async_interval};

const CAPITAL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const LOWERCASE: &str = "abcdefghijklmnopqrstuvwxyz";
const NUMBER: &str = "0123456789";

pub fn type_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

/// [`execute_timeout`] execute the [`fut`] in timeout interval. It will return Err when timeout or fur return a Err.
pub async fn execute_timeout<T>(fut: impl Future<Output = Result<T>>, timeout: u64) -> Result<T> {
    let mut ticker = interval(Duration::from_secs(timeout)).await;
    select! {
        out = fut => {
            match out {
                Err(e) => Err(anyhow!(e)),
                Ok(v) => Ok(v),
            }
        },
        _ = ticker.tick() => {
             Err(anyhow!("execute timeout"))
        }
    }
}

pub fn check_and_create_dir<P: AsRef<OsStr>>(dir: P) -> Result<()> {
    let dir_path = Path::new(&dir);
    if !dir_path.exists() {
        fs::create_dir_all(dir_path).expect("create {dir} failed");
    } else if dir_path.is_file() {
        let parent = dir_path.parent().unwrap();
        return Err(anyhow!(
            "has exist the same file in dir: {}",
            parent.to_str().unwrap()
        ));
    }

    Ok(())
}

pub fn check_and_create_filename<P: AsRef<OsStr>>(filename: P) -> Result<()> {
    let path = Path::new(&filename);
    if !path.exists() {
        File::create(path)?;
    } else if !path.is_file() {
        let parent = path.parent().unwrap();
        return Err(anyhow!(
            "has exist the same file in dir: {}",
            parent.to_str().unwrap()
        ));
    }

    Ok(())
}

pub fn check_exist<P: AsRef<OsStr>>(path: P) -> bool {
    let p = Path::new(&path);
    p.exists()
}

pub fn is_debug() -> bool {
    let v: String = var("FOR_DEBUG").unwrap_or_default();
    matches!(
        v.to_lowercase().as_str(),
        "t" | "true" | "1" | "on" | "open"
    )
}

pub fn random_str(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn random_num(start: u64, end: u64) -> u64 {
    thread_rng().gen_range(start..end)
}

pub async fn interval(dur: Duration) -> Interval {
    let mut ticker = async_interval(dur);
    ticker.tick().await;
    ticker
}

// pub fn dir_recursive(dir: PathBuf, exts: &[OsString]) -> Result<Vec<PathBuf>> {
//     if !check_exist(&dir) {
//         return Err(anyhow!("dir[{dir:?}] not exist"));
//     }

//     let dirs: std::iter::Enumerate<fs::ReadDir> = fs::read_dir(&dir)
//         .expect("read dir[{dir:?}] failed")
//         .enumerate();

//     let mut list = vec![];
//     for (_, dr) in dirs {
//         assert!(dr.is_ok());
//         let entry = dr.unwrap();

//         if entry.file_type().unwrap().is_dir() {
//             let leaves = dir_recursive(dir.clone().join(entry.file_name()))?;
//             list.extend(leaves);
//             continue;
//         }
//         let filename = dir.join(entry.file_name());
//         if !exts.is_empty() {
//             if exts.contains(&filename.extension().unwrap_or_default()) {
//                 list.push(filename);
//             }
//         } else {
//             list.push(filename);
//         }
//     }

//     Ok(list)
// }

/// 递归获取目录下指定后缀的文件列表
pub fn dir_recursive(dir: PathBuf, exts: &[OsString]) -> Result<Vec<PathBuf>> {
    // 检查目录是否存在
    if !dir.exists() {
        return Err(anyhow!("Directory [{:?}] does not exist", dir));
    }
    if !dir.is_dir() {
        return Err(anyhow!("[{:?}] is not a directory", dir));
    }

    let mut file_list = Vec::new();

    // 读取目录条目
    let entries =
        fs::read_dir(&dir).with_context(|| format!("Failed to read directory: {:?}", dir))?;

    for entry in entries {
        let entry =
            entry.with_context(|| format!("Failed to read entry in directory: {:?}", dir))?;
        let path = entry.path();

        // 递归处理子目录
        if path.is_dir() {
            let sub_files = dir_recursive(path, exts)?;
            file_list.extend(sub_files);
            continue;
        }

        // 文件扩展名过滤逻辑
        let should_include = match exts.is_empty() {
            true => true, // 无后缀过滤条件
            false => {
                // 获取文件扩展名并进行匹配
                path.extension()
                    .map(|ext| exts.contains(&ext.to_os_string()))
                    .unwrap_or(false) // 无后缀文件不匹配
            }
        };

        if should_include {
            file_list.push(path);
        }
    }

    Ok(file_list)
}

// async fn async_dir_recursive(dir: PathBuf, extensions: &[OsString]) -> Result<Vec<PathBuf>> {
//     let mut files = Vec::new();
//     let mut entries = async_fs::read_dir(dir).await?;

//     while let Some(entry) = entries.next_entry().await? {
//         let path = entry.path();
//         if path.is_dir() {
//             let sub_files = async_dir_recursive(path, extensions).await?;
//             files.extend(sub_files);
//         } else if let Some(ext) = path.extension() {
//             if extensions.contains(&ext.to_os_string()) {
//                 files.push(path);
//             }
//         }
//     }

//     Ok(files)
// }
