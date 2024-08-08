use anyhow::{anyhow, Result};
use futures::Future;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::{
    env::var,
    fs::{self, File},
    path::Path,
    time::Duration,
};
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

pub fn dir_recursive(dir: PathBuf) -> Result<Vec<PathBuf>> {
    if !check_exist(&dir) {
        return Err(anyhow!("dir[{dir:?}] not exist"));
    }

    let dirs = fs::read_dir(&dir)
        .expect("read dir[{dir:?}] failed")
        .enumerate();

    let mut list = vec![];
    for (_, dr) in dirs {
        assert!(dr.is_ok());
        let entry = dr.unwrap();
        if entry.file_type().unwrap().is_dir() {
            let leaves = dir_recursive(dir.clone().join(entry.file_name()))?;
            list.extend(leaves);
            continue;
        }
        list.push(dir.join(entry.file_name()));
    }

    Ok(list)
}
