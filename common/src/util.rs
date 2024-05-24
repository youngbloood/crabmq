use std::{
    env::var,
    fs::{self, File},
    path::Path,
    time::Duration,
};

use anyhow::{anyhow, Result};
use futures::Future;
use tokio::{select, time::interval};

pub fn type_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

/// [`execute_timeout`] execute the [`fut`] in timeout interval. It will return Err when timeout or fur return a Err.
pub async fn execute_timeout<T>(fut: impl Future<Output = Result<T>>, timeout: u64) -> Result<T> {
    let mut ticker = interval(Duration::from_secs(timeout));
    ticker.tick().await;
    select! {
        out = fut => {
            if let Err(e) = out {
                return Err(anyhow!(e));
            }
            return out;
        },
        _ = ticker.tick() => {
            return Err(anyhow!("execute timeout"));
        }
    };
}

pub fn check_and_create_dir(dir: &str) -> Result<()> {
    let dir_path = Path::new(dir);
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

pub fn check_and_create_filename(filename: &str) -> Result<()> {
    let path = Path::new(filename);
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

pub fn check_exist(path: &str) -> bool {
    let p = Path::new(path);
    return p.exists();
}

pub fn is_debug() -> bool {
    let v: String = var("FOR_DEBUG").unwrap_or_default();
    match v.to_lowercase().as_str() {
        "t" | "true" | "1" | "on" | "open" => {
            return true;
        }
        _ => {
            return false;
        }
    }
}
