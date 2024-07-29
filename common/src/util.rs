use anyhow::{anyhow, Result};
use crossbeam::queue::{ArrayQueue, SegQueue};
use futures::Future;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::{
    env::var,
    fs::{self, File},
    path::Path,
    time::Duration,
};
use tokio::time::Interval;
use tokio::{select, time::interval as async_interval};
use tracing::debug;

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

#[derive(Debug)]
struct UnboundSyncVec<T> {
    array: ArrayQueue<T>,
    seg:SegQueue<T>,
}

impl<T> UnboundSyncVec<T> {
    fn with_capacity(cap: usize) -> Self {
        UnboundSyncVec {
            array:ArrayQueue::new(cap),
            seg:SegQueue::new(),
        }
    }

    fn push(&self,t:T) {
        if self.array.is_full() {
            self.seg.push(t);
        }else{
            let _ = self.array.push(t);
        }
    }

    fn pop(&self) -> Option<T> {
        if !self.array.is_empty(){
            return self.array.pop();
        }
        self.seg.pop()
    }

    fn pop_all(&self) -> Vec<T> {
        let mut res = vec![];
        while let Some(v)=self.array.pop(){
            res.push(v);
        }
        while let Some(v)=self.seg.pop(){
            res.push(v);
        }
        res
    }
} 

#[derive(Debug)]
pub struct SwitcherVec<T> {
    switcher: AtomicBool,

    t1:UnboundSyncVec<T>,
    t2:UnboundSyncVec<T>,
}

unsafe impl<T> Send for SwitcherVec<T> {}
unsafe impl<T> Sync for SwitcherVec<T> {}

impl<T> Default for SwitcherVec<T> {
    fn default() -> Self {
        Self {
            switcher: Default::default(),
            t1:UnboundSyncVec::with_capacity(10000),
            t2:UnboundSyncVec::with_capacity(10000),
        }
    }
}

impl<T> SwitcherVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        SwitcherVec {
            switcher: AtomicBool::new(false),
            t1:UnboundSyncVec::with_capacity(cap),
            t2:UnboundSyncVec::with_capacity(cap),
        }
    }

    pub fn push(&self, v: T) {
        if !self.switcher.load(Relaxed) {
            debug!("push into t1");
            self.t1.push(v);
        } else {
            debug!("push into t2");
            self.t2.push(v);
        }
    }

    pub fn pop(&self) -> Vec<T> {
        if self
            .switcher
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok()
        {
            return self.t1.pop_all();
        }

        if self
            .switcher
            .compare_exchange(true, false, Relaxed, Relaxed)
            .is_ok()
        {
            return self.t2.pop_all();
        }

        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};
    use crate::util::interval;
    use super::SwitcherVec;

    #[tokio::test]
    async fn test_switcher_push_and_pop() {
        let switcher = SwitcherVec::with_capacity(5);
        switcher.push(1);
        switcher.push(2);
        switcher.push(3);
        println!("pop1 = {:?}", switcher.pop());
        println!("pop2 = {:?}", switcher.pop());
        println!("pop3 = {:?}", switcher.pop());
        // println!("pop4 = {:?}", switcher.pop());

        switcher.push(4);
        switcher.push(5);
        switcher.push(6);
        println!("pop5 = {:?}", switcher.pop());

        switcher.push(7);
        switcher.push(8);
        switcher.push(9);
        println!("pop6 = {:?}", switcher.pop());
    }

    #[tokio::test]
    async fn test_switcher_push_and_pop2() {
        let switcher = Arc::new(SwitcherVec::with_capacity(5));
        let arc_switcher = switcher.clone();
        tokio::spawn(async move {
            for i in 1..10000 {
                arc_switcher.push(i);
            }
        });

        let mut ticker = interval(Duration::from_millis(1)).await;
        let mut null_num = 0;
        let mut times = 1;
        while null_num <= 2 {
            ticker.tick().await;
            let list = switcher.pop();
            if list.is_empty() {
                null_num += 1;
            }
            for v in list {
                assert_eq!(times, v);
                times += 1;
            }
        }
    }
}
