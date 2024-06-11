use crate::{util::type_of, ArcMux};
use anyhow::*;
use chrono::Local;
use lazy_static::*;
use parking_lot::Mutex;
use snowflake::SnowflakeIdBucket;
use std::{borrow::Borrow, cell::UnsafeCell, sync::Arc};
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    mpsc::error::SendError,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use ulid_rs::Ulid;

lazy_static! {
    // 全局cancel信号
    pub static ref CANCEL_TOKEN: CancellationToken = CancellationToken::new();
    //
    pub static ref STOPPED : Error=anyhow!("process had stopped");

    /// 全局的snowflake id生成器
    ///
    /// SnowflakeIdBucket具有buffer
    pub static ref SNOWFLAKE: SnowFlake = SnowFlake::new(1, 1);

    pub static ref ULID: Ulid = Ulid::new(Local::now().timestamp() as _, ||4);

    /// 全局的client过期
    pub static ref CLIENT_DROP_GUARD: ClientDropGuard = ClientDropGuard::init();
}

pub fn cancel() {
    CANCEL_TOKEN.cancel()
}

pub struct SnowFlake {
    inner: ArcMux<SnowflakeIdBucket>,
}
impl SnowFlake {
    pub fn new(mechine_id: i32, node_id: i32) -> Self {
        let bucket: SnowflakeIdBucket = SnowflakeIdBucket::new(1, 1);
        SnowFlake {
            inner: Arc::new(Mutex::new(bucket)),
        }
    }

    pub fn get_id(&self) -> i64 {
        self.inner.lock().get_id()
        // block_on(self.inner.lock()).get_id()
    }
}

pub struct ClientDrop {
    tx: Sender<String>,
    rx: Receiver<String>,
}

unsafe impl Send for ClientDrop {}
unsafe impl Sync for ClientDrop {}

impl ClientDrop {
    pub fn new() -> Self {
        let (tx, rx) = broadcast::channel(1);
        ClientDrop { tx, rx }
    }

    pub async fn send(&self, addr: &str) -> Result<()> {
        self.tx.send(addr.to_string())?;
        Ok(())
    }

    pub async fn recv(&mut self) -> String {
        self.rx.recv().await.unwrap()
    }
}

pub type ClientDropGuard = Guard<ClientDrop>;

impl ClientDropGuard {
    pub fn init() -> Self {
        ClientDropGuard {
            inner: Arc::new(UnsafeCell::new(ClientDrop::new())),
        }
    }

    pub async fn send(&self, addr: &str) -> Result<()> {
        self.get().send(addr).await
    }

    pub async fn recv(&self) -> String {
        self.get_mut().recv().await
    }
}

pub struct Guard<T> {
    inner: Arc<UnsafeCell<T>>,
}

unsafe impl<T> Send for Guard<T> {}
unsafe impl<T> Sync for Guard<T> {}

impl<T> Guard<T>
where
    T: Sync + Send,
{
    pub fn new(t: T) -> Self
    where
        T: Sync + Send,
    {
        Guard {
            inner: Arc::new(UnsafeCell::new(t)),
        }
    }

    pub fn get(&self) -> &T {
        unsafe { self.inner.get().as_ref() }.unwrap()
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_mut(&self) -> &mut T {
        unsafe { self.inner.get().as_mut() }.unwrap()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Self {
        Guard {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        let t = unsafe { self.inner.get().as_ref() }.unwrap();
        let type_name = type_of(t);
        debug!("drop the guard of type: {type_name}")
    }
}

// struct ULID {
//     inner: Ulid,
// }

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::Guard;
    use futures::future::join_all;
    use tokio::{self};

    struct ConcurrentUnsafe {
        a: u64,
    }

    struct ConcurrentSafe {
        a: AtomicU64,
    }

    // unsafe impl Sync for SS {}
    // unsafe impl Send for SS {}

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn test_guard_concurrent() {
        let num = 1_000_000_u64;
        let guard_safe = Guard::new(ConcurrentSafe {
            a: AtomicU64::new(0),
        });
        let guard_unsafe = Guard::new(ConcurrentUnsafe { a: 0 });

        let mut handlers_safe = vec![];
        let mut handlers_unsafe = vec![];
        for i in 0..num {
            let g_safe = guard_safe.clone();
            handlers_safe.push(tokio::spawn(async move {
                g_safe
                    .get_mut()
                    .a
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }));

            let g_unsafe = guard_unsafe.clone();
            handlers_unsafe.push(tokio::spawn(async move {
                g_unsafe.get_mut().a += 1;
            }));
        }

        let result_list_safe = join_all(handlers_safe).await;
        for v in result_list_safe {
            if let Err(e) = v {
                eprintln!("safe: {e}");
            }
        }

        let result_list_unsafe = join_all(handlers_unsafe).await;
        for v in result_list_unsafe {
            if let Err(e) = v {
                eprintln!("unsafe: {e}");
            }
        }

        assert_eq!(
            guard_safe
                .get()
                .a
                .load(std::sync::atomic::Ordering::Acquire)
                == num,
            true,
        );

        assert_eq!(guard_unsafe.get().a != num, true,)
    }
}
