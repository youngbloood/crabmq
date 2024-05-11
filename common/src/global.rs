use crate::{util::type_of, ArcMux};
use anyhow::*;
use futures::executor::block_on;
use lazy_static::*;
use snowflake::SnowflakeIdBucket;
use std::{cell::UnsafeCell, sync::Arc};
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    Mutex,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

lazy_static! {
    // 全局cancel信号
    pub static ref CANCEL_TOKEN: CancellationToken = CancellationToken::new();
    //
    pub static ref STOPPED : Error=anyhow!("process had stopped");

    /// 全局的snowflake id生成器
    ///
    /// SnowflakeIdBucket具有buffer
    pub static ref SNOWFLAKE: SnowFlake = SnowFlake::new(1, 1);

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
        block_on(self.inner.lock()).get_id()
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

    pub fn get_mut(&self) -> &mut T {
        unsafe { self.inner.get().as_mut() }.unwrap()
    }

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
