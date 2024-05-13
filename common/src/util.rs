use std::time::Duration;

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

// use parking_lot::RwLock;
// use std::{borrow::BorrowMut, cell::UnsafeCell};

// pub struct AtomicValue<T> {
//     mutex: RwLock<()>,
//     inner: UnsafeCell<T>,
// }

// impl<T> AtomicValue<T>
// where
//     T: Default + Copy,
// {
//     pub fn new(t: T) -> Self {
//         AtomicValue {
//             mutex: RwLock::new(()),
//             inner: UnsafeCell::new(t),
//         }
//     }

//     pub fn load(&self) -> T {
//         self.mutex.read();
//         let c = *self.inner.get_mut().cl;
//         c
//     }

//     pub fn store(&mut self, v: T) -> T {
//         self.mutex.write();
//         let old_v = self.inner.into_inner();
//         self.inner = UnsafeCell::new(v);

//         old_v
//     }

//     pub fn action(&mut self, mut callback: impl FnMut(&mut T)) {
//         self.mutex.write();
//         callback(self.inner.get_mut());
//     }
// }
