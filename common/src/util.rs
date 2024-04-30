use anyhow::Result;
use lazy_static::*;

use snowflake::SnowflakeIdBucket;
use std::sync::atomic::{AtomicU64 as AU64, Ordering};
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
};

use crate::global::CANCEL_TOKEN;
pub struct AtomicU64 {
    inner: AU64,
    order: Ordering,
}

impl AtomicU64 {
    pub fn new() -> Self {
        AtomicU64 {
            inner: AU64::default(),
            order: Ordering::Relaxed,
        }
    }

    pub fn set_order(&mut self, order: Ordering) -> &mut Self {
        self.order = order;
        self
    }

    pub fn load(&self) -> u64 {
        self.inner.load(self.order)
    }

    pub fn store(&mut self, v: u64) {
        self.inner.store(v, self.order)
    }

    pub fn swap(&mut self, v: u64) -> u64 {
        self.inner.swap(v, self.order)
    }

    /// Adds to the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    pub fn increase(&mut self) -> u64 {
        self.inner.fetch_add(1, self.order)
    }

    /// Adds to the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    pub fn increase_with(&mut self, v: u64) -> u64 {
        self.inner.fetch_add(v, self.order)
    }

    /// Subtracts from the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    pub fn decrease(&mut self) -> u64 {
        self.inner.fetch_sub(1, self.order)
    }

    /// Subtracts from the current value, returning the previous value.
    ///
    /// This operation wraps around on overflow.
    pub fn decrease_with(&mut self, v: u64) -> u64 {
        self.inner.fetch_sub(v, self.order)
    }

    pub fn ge(&self, target: u64) -> bool {
        self.load() >= target
    }

    pub fn gt(&self, target: u64) -> bool {
        self.load() > target
    }

    pub fn eq(&self, target: u64) -> bool {
        self.load() == target
    }

    pub fn le(&self, target: u64) -> bool {
        self.load() <= target
    }

    pub fn lt(&self, target: u64) -> bool {
        self.load() < target
    }
}
