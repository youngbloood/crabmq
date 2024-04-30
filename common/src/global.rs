use std::{borrow::BorrowMut, cell::RefCell, sync::Arc};

use anyhow::*;
use futures::executor::block_on;
use lazy_static::*;
use snowflake::SnowflakeIdBucket;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::ArcMux;

lazy_static! {
    // 全局cancel信号
    pub static ref CANCEL_TOKEN: CancellationToken = CancellationToken::new();
    //
    pub static ref STOPPED : Error=anyhow!("process had stopped");

    /// 全局的snowflake id生成器
    ///
    /// SnowflakeIdBucket具有buffer
    pub static ref SNOWFLAKE: SnowFlake = SnowFlake::new(1, 1);
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
