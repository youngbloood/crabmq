use anyhow::Result;
use bytes::Bytes;

// TODO：持久化到硬盘
trait DiskQueue {
    fn put(&self, bts: Bytes) -> Result<()>;
    fn read(&self) -> Bytes;
    fn close(&self) -> Result<()>;
    fn delete(&mut self) -> Result<()>;
    fn depth(&self) -> i64;
    fn empty(&self) -> Result<()>;
}
