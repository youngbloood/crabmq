use super::{
    message_manager::MessageManager,
    record::{
        FdCache, NormalPtr, RecordManager, RecordManagerStrategy as _, RecordManagerStrategyNormal,
    },
};
use anyhow::{anyhow, Result};
use protocol::message::{Message, MessageOperation as _};

use std::path::PathBuf;

pub struct Instant {
    dir: PathBuf,

    ready_record_manager: RecordManager<RecordManagerStrategyNormal>,
    not_ready_record_manager: RecordManager<RecordManagerStrategyNormal>,
    delete_record_manager: RecordManager<RecordManagerStrategyNormal>,

    message_manager: MessageManager,

    read_ptr: NormalPtr,
    consume_ptr: NormalPtr,
}

impl Instant {
    pub fn new(
        dir: PathBuf,
        max_msg_num_per_file: u64,
        max_size_per_file: u64,
        record_num_per_file: u64,
        record_size_per_file: u64,
        fd_cache_size: usize,
    ) -> Result<Self> {
        let dir = dir.join("instant");

        let fd_cache = FdCache::new(10);
        let mut ready_record_manager = RecordManagerStrategyNormal::new(
            dir.join("ready_record"),
            false,
            record_num_per_file,
            record_size_per_file,
            fd_cache_size,
        )?;
        ready_record_manager.with_fd_cache(fd_cache.clone());
        let not_ready_record_manager = RecordManagerStrategyNormal::new(
            dir.join("not_ready_record"),
            false,
            record_num_per_file,
            record_size_per_file,
            fd_cache_size,
        )?;
        let delete_record_manager = RecordManagerStrategyNormal::new(
            dir.join("delete_record"),
            false,
            record_num_per_file,
            record_size_per_file,
            fd_cache_size,
        )?;
        let message_manager = MessageManager::new(
            dir.join("messages"),
            max_msg_num_per_file,
            max_size_per_file,
        );
        let read_ptr = NormalPtr::new(dir.join("meta"), fd_cache.clone());
        let consume_ptr = NormalPtr::new(dir.join("meta"), fd_cache.clone());

        Ok(Instant {
            dir,

            ready_record_manager: RecordManager::new(ready_record_manager),
            not_ready_record_manager: RecordManager::new(not_ready_record_manager),
            delete_record_manager: RecordManager::new(delete_record_manager),

            message_manager,

            read_ptr,
            consume_ptr,
        })
    }

    pub async fn load(&self) -> Result<()> {
        self.ready_record_manager.load().await?;
        self.not_ready_record_manager.load().await?;
        self.delete_record_manager.load().await?;
        self.message_manager.load().await?;
        self.consume_ptr.load()?;
        self.read_ptr.load()?;
        Ok(())
    }

    pub async fn set_not_ready(&self, id: &str, not_ready: bool) -> Result<()> {
        if not_ready {
            if let Some((_, src)) = self.ready_record_manager.strategy.find(id).await? {
                self.not_ready_record_manager.strategy.push(src).await?;
                return Ok(());
            }
            return Err(anyhow!("not found the record: id[{id}]"));
        }

        if let Some((_, src)) = self.not_ready_record_manager.strategy.find(id).await? {
            self.ready_record_manager.strategy.push(src).await?;
            return Ok(());
        }

        Err(anyhow!("not found the record: id[{id}]"))
    }

    pub async fn handle_msg(&self, msg: Message) -> Result<()> {
        let is_delete = msg.is_deleted();
        let is_consume = msg.is_consumed();

        if is_delete {
            self.delete(msg.get_id()).await?;
            return Ok(());
        }
        if is_consume {
            self.consume(msg.get_id()).await?;
            return Ok(());
        }

        let not_ready = msg.is_notready();
        let record = self.message_manager.push(msg).await?;

        if not_ready {
            self.not_ready_record_manager.push(record).await?;
        } else {
            let (record_filename, index) = self.ready_record_manager.push(record).await?;
            if self.consume_ptr.is_empty() {
                self.consume_ptr.set(record_filename.clone(), index)?;
            }
            if self.read_ptr.is_empty() {
                self.read_ptr.set(record_filename, index)?;
            }
        }

        Ok(())
    }

    /// seek a message from Storage.
    pub async fn seek(&self) -> Result<Option<Message>> {
        let record = self.read_ptr.seek()?;
        if let Some(record) = record {
            let msg = self.message_manager.find_by(record).await?;
            return Ok(msg);
        }

        Ok(None)
    }

    /// pop a message from Storage, then the read_ptr will rorate.
    pub async fn pop(&self) -> Result<Option<Message>> {
        let record = self.read_ptr.next()?;
        if let Some(record) = record {
            let msg = self.message_manager.find_by(record).await?;
            return Ok(msg);
        }

        Ok(None)
    }

    pub async fn flush(&self) -> Result<()> {
        self.message_manager.persist().await?;
        self.ready_record_manager.persist().await?;
        self.not_ready_record_manager.persist().await?;
        self.delete_record_manager.persist().await?;
        self.consume_ptr.persist()?;
        // self.read_ptr.persist()?;
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        if let Some((_, src)) = self.ready_record_manager.strategy.find(id).await? {
            self.delete_record_manager.strategy.push(src).await?;
        }

        if let Some((_, src)) = self.not_ready_record_manager.strategy.find(id).await? {
            self.delete_record_manager.strategy.push(src).await?;
        }

        Ok(())
    }

    async fn consume(&self, id: &str) -> Result<()> {
        if let Some((_, src)) = self.ready_record_manager.strategy.find(id).await? {
            self.delete_record_manager.strategy.push(src).await?;
        }

        if let Some((_, src)) = self.not_ready_record_manager.strategy.find(id).await? {
            self.delete_record_manager.strategy.push(src).await?;
        }

        Ok(())
    }

    pub async fn update_consume(&self, id: &str, consume: bool) -> Result<()> {
        Ok(())
    }

    pub async fn update_delete(&self, id: &str, delete: bool) -> Result<()> {
        Ok(())
    }

    pub async fn update_notready(&self, id: &str, notready: bool) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use common::util::{interval, random_str};
    use protocol::message::v1::MessageV1;
    use rand::Rng as _;
    use std::{path::Path, time::Duration};
    use tokio::select;

    #[test]
    fn test_instant_new() {
        let instant = Instant::new(
            Path::new("../target/topic1").to_path_buf(),
            10000,
            100000,
            10000,
            100000,
            20,
        );
    }

    #[tokio::test]
    async fn test_instant_load() {
        let instant = Instant::new(
            Path::new("../target/topic1").to_path_buf(),
            10000,
            100000,
            10000,
            100000,
            20,
        )
        .unwrap();
        assert!(instant.load().await.is_ok());
    }

    async fn get_instant(p: PathBuf) -> Instant {
        let instant = Instant::new(p, 10000, 100000, 10000, 100000, 20).unwrap();
        assert!(instant.load().await.is_ok());
        instant
    }

    #[tokio::test]
    async fn test_instant_push_and_flush() {
        let instant: Instant = get_instant(Path::new("../target/topic1").to_path_buf()).await;
        println!("load success");

        for i in 0..40 {
            let msg = MessageV1::default();
            assert!(instant.handle_msg(Message::V1(msg)).await.is_ok());
            if i / 3 == 0 {
                assert!(instant.flush().await.is_ok());
            }
        }

        assert!(instant.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_instant_meta_next() {
        let instant: Instant = get_instant(Path::new("../target/topic1").to_path_buf()).await;
        println!("load success");

        for _ in 0..20 {
            let result = instant.read_ptr.next();
            if result.is_err() {
                panic!("{:?}", result.unwrap());
            }
            assert!(result.is_ok());
            if let Some(record) = result.unwrap() {
                println!("record = {record:?}");
                match instant.message_manager.find_by(record).await {
                    Ok(msg_opt) => println!("msg_opt = {msg_opt:?}"),
                    Err(e) => eprintln!("err={e:?}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_instant_meta_seek() {
        let instant: Instant = get_instant(Path::new("../target/topic1").to_path_buf()).await;
        println!("load success");

        for i in 0..20 {
            let result = instant.read_ptr.seek();
            if result.is_err() {
                panic!("{:?}", result.unwrap());
            }
            assert!(result.is_ok());
            if let Some(record) = result.unwrap() {
                println!("record = {record:?}");
                match instant.message_manager.find_by(record).await {
                    Ok(msg_opt) => println!("msg_opt = {msg_opt:?}\n"),
                    Err(e) => eprintln!("err={e:?}"),
                }
            }

            if i % 2 == 0 {
                println!("\n\nrorate to next:\n");
                let _ = instant.read_ptr.next();
            }
        }
    }
}
