use super::{
    message_manager::MessageManager,
    record::{
        FdCache, NormalPtr, RecordManager, RecordManagerStrategy as _, RecordManagerStrategyNormal,
    },
};
use crate::message::Message;
use anyhow::{anyhow, Result};
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
    pub fn new(dir: PathBuf) -> Result<Self> {
        let dir = dir.join("instant");

        let fd_cache = FdCache::new(10);
        let mut ready_record_manager =
            RecordManagerStrategyNormal::new(dir.join("ready_record"), false, 10, 10000, 20)?;
        ready_record_manager.with_fd_cache(fd_cache.clone());
        let not_ready_record_manager =
            RecordManagerStrategyNormal::new(dir.join("not_ready_record"), false, 1000, 10000, 20)?;
        let delete_record_manager =
            RecordManagerStrategyNormal::new(dir.join("delete_record"), false, 1000, 10000, 20)?;
        let message_manager = MessageManager::new(dir.join("messages"), 100010, 10000);
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
            self.delete(msg.id()).await?;
            return Ok(());
        }
        if is_consume {
            self.consume(msg.id()).await?;
            return Ok(());
        }

        let not_ready = msg.is_not_ready();
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
        self.ready_record_manager.persist().await?;
        self.not_ready_record_manager.persist().await?;
        self.delete_record_manager.persist().await?;
        self.message_manager.persist().await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ProtocolBody, ProtocolHead};
    use bytes::Bytes;
    use common::util::random_str;
    use rand::Rng as _;
    use std::path::Path;

    #[test]
    fn test_instant_new() {
        let instant = Instant::new(Path::new("../target/topic1").to_path_buf());
    }

    #[tokio::test]
    async fn test_instant_load() {
        let instant = Instant::new(Path::new("../target/topic1").to_path_buf()).unwrap();
        assert!(instant.load().await.is_ok());
    }

    async fn get_instant(p: PathBuf) -> Instant {
        let instant = Instant::new(p).unwrap();
        assert!(instant.load().await.is_ok());
        instant
    }

    #[tokio::test]
    async fn test_instant_push_and_flush() {
        let instant: Instant = get_instant(Path::new("../target/topic1").to_path_buf()).await;
        println!("load success");
        let mut head = ProtocolHead::new();
        assert!(head.set_topic("default").is_ok());
        assert!(head.set_channel("channel-name").is_ok());
        assert!(head.set_version(1).is_ok());

        for i in 0..40 {
            let mut body = ProtocolBody::new();
            // 设置id
            assert!(body.with_id((i + 1000).to_string().as_str()).is_ok());
            body.with_ack(true).with_not_ready(false).with_persist(true);
            body.with_not_ready(i % 2 == 0);

            let mut rng = rand::thread_rng();
            let length = rng.gen_range(5..50);
            let body_str = random_str(length as _);
            assert!(body
                .with_body(Bytes::copy_from_slice(body_str.as_bytes()))
                .is_ok());

            assert!(instant
                .handle_msg(Message::with_one(head.clone(), body))
                .await
                .is_ok());
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
