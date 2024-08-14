use super::{
    message_manager::MessageManager,
    record::{RecordManager, RecordManagerStrategy as _, RecordManagerStrategyTime, TimePtr},
};
use anyhow::{anyhow, Ok, Result};
use protocol::message::{Message, MessageOperation as _};

use std::path::PathBuf;

pub struct Defer {
    dir: PathBuf,

    ready_record_manager: RecordManager<RecordManagerStrategyTime>,
    not_ready_record_manager: RecordManager<RecordManagerStrategyTime>,
    delete_record_manager: RecordManager<RecordManagerStrategyTime>,

    message_manager: MessageManager,

    read_ptr: TimePtr,
    consume_ptr: TimePtr,
}

impl Defer {
    pub fn new(
        dir: PathBuf,
        template: &str,
        max_msg_num_per_file: u64,
        max_size_per_file: u64,
        fd_cache_size: usize,
    ) -> Result<Self> {
        let dir = dir.join("defer");
        let ready_record_manager = RecordManagerStrategyTime::new(
            dir.join("ready_record"),
            false,
            template,
            fd_cache_size,
        )?;
        let not_ready_record_manager = RecordManagerStrategyTime::new(
            dir.join("not_ready_record"),
            false,
            template,
            fd_cache_size,
        )?;
        let delete_record_manager = RecordManagerStrategyTime::new(
            dir.join("delete_record"),
            false,
            template,
            fd_cache_size,
        )?;
        let message_manager = MessageManager::new(
            dir.join("messages"),
            max_msg_num_per_file,
            max_size_per_file,
        );
        let read_ptr = TimePtr::new(dir.clone(), dir.join("meta"));
        let consume_ptr = TimePtr::new(dir.clone(), dir.join("meta"));

        Ok(Defer {
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
        self.ready_record_manager
            .persist()
            .await
            .expect("persist ready_record_manager failed");
        self.not_ready_record_manager
            .persist()
            .await
            .expect("persist not_read_record_manager failed");
        self.delete_record_manager
            .persist()
            .await
            .expect("persist delete_record_manager failed");
        self.message_manager
            .persist()
            .await
            .expect("persist message_manager failed");
        self.consume_ptr
            .persist()
            .expect("persist consume_ptr failed");
        // self.read_ptr.persist()?;
        Ok(())
    }

    pub async fn update_consume(&self, id: &str, consume: bool) -> Result<()> {
        self.ready_record_manager
            .update_consume_flag(id, consume)
            .await?;
        self.not_ready_record_manager
            .update_consume_flag(id, consume)
            .await?;

        Ok(())
    }

    pub async fn update_delete(&self, id: &str, delete: bool) -> Result<()> {
        self.ready_record_manager
            .update_delete_flag(id, delete)
            .await?;
        self.not_ready_record_manager
            .update_delete_flag(id, delete)
            .await?;
        Ok(())
    }

    pub async fn update_notready(&self, id: &str, notready: bool) -> Result<()> {
        if notready {
            let record = self.ready_record_manager.find(id).await?;
            if record.is_none() {
                return Ok(());
            }
            let (_, record) = record.unwrap();
            self.not_ready_record_manager.push(record).await?;
            self.ready_record_manager.delete(id).await?;
            return Ok(());
        }
        let record = self.not_ready_record_manager.find(id).await?;
        if record.is_none() {
            return Ok(());
        }
        let (_, record) = record.unwrap();
        self.ready_record_manager.push(record).await?;
        self.not_ready_record_manager.delete(id).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::util::interval;
    use protocol::message::v1::MessageV1;
    use std::result::Result::Ok;
    use std::{path::Path, time::Duration};

    #[tokio::test]
    async fn test_defer_load() {
        let defer = Defer::new(
            Path::new("../target/topic1").to_path_buf(),
            "{daily}/{hourly}/{minutely:5}",
            10000,
            100000,
            100,
        )
        .unwrap();
        assert!(defer.load().await.is_ok());
    }

    async fn get_defer(p: PathBuf) -> Defer {
        let defer = Defer::new(
            p,
            "{daily}/{hourly}hour/mini{minutely:5}",
            10000,
            100000,
            100,
        )
        .expect("generate defer failed");
        assert!(defer.load().await.is_ok());
        defer
    }

    #[tokio::test]
    async fn test_defer_push_and_flush() {
        let defer = get_defer(Path::new("../target/topic1").to_path_buf()).await;
        println!("load success");

        let mut ticker = interval(Duration::from_secs(1)).await;
        for i in 0..600 {
            let mut msg = MessageV1::default();
            assert!(defer.handle_msg(Message::V1(msg)).await.is_ok());
            if i / 3 == 0 {
                assert!(defer.flush().await.is_ok());
            }
            ticker.tick().await;
        }

        assert!(defer.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_defer_meta_next() {
        let defer = get_defer(Path::new("../target/topic1").to_path_buf()).await;
        for _ in 0..600 {
            let result = defer.read_ptr.next();
            if result.is_err() {
                panic!("{:?}", result.unwrap());
            }
            assert!(result.is_ok());
            if let Some(record) = result.unwrap() {
                println!("record = {record:?}");

                match defer.message_manager.find_by(record).await {
                    Ok(msg_opt) => println!("msg_opt = {msg_opt:?}"),
                    Err(e) => eprintln!("err={e:?}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_defer_meta_seek() {
        let defer = get_defer(Path::new("../target/topic1").to_path_buf()).await;
        for i in 0..600 {
            let result = defer.read_ptr.seek();
            if result.is_err() {
                panic!("{:?}", result.unwrap());
            }
            assert!(result.is_ok());
            if let Some(record) = result.unwrap() {
                println!("record = {record:?}");

                match defer.message_manager.find_by(record).await {
                    Ok(msg_opt) => println!("msg_opt = {msg_opt:?}"),
                    Err(e) => eprintln!("err={e:?}"),
                }
            }
            if i % 2 == 0 {
                println!("rorate to next\n\n");
                let _ = defer.read_ptr.next();
            }
        }
    }
}
