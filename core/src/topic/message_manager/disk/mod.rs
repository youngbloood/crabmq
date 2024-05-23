pub mod defer;
pub mod instant;

use self::{defer::DeferMessageMeta, instant::InstantMessageMeta};
use super::{MessageManager, MessageQueue};
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_dir, check_and_create_filename, check_exist, is_debug},
};
use parking_lot::RwLock;
use std::{fs, path::PathBuf, pin::Pin, vec};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt as _,
    select,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';

pub struct MessageQueueDisk {
    dir: PathBuf,

    instant: Manager<InstantMessageMeta>,

    defer: Manager<DeferMessageMeta>,

    tx: Sender<Message>,
    rx: Receiver<Message>,
    cancel: CancellationToken,
}

unsafe impl Send for MessageQueueDisk {}
unsafe impl Sync for MessageQueueDisk {}

impl MessageQueue for MessageQueueDisk {
    async fn push(&mut self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            self.defer.push(msg).await?;
            return Ok(());
        }
        self.instant.push(msg).await?;
        Ok(())
    }

    async fn pop(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    fn stop(&mut self) {
        self.cancel.cancel();
    }
}

pub async fn build_disk_queue(
    dir: PathBuf,
    max_msg_num_per_file: u64,
    max_size_per_file: u64,
) -> Result<MessageManager> {
    let mut disk_queue = MessageQueueDisk::new(dir, max_msg_num_per_file, max_size_per_file);
    disk_queue.load().await?;

    let guard: Guard<MessageQueueDisk> = Guard::new(disk_queue);
    let guard_ret = guard.clone();
    tokio::spawn(load_cache(guard));

    Ok(MessageManager::new(None, Some(guard_ret)))
}

fn calc_cache_length(all_len: usize) -> usize {
    let get_num: usize;
    if all_len < 100 {
        get_num = all_len;
    } else if all_len >= 100 && all_len < 10_000 {
        get_num = 100 + all_len / 100;
    } else if all_len >= 10_000 && all_len < 100_000 {
        get_num = 1000 + all_len / 1000;
    } else {
        get_num = 10000 + all_len / 10_000;
    }
    get_num
}

/// Load cache message from disk. In order to pop message from disk quickly.
async fn load_cache(guard: Guard<MessageQueueDisk>) {
    let sender = guard.get_mut().tx.clone();

    loop {
        select! {
            _ = CANCEL_TOKEN.cancelled() => {
                return ;
            }

            defer_cache = guard.get_mut().defer.meta.read_to_cache() => {
                match defer_cache{
                    Ok(list) => {
                        let mut iter = list.iter();
                        while let Some(m) = iter.next(){
                            let _ = sender.send(m.clone()).await;
                        }
                    }
                    Err(e) => {

                    }
                }
            }

            defer_cache = guard.get_mut().instant.meta.read_to_cache() => {
                match defer_cache{
                    Ok(list) => {
                        let mut iter = list.iter();
                        while let Some(m) = iter.next(){
                            let _ = sender.send(m.clone()).await;
                        }
                    }
                    Err(e) => {

                    }
                }
            }
        }
    }
}

impl MessageQueueDisk {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        let parent = dir.clone();
        let (tx, rx) = mpsc::channel(1000);

        MessageQueueDisk {
            dir: dir,
            tx,
            rx,
            instant: Manager::new(
                parent.join("instant"),
                InstantMessageMeta::new(parent.join("instant").to_str().unwrap()),
                max_msg_num_per_file,
                max_size_per_file,
            ),
            defer: Manager::new(
                parent.join("defer"),
                DeferMessageMeta::new(parent.join("defer").to_str().unwrap()),
                max_msg_num_per_file,
                max_size_per_file,
            ),
            cancel: CancellationToken::new(),
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        self.instant.load().await?;
        self.defer.load().await?;
        Ok(())
    }

    pub async fn persist(&mut self) -> Result<()> {
        self.instant.persist().await?;
        self.defer.persist().await?;
        Ok(())
    }
}

pub struct Manager<T: MetaManager> {
    /// 消息存放目录
    dir: PathBuf,

    /// 消息存放文件前缀
    prefix: String,

    /// 文件rorate因子
    factor: u64,

    /// 读取消息位置(读取的位置)
    meta: T,

    /// 每个文件最大消息数量
    max_msg_num_per_file: u64,

    /// 每个文件最多存放的大小
    max_size_per_file: u64,

    /// 读取消息池
    read_pool: Vec<ProtocolBody>,

    /// 写入消息
    writer: MessageDisk,
}

impl<T> Manager<T>
where
    T: MetaManager,
{
    pub fn new(dir: PathBuf, t: T, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        Manager {
            dir,
            prefix: String::new(),
            factor: 0,
            meta: t,
            max_msg_num_per_file,
            max_size_per_file,
            read_pool: Vec::new(),
            writer: MessageDisk::new(),
        }
    }

    pub async fn push(&mut self, msg: Message) -> Result<()> {
        if self.writer.msg_num >= self.max_msg_num_per_file
            || self.writer.msg_size + msg.calc_len() as u64 > self.max_size_per_file
        {
            let dir = self.dir.join(&self.prefix);
            self.writer.persist(dir.to_str().unwrap()).await?;
            self.rorate()?;
        }

        if msg.is_defer() {
            let id = msg.id();
            self.meta.update((
                self.factor,
                self.writer.msg_size,
                msg.calc_len() as _,
                id,
                msg.defer_time(),
            ));
        } else {
            self.meta.update((0, 0, 0, "", 1));
        }
        self.writer.push(msg);
        Ok(())
    }

    pub async fn read(&mut self) -> Option<Message> {
        None
    }

    pub fn rorate(&mut self) -> Result<()> {
        self.factor += 1;
        self.writer = MessageDisk::new();
        self.writer.filename = self
            .dir
            .join(gen_filename(self.factor))
            .to_str()
            .unwrap()
            .to_string();
        check_and_create_filename(self.writer.filename.as_str())?;
        Ok(())
    }

    pub async fn load(&mut self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        self.meta.load()?;

        let rd = fs::read_dir(self.dir.to_str().unwrap())?;
        // u64::MAX: 922_3372_0368_5477_5807
        let mut max_factor = 0_u64;
        let mut file_iter = rd.into_iter();
        let mut cant_parse = vec![];
        while let Some(entry) = file_iter.next() {
            let de = entry?;
            let filename = de.file_name().into_string().unwrap();

            match filename.parse::<u64>() {
                Ok(factor) => {
                    if factor > max_factor {
                        max_factor = factor;
                    }
                }
                // 可能是其他文件，忽略掉
                Err(_) => {
                    let fname = self.dir.join(filename);
                    cant_parse.push(fname.to_str().unwrap().to_string());
                }
            }
        }
        if cant_parse.len() != 0 {
            warn!("files{cant_parse:?} not standard message file, ignore them.")
        }

        // 初始化Manager.factor和Manager.writer，为下次写入消息时发生文件滚动做准备
        self.factor = max_factor;
        self.writer.filename = self
            .dir
            .join(gen_filename(self.factor))
            .to_str()
            .unwrap()
            .to_string();

        if check_exist(self.writer.filename.as_str()) {
            self.writer.load().await?;
        }

        Ok(())
    }

    pub async fn persist(&mut self) -> Result<()> {
        let dir = self.dir.join(&self.prefix);
        self.meta.persist()?;
        self.writer.persist(dir.to_str().unwrap()).await?;
        Ok(())
    }
}

/// 普通消息，一个消息对应一个文件
pub struct MessageDisk {
    filename: String,
    msg_size: u64,
    msg_num: u64,
    msgs: RwLock<Vec<Message>>,

    fd: Option<RwLock<File>>,
}

impl MessageDisk {
    pub fn new() -> Self {
        MessageDisk {
            filename: String::new(),
            msg_size: 0,
            msg_num: 0,
            msgs: RwLock::new(vec![]),
            fd: None,
        }
    }

    pub fn push(&mut self, msg: Message) {
        self.msg_size += msg.calc_len() as u64;
        self.msg_num += 1;
        let mut rg = self.msgs.write();
        rg.push(msg);
    }

    async fn init_fd(&mut self) -> Result<()> {
        if self.fd.is_some() {
            return Ok(());
        }
        let mut oo: OpenOptions = OpenOptions::new();
        let fd = oo
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(self.filename.as_str())
            .await?;
        self.fd = Some(RwLock::new(fd));
        Ok(())
    }

    pub async fn load(&mut self) -> Result<()> {
        self.init_fd().await?;
        let fd = self.fd.as_mut().unwrap().get_mut();
        let mut pfd = Pin::new(fd);
        loop {
            let head: ProtocolHead;
            match ProtocolHead::parse_from(&mut pfd).await {
                Ok(h) => {
                    self.msg_size += h.calc_len() as u64;
                    head = h;
                }
                Err(e) => {
                    if e.to_string().contains("eof") {
                        break;
                    }
                    return Err(anyhow!(e));
                }
            }
            match ProtocolBody::parse_from(&mut pfd).await {
                Ok(body) => {
                    self.msg_size += body.calc_len() as u64;
                    self.msg_num += 1;
                    if is_debug() {
                        let msg = Message::with_one(head, body);
                        println!("msg = {msg:?}");
                    }
                    // 不push到msgs中
                }
                Err(e) => {
                    if e.to_string().contains("eof") {
                        break;
                    }
                    return Err(anyhow!(e));
                }
            }
        }

        Ok(())
    }

    pub async fn persist(&mut self, dir: &str) -> Result<()> {
        {
            let rg = self.msgs.read();
            if rg.len() == 0 {
                return Ok(());
            }
        }
        self.init_fd().await?;

        let mut wg = self.msgs.write();
        let mut iter = wg.iter();

        let mut bts = vec![];
        while let Some(mu) = iter.next() {
            bts.append(&mut mu.as_bytes());
        }

        match self.fd.as_ref() {
            Some(fd) => fd.write().write(&bts).await?,
            None => return Err(anyhow!("not found file handler")),
        };
        wg.clear();

        Ok(())
    }
}

trait MetaManager {
    fn new(dir: &str) -> Self;
    async fn consume(&mut self) -> Result<()>;
    async fn read_to_cache(&mut self) -> Result<Vec<Message>>;
    fn load(&mut self) -> Result<()>;
    fn update(&mut self, args: (u64, u64, u64, &str, u64));
    fn persist(&self) -> Result<()>;
    fn meta_filename(&self) -> String;
}

pub fn gen_filename(factor: u64) -> String {
    format!("{:0>15}", factor)
}

#[cfg(test)]
pub mod tests {
    use super::MessageQueueDisk;
    use crate::{
        message::Message,
        protocol::{ProtocolBody, ProtocolHead},
        topic::message_manager::{
            disk::{defer::DeferMessageMeta, instant::InstantMessageMeta, MetaManager as _},
            MessageQueue as _,
        },
    };
    use bytes::Bytes;
    use std::path::Path;

    #[test]
    fn test_factor_file() {
        let filename = format!("{:0>15}", 111);
        assert_eq!(filename, "000000000000111");
    }

    fn init_log() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    async fn load_mm() -> MessageQueueDisk {
        let p: &Path = Path::new("../target");
        let mut mm = MessageQueueDisk::new(p.join("message"), 10, 299);
        match mm.load().await {
            Ok(_) => mm,
            Err(e) => panic!("{e:?}"),
        }
    }

    #[tokio::test]
    async fn test_message_manager_load() {
        load_mm().await;
    }

    #[tokio::test]
    async fn test_message_manager_defer_push_and_persist() {
        let mut mm = load_mm().await;
        let mut head = ProtocolHead::new();
        assert_eq!(head.set_version(1).is_ok(), true);

        for i in 0..10 {
            let mut body = ProtocolBody::new();
            // 设置id
            assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
            body.with_ack(true)
                .with_defer_time(100 + i)
                .with_not_ready(false)
                .with_persist(true);
            assert_eq!(
                body.with_body(Bytes::from_iter(format!("2000000{i}").bytes()))
                    .is_ok(),
                true
            );
            assert_eq!(
                mm.push(Message::with_one(head.clone(), body)).await.is_ok(),
                true
            );
        }

        assert_eq!(mm.persist().await.is_ok(), true);
    }

    #[tokio::test]
    async fn test_message_manager_defer_consume() {
        init_log();
        let mut mm = load_mm().await;
        let pre = mm.defer.meta.list.len();
        assert_eq!(mm.defer.meta.consume().await.is_ok(), true);
        let post = mm.defer.meta.list.len();
        assert_eq!(post + 1, pre);
    }

    #[tokio::test]
    async fn test_message_manager_defer_read_to_cache() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.defer.meta.read_to_cache().await.is_ok(), true);
    }

    #[tokio::test]
    async fn test_message_manager_defer_read_to_cache_repeat() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.defer.meta.read_to_cache().await.is_ok(), true);

        let cache = mm.defer.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len(), 0);

        let cache = mm.defer.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_message_manager_defer_read_to_cache_and_consume() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.defer.meta.read_to_cache().await.is_ok(), true);

        // consume 1条消息，read_to_cache时会再加载额外1条消息（前提是至少还有1条消息未被消费）
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        let cache = mm.defer.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len() <= 1, true);

        // consume 5条消息，read_to_cache时会再加载额外5条消息（前提是至少还有5条消息未被消费）
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        let cache = mm.defer.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len() <= 5, true)
    }

    #[tokio::test]
    async fn test_message_manager_push_instant_and_persist() {
        let mut mm = load_mm().await;
        let mut head = ProtocolHead::new();
        assert_eq!(head.set_version(1).is_ok(), true);
        for i in 0..10 {
            let mut body = ProtocolBody::new();
            // 设置id
            assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
            body.with_ack(true).with_not_ready(false).with_persist(true);
            assert_eq!(
                body.with_body(Bytes::from_iter(format!("2000000{i}").bytes()))
                    .is_ok(),
                true
            );

            assert_eq!(
                mm.push(Message::with_one(head.clone(), body)).await.is_ok(),
                true
            );
        }

        assert_eq!(mm.persist().await.is_ok(), true);
    }

    #[tokio::test]
    async fn test_message_manager_push_instant_consume() {
        init_log();
        let mut mm = load_mm().await;
        let pre = mm.instant.meta.now.left_num;
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        let post = mm.instant.meta.now.left_num;
        assert_eq!(post + 1, pre);
    }

    #[tokio::test]
    async fn test_message_manager_instant_read_to_cache() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.instant.meta.read_to_cache().await.is_ok(), true);
    }

    #[tokio::test]
    async fn test_message_manager_instant_read_to_cach_repeat() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.instant.meta.read_to_cache().await.is_ok(), true);
        let cache = mm.instant.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len(), 0);
        let cache = mm.instant.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_message_manager_instant_read_to_cache_and_consume() {
        init_log();
        let mut mm = load_mm().await;

        assert_eq!(mm.instant.meta.read_to_cache().await.is_ok(), true);

        // consume 1条消息，read_to_cache时会再加载额外1条消息（前提是至少还有1条消息未被消费）
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.persist().is_ok(), true);
        let cache = mm.instant.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len() <= 1, true);

        // consume 5条消息，read_to_cache时会再加载额外5条消息（前提是至少还有5条消息未被消费）
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.consume().await.is_ok(), true);
        assert_eq!(mm.instant.meta.persist().is_ok(), true);
        let cache = mm.instant.meta.read_to_cache().await;
        assert_eq!(cache.is_ok(), true);
        assert_eq!(cache.unwrap().len() <= 5, true);
    }
}
