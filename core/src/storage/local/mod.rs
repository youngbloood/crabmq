mod defer;
mod instant;
mod record;

use self::{defer::DeferMessageMeta, instant::InstantMessageMeta};
use super::{storage::StorageOperation, TopicOperation};
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
    tsuixuq::Tsuixuq,
};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use chrono::Local;
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_dir, check_and_create_filename, check_exist, interval, is_debug},
};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fs,
    io::SeekFrom,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
    vec,
};
use std::{path::Path, sync::atomic::Ordering::SeqCst};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt as _},
    select,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';

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

pub struct StorageLocal {
    dir: PathBuf,

    /// topic_name: TopicMessage
    topics: RwLock<HashMap<String, TopicMessage>>,

    max_msg_num_per_file: u64,
    max_size_per_file: u64,

    /// 取消信号
    cancel: CancellationToken,
}

unsafe impl Send for StorageLocal {}
unsafe impl Sync for StorageLocal {}

impl StorageLocal {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        StorageLocal {
            dir: dir,
            topics: RwLock::new(HashMap::new()),
            cancel: CancellationToken::new(),
            max_msg_num_per_file,
            max_size_per_file,
        }
    }
}

#[async_trait::async_trait]
impl StorageOperation for StorageLocal {
    async fn init(&self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;

        for entry in fs::read_dir(self.dir.to_str().unwrap())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let topic_name = entry.file_name();
            let parent = Path::new(self.dir.to_str().unwrap()).join(topic_name.to_str().unwrap());
            debug!("StorageLocal: load topic: {}", topic_name.to_str().unwrap());

            let instant = Manager::new(
                parent.join("instant"),
                InstantMessageMeta::new(parent.join("instant").to_str().unwrap()),
                self.max_msg_num_per_file,
                self.max_size_per_file,
            );
            instant.load().await?;

            let defer = Manager::new(
                parent.join("defer"),
                DeferMessageMeta::new(parent.join("defer").to_str().unwrap()),
                self.max_msg_num_per_file,
                self.max_size_per_file,
            );
            defer.load().await?;

            let mut wg = self.topics.write();
            wg.insert(
                topic_name.to_str().unwrap().to_string(),
                Guard::new(TopicMessageBase::new(
                    topic_name.to_str().unwrap(),
                    instant,
                    defer,
                )),
            );
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let rg = self.topics.read();
        let mut iter = rg.iter();
        while let Some((_, topic)) = iter.next() {
            topic.get().instant.persist().await?;
            topic.get().defer.persist().await?;
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }

    async fn get_or_create_topic(&self, topic_name: &str) -> Result<Arc<Box<dyn TopicOperation>>> {
        if !self.topics.read().contains_key(topic_name) {
            let parent = Path::new(self.dir.to_str().unwrap()).join(topic_name);
            debug!("StorageLocal: load topic: {}", topic_name);

            let instant = Manager::new(
                parent.join("instant"),
                InstantMessageMeta::new(parent.join("instant").to_str().unwrap()),
                self.max_msg_num_per_file,
                self.max_size_per_file,
            );
            instant.load().await?;

            let defer = Manager::new(
                parent.join("defer"),
                DeferMessageMeta::new(parent.join("defer").to_str().unwrap()),
                self.max_msg_num_per_file,
                self.max_size_per_file,
            );
            defer.load().await?;

            self.topics.write().insert(
                topic_name.to_string(),
                Guard::new(TopicMessageBase::new(topic_name, instant, defer)),
            );
        }
        let rg = self.topics.read();
        let topic = rg.get(topic_name).unwrap();
        Ok(Arc::new(Box::new(topic.clone()) as Box<dyn TopicOperation>))
    }
}

type TopicMessage = Guard<TopicMessageBase>;

struct TopicMessageBase {
    name: String,
    instant: Manager<InstantMessageMeta>,
    defer: Manager<DeferMessageMeta>,
}

impl TopicMessageBase {
    fn new(
        name: &str,
        instant: Manager<InstantMessageMeta>,
        defer: Manager<DeferMessageMeta>,
    ) -> Self {
        TopicMessageBase {
            name: name.to_string(),
            instant,
            defer,
        }
    }
}

#[async_trait::async_trait]
impl TopicOperation for TopicMessage {
    fn name(&self) -> &str {
        self.get().name.as_str()
    }

    async fn next_defer(&self) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() =>{
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.get().defer.meta.get_mut().next().await {
                        Ok(msg) => {
                            match msg {
                                Some(msg) => {
                                    return Ok(Some(msg));
                                }
                                None => {
                                    select! {
                                        _ = CANCEL_TOKEN.cancelled() =>{
                                            return Err(anyhow!("process stopped"))
                                        }
                                        _ = ticker.tick() => {
                                            return Ok(None)
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                } => {
                    match msg {
                        Ok(msg) => {
                           if let Some(m) = msg {
                                return Ok(Some(m));
                           }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                }
            }
        }
    }

    async fn next_instant(&self) -> Result<Option<Message>> {
        let mut ticker = interval(Duration::from_secs(1)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() =>{
                    return Err(anyhow!("process stopped"))
                }

                msg = async {
                    match self.get().instant.meta.get_mut().next().await {
                        Ok((msg,_)) => {
                            match msg {
                                Some(msg) => {
                                    return Ok(Some(msg));
                                }
                                None => {
                                    select! {
                                        _ = CANCEL_TOKEN.cancelled() =>{
                                            return Err(anyhow!("process stopped"))
                                        }
                                        _ = ticker.tick() => {
                                            return Ok(None)
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                } => {
                    match msg {
                        Ok(msg) => {
                           if let Some(m) = msg {
                                return Ok(Some(m));
                           }
                        }
                        Err(e) => {
                            return Err(anyhow!(e));
                        }
                    }
                }
            }
        }
    }

    async fn push(&self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            self.get().defer.push(msg).await?;
            return Ok(());
        }
        self.get().instant.push(msg).await?;
        Ok(())
    }
}

pub struct Manager<T: MetaManager> {
    /// 消息存放目录
    dir: PathBuf,

    /// 文件rorate因子
    factor: AtomicU64,

    /// meta信息管理
    meta: Guard<T>,

    /// 每个文件最大消息数量
    max_msg_num_per_file: u64,

    /// 每个文件最多存放的大小
    max_size_per_file: u64,

    /// 读取消息池
    read_pool: Vec<ProtocolBody>,

    /// 写入消息
    writer: RwLock<MessageDisk>,
}

impl<T> Manager<T>
where
    T: MetaManager,
{
    pub fn new(dir: PathBuf, t: T, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        Manager {
            dir,
            factor: AtomicU64::new(0),
            meta: Guard::new(t),
            max_msg_num_per_file,
            max_size_per_file,
            read_pool: Vec::new(),
            writer: RwLock::new(MessageDisk::new()),
        }
    }

    pub async fn push(&self, msg: Message) -> Result<()> {
        if self.writer.read().msg_num >= self.max_msg_num_per_file
            || self.writer.read().msg_size + msg.calc_len() as u64 > self.max_size_per_file
        {
            self.writer.write().persist().await?;
            self.rorate()?;
        }

        if msg.is_defer() {
            let id = msg.id();
            self.meta.get_mut().update((
                self.factor.load(SeqCst),
                self.writer.read().msg_size,
                msg.calc_len() as _,
                id,
                msg.defer_time(),
            ));
        } else {
            self.meta.get_mut().update((0, 0, 0, "", 1));
        }
        self.writer.write().push(msg);
        Ok(())
    }

    pub fn rorate(&self) -> Result<()> {
        self.factor.fetch_add(1, SeqCst);
        let next_filename = self
            .dir
            .join(gen_filename(self.factor.load(SeqCst)))
            .to_str()
            .unwrap()
            .to_string();
        let mut wg = self.writer.write();
        wg.reset_with_filename(next_filename.as_str());
        check_and_create_filename(wg.filename.as_str())?;
        Ok(())
    }

    pub async fn load(&self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        self.meta.get_mut().load()?;

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
        self.factor.store(max_factor, SeqCst);

        let mut wg = self.writer.write();
        wg.filename = self
            .dir
            .join(gen_filename(self.factor.load(SeqCst)))
            .to_str()
            .unwrap()
            .to_string();

        if check_exist(wg.filename.as_str()) {
            wg.load().await?;
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        if self.writer.read().msgs.read().len() == 0 {
            return Ok(());
        }
        self.meta.get_mut().persist()?;
        self.writer.write().persist().await?;
        Ok(())
    }
}

/// 普通消息，一个消息对应一个文件
///
/// 格式为<8-bytes msg-size><8-bytes msg-num><msg1><msg2>...<msgn>
pub struct MessageDisk {
    /// 写入的文件
    filename: String,

    /// 校验文件中的消息是否合法
    validate: bool,

    /// 持久化文件的句柄
    fd: Option<RwLock<File>>,

    /// push来的消存放在内存中
    msgs: RwLock<Vec<Message>>,

    /// 写入位置的offset
    write_offset: u64,

    /// 当前文件的消息大小（8byte）
    msg_size: u64,

    /// 当前文件消息数量（8byte）
    msg_num: u64,
}

impl MessageDisk {
    pub fn new() -> Self {
        MessageDisk {
            filename: String::new(),
            msg_size: 8 + 8,
            msg_num: 0,
            write_offset: 16,
            msgs: RwLock::new(vec![]),
            fd: None,
            validate: false,
        }
    }

    pub fn reset_with_filename(&mut self, filename: &str) {
        self.filename = filename.to_string();
        self.fd = None;
        self.msgs = RwLock::new(vec![]);
        self.write_offset = 16;
        self.msg_size = 16;
        self.msg_num = 0;
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
            .open(self.filename.as_str())
            .await?;
        self.fd = Some(RwLock::new(fd));
        Ok(())
    }

    pub async fn load(&mut self) -> Result<()> {
        self.init_fd().await?;
        debug!("load from filename: {}", self.filename.as_str());

        let fd = self.fd.as_mut().unwrap().get_mut();
        let mut pfd = Pin::new(fd);

        // load msg_size
        let mut num_buf = BytesMut::new();
        num_buf.resize(8, 0);
        pfd.read_exact(&mut num_buf).await?;
        self.msg_size = u64::from_be_bytes(
            num_buf
                .to_vec()
                .try_into()
                .expect("convert to u64 array failed"),
        );

        // load msg_num
        num_buf.resize(8, 0);
        pfd.read_exact(&mut num_buf).await?;
        self.msg_num = u64::from_be_bytes(
            num_buf
                .to_vec()
                .try_into()
                .expect("convert to u64 array failed"),
        );

        self.write_offset = self.msg_size;

        if !self.validate {
            return Ok(());
        }

        loop {
            let head: ProtocolHead;
            match ProtocolHead::parse_from(&mut pfd).await {
                Ok(h) => {
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

    pub async fn persist(&mut self) -> Result<()> {
        {
            let msgs_rg = self.msgs.read();
            if msgs_rg.len() == 0 {
                return Ok(());
            }
        }

        self.init_fd().await?;
        let mut msgs_wg = self.msgs.write();
        let mut iter = msgs_wg.iter();

        let mut bts = vec![];
        while let Some(mu) = iter.next() {
            bts.extend(mu.as_bytes());
        }

        match self.fd.as_ref() {
            Some(fd) => {
                let mut fwg = fd.write();

                // 写入msg_size
                fwg.seek(SeekFrom::Start(0)).await?;
                fwg.write_all(&self.msg_size.to_be_bytes()).await?;

                // 写入msg_num
                fwg.seek(SeekFrom::Start(8)).await?;
                fwg.write_all(&self.msg_num.to_be_bytes()).await?;

                // 在offset位置写入内存中的msgs
                fwg.seek(SeekFrom::Start(self.write_offset)).await?;
                fwg.write_all(&bts).await?;

                fwg.sync_all().await?;
                self.write_offset = self.msg_size;
            }
            None => return Err(anyhow!("not found file handler")),
        };
        msgs_wg.clear();

        Ok(())
    }
}

trait MetaManager: Sync + Send + std::fmt::Debug {
    fn new(dir: &str) -> Self;
    async fn consume(&mut self) -> Result<()>;
    fn load(&mut self) -> Result<()>;
    fn update(&mut self, args: (u64, u64, u64, &str, u64));
    fn persist(&self) -> Result<()>;
    fn meta_filename(&self) -> String;
}

pub fn gen_filename(factor: u64) -> String {
    format!("{:0>15}", factor)
}

#[derive(Debug)]
struct FileHandler {
    fd: File,

    /// 最后使用的时间戳
    last: u64,
}

impl FileHandler {
    pub fn new(fd: File) -> Self {
        FileHandler {
            fd,
            last: Local::now().timestamp() as u64,
        }
    }

    pub async fn parse_message(&mut self) -> Result<(Option<Message>, bool)> {
        let head: ProtocolHead;
        let mut rolling = false;
        let mut fdp = Pin::new(&mut self.fd);
        match ProtocolHead::parse_from(&mut fdp).await {
            Ok(h) => head = h,
            Err(e) => {
                if e.to_string().contains("eof") {
                    rolling = true;
                    return Ok((None, rolling));
                } else {
                    return Err(anyhow!(e));
                }
            }
        }
        match ProtocolBody::parse_from(&mut fdp).await {
            Ok(body) => return Ok((Some(Message::with_one(head, body)), rolling)),
            Err(e) => {
                if e.to_string().contains("eof") {
                    rolling = true;
                } else {
                    return Err(anyhow!(e));
                }
            }
        }
        return Ok((None, rolling));
    }
}

#[cfg(test)]
pub mod tests {
    use super::StorageLocal;
    use crate::{
        message::Message,
        protocol::{ProtocolBody, ProtocolHead},
        storage::storage::StorageOperation as _,
    };
    use bytes::Bytes;
    use common::util::{check_and_create_filename, random_str};
    use rand::Rng;
    use std::path::Path;
    use tokio::{
        fs::OpenOptions,
        io::{AsyncSeekExt, AsyncWriteExt},
    };

    #[test]
    fn test_factor_file() {
        let filename = format!("{:0>15}", 111);
        assert_eq!(filename, "000000000000111");
    }

    fn init_log() {
        let _ = tracing_subscriber::fmt::try_init();
    }

    async fn init_mm() -> StorageLocal {
        let p: &Path = Path::new("../target");
        let mut local = StorageLocal::new(p.join("message"), 10, 299);
        match local.init().await {
            Ok(_) => local,
            Err(e) => panic!("{e:?}"),
        }
    }

    #[tokio::test]
    async fn test_message_manager_load() {
        let local = init_mm().await;
        // println!("msg_size = {}", local.instant.writer.msg_size);
        // println!("msg_num = {}", local.instant.writer.msg_num);
    }

    // #[tokio::test]
    // async fn test_message_manager_defer_push_and_flush() {
    //     let mut local = init_mm().await;
    //     println!("load success");
    //     let mut head = ProtocolHead::new();
    //     assert_eq!(head.set_version(1).is_ok(), true);

    //     for i in 0..20 {
    //         let mut body = ProtocolBody::new();
    //         // 设置id
    //         assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
    //         body.with_ack(true)
    //             .with_defer_time(100 + i)
    //             .with_not_ready(false)
    //             .with_persist(true);

    //         let mut rng = rand::thread_rng();
    //         let length = rng.gen_range(5..50);
    //         let body_str = random_str(length as _);
    //         assert_eq!(
    //             body.with_body(Bytes::copy_from_slice(body_str.as_bytes()))
    //                 .is_ok(),
    //             true
    //         );
    //         assert_eq!(
    //             local
    //                 .push(Message::with_one(head.clone(), body))
    //                 .await
    //                 .is_ok(),
    //             true
    //         );
    //         if i / 3 == 0 {
    //             assert_eq!(local.flush().await.is_ok(), true);
    //         }
    //     }

    //     assert_eq!(local.flush().await.is_ok(), true);
    // }

    // #[tokio::test]
    // async fn test_message_manager_defer_next() {
    //     init_log();
    //     let local = init_mm().await;
    //     while let Ok(msg) = local.next_defer().await {
    //         if let Some(m) = msg {
    //             println!("msg = {m:?}");
    //         } else {
    //             break;
    //         }
    //     }
    // }

    // #[tokio::test]
    // async fn test_message_manager_instant_push_and_flush() {
    //     let mut local = init_mm().await;
    //     println!("load success");
    //     let mut head = ProtocolHead::new();
    //     assert_eq!(head.set_topic("default").is_ok(), true);
    //     assert_eq!(head.set_channel("channel-name").is_ok(), true);
    //     assert_eq!(head.set_version(1).is_ok(), true);

    //     for i in 0..20 {
    //         let mut body = ProtocolBody::new();
    //         // 设置id
    //         assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
    //         body.with_ack(true).with_not_ready(false).with_persist(true);

    //         let mut rng = rand::thread_rng();
    //         let length = rng.gen_range(5..50);
    //         let body_str = random_str(length as _);
    //         assert_eq!(
    //             body.with_body(Bytes::copy_from_slice(body_str.as_bytes()))
    //                 .is_ok(),
    //             true
    //         );
    //         assert_eq!(
    //             local
    //                 .push(Message::with_one(head.clone(), body))
    //                 .await
    //                 .is_ok(),
    //             true
    //         );
    //         if i / 3 == 0 {
    //             assert_eq!(local.flush().await.is_ok(), true);
    //         }
    //     }

    //     assert_eq!(local.flush().await.is_ok(), true);
    // }

    // #[tokio::test]
    // async fn test_message_manager_instant_next() {
    //     init_log();
    //     let local = init_mm().await;
    //     while let Ok(msg) = local.next_instant().await {
    //         if let Some(m) = msg {
    //             println!("msg = {m:?}");
    //         } else {
    //             break;
    //         }
    //     }
    // }

    // #[tokio::test]
    // async fn test_message_manager_instant_consume() {
    //     init_log();
    //     let mm = init_mm().await;
    //     let pre = mm.instant.meta.get().now.left_num;
    //     assert_eq!(mm.instant.meta.get_mut().consume().await.is_ok(), true);
    //     let post = mm.instant.meta.get().now.left_num;
    //     assert_eq!(post + 1, pre);
    // }

    #[tokio::test]
    async fn test_file_seek() {
        let _ = check_and_create_filename("./test_file_seek");

        let mut fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open("./test_file_seek")
            .await
            .unwrap();
        assert_eq!(
            fd.write("abcdefghijklmnopqrstuvwxyz".as_bytes())
                .await
                .is_ok(),
            true
        );

        assert_eq!(fd.flush().await.is_ok(), true);

        assert_eq!(fd.seek(std::io::SeekFrom::Start(4)).await.is_ok(), true);
        assert_eq!(fd.write("EFGH".as_bytes()).await.is_ok(), true);
        assert_eq!(fd.flush().await.is_ok(), true);

        assert_eq!(fd.seek(std::io::SeekFrom::Start(20)).await.is_ok(), true);
        assert_eq!(fd.write("UVW".as_bytes()).await.is_ok(), true);
        assert_eq!(fd.flush().await.is_ok(), true);

        assert_eq!(fd.rewind().await.is_ok(), true);
        assert_eq!(fd.write("ABC".as_bytes()).await.is_ok(), true);
        assert_eq!(fd.flush().await.is_ok(), true);
    }
}
