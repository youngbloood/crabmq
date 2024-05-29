mod defer;
mod instant;
mod record;

use self::{defer::DeferMessageMeta, instant::InstantMessageMeta};
use super::StorageOperation;
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use chrono::Local;
use common::{
    global::Guard,
    util::{check_and_create_dir, check_and_create_filename, check_exist, is_debug},
};
use parking_lot::RwLock;
use std::{fs, io::SeekFrom, path::PathBuf, pin::Pin, vec};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt as _},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

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
    instant: Manager<InstantMessageMeta>,
    defer: Manager<DeferMessageMeta>,

    /// 取消信号
    cancel: CancellationToken,
}

unsafe impl Send for StorageLocal {}
unsafe impl Sync for StorageLocal {}

impl StorageOperation for StorageLocal {
    async fn init(&mut self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        self.instant.load().await?;
        self.defer.load().await?;
        Ok(())
    }

    async fn next(&mut self) -> Option<Message> {
        None
    }

    async fn push(&mut self, msg: Message) -> Result<()> {
        if msg.is_defer() {
            self.defer.push(msg).await?;
            return Ok(());
        }
        self.instant.push(msg).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.instant.persist().await?;
        self.defer.persist().await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}

impl StorageLocal {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        let parent = dir.clone();
        // let (singal_tx, singal_rx) = mpsc::channel(1);
        StorageLocal {
            dir: dir,
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
}

pub struct Manager<T: MetaManager> {
    /// 消息存放目录
    dir: PathBuf,

    /// 文件rorate因子
    factor: u64,

    /// meta信息管理
    meta: Guard<T>,

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
            factor: 0,
            meta: Guard::new(t),
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
            self.writer.persist().await?;
            self.rorate()?;
        }

        if msg.is_defer() {
            let id = msg.id();
            self.meta.get_mut().update((
                self.factor,
                self.writer.msg_size,
                msg.calc_len() as _,
                id,
                msg.defer_time(),
            ));
        } else {
            self.meta.get_mut().update((0, 0, 0, "", 1));
        }
        self.writer.push(msg);
        Ok(())
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
        self.meta.get_mut().persist()?;
        self.writer.persist().await?;
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

trait MetaManager: Sync + Send {
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
        cache::CACHE_TYPE_MEM,
        message::Message,
        protocol::{ProtocolBody, ProtocolHead},
        storage::StorageOperation as _,
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
        println!("msg_size = {}", local.instant.writer.msg_size);
        println!("msg_num = {}", local.instant.writer.msg_num);
    }

    #[tokio::test]
    async fn test_message_manager_defer_push_and_flush() {
        let mut local = init_mm().await;
        println!("load success");
        let mut head = ProtocolHead::new();
        assert_eq!(head.set_version(1).is_ok(), true);

        for i in 0..30 {
            let mut body = ProtocolBody::new();
            // 设置id
            assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
            body.with_ack(true)
                .with_defer_time(100 + i)
                .with_not_ready(false)
                .with_persist(true);

            let mut rng = rand::thread_rng();
            let length = rng.gen_range(5..50);
            let body_str = random_str(length as _);
            assert_eq!(
                body.with_body(Bytes::copy_from_slice(body_str.as_bytes()))
                    .is_ok(),
                true
            );
            assert_eq!(
                local
                    .push(Message::with_one(head.clone(), body))
                    .await
                    .is_ok(),
                true
            );
            if i / 3 == 0 {
                assert_eq!(local.flush().await.is_ok(), true);
            }
        }

        assert_eq!(local.flush().await.is_ok(), true);
    }

    // #[tokio::test]
    // async fn test_message_manager_defer_consume() {
    //     init_log();
    //     let local = init_mm().await;
    //     let pre = local.defer.meta.get().list.len();
    //     assert_eq!(local.defer.meta.get_mut().consume().await.is_ok(), true);
    //     let post = local.defer.meta.get().list.len();
    //     assert_eq!(post + 1, pre);
    // }

    #[tokio::test]
    async fn test_message_manager_instant_push_and_flush() {
        let mut local = init_mm().await;
        let mut head = ProtocolHead::new();
        assert_eq!(head.set_channel("default").is_ok(), true);
        assert_eq!(head.set_topic("default-topic").is_ok(), true);
        assert_eq!(head.set_version(1).is_ok(), true);
        for i in 0..1 {
            let mut body = ProtocolBody::new();
            // 设置id
            assert_eq!(body.with_id((i + 1000).to_string().as_str()).is_ok(), true);
            body.with_ack(true).with_not_ready(false).with_persist(true);

            let mut rng = rand::thread_rng();
            let length = rng.gen_range(1..20);
            let body_str = random_str(length as _);
            assert_eq!(
                body.with_body(Bytes::copy_from_slice(body_str.as_bytes()))
                    .is_ok(),
                true
            );

            assert_eq!(
                local
                    .push(Message::with_one(head.clone(), body))
                    .await
                    .is_ok(),
                true
            );
        }

        assert_eq!(local.flush().await.is_ok(), true);
    }

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
