use crate::message::Message;
use crate::protocol::{ProtocolBody, ProtocolHead};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use common::util::{check_and_create_filename, check_exist, is_debug};
use parking_lot::RwLock;
use std::fs;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt as _;
use std::pin::Pin;
use std::sync::atomic::Ordering::SeqCst;
use std::{path::PathBuf, sync::atomic::AtomicU64};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _};
use tracing::{debug, warn};

use super::gen_filename;
use super::record::{FdCacheAync, MessageRecord};

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';
const HEAD_SIZE_PER_FILE: u64 = 8;

pub struct MessageManager {
    dir: PathBuf,

    /// 文件rorate因子
    factor: AtomicU64,

    fd_cache: FdCacheAync,

    /// 每个文件最大消息数量
    max_msg_num_per_file: u64,

    /// 每个文件最多存放的大小
    max_size_per_file: u64,

    /// 写入消息
    writer: RwLock<MessageDisk>,
}

impl MessageManager {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        let fd_cache = FdCacheAync::new(10);
        MessageManager {
            dir: dir.clone(),
            factor: AtomicU64::new(0),
            fd_cache: fd_cache.clone(),
            max_msg_num_per_file,
            max_size_per_file,
            writer: RwLock::new(MessageDisk::new(dir.join(gen_filename(0)), fd_cache)),
        }
    }

    pub async fn push(&self, msg: Message) -> Result<MessageRecord> {
        if self.writer.read().msg_num >= self.max_msg_num_per_file
            || self.writer.read().msg_size + msg.calc_len() as u64 > self.max_size_per_file
        {
            self.writer.write().persist().await?;
            self.rorate()?;
        }

        let id = msg.id();
        let record = MessageRecord {
            factor: self.factor.load(SeqCst),
            offset: self.writer.read().msg_size,
            length: msg.calc_len() as _,
            id: id.to_string(),
            defer_time: msg.defer_time(),
            consume_time: 0,
            delete_time: 0,
        };

        self.writer.write().push(msg);

        Ok(record)
    }

    pub fn rorate(&self) -> Result<()> {
        self.factor.fetch_add(1, SeqCst);
        let next_filename = self.dir.join(gen_filename(self.factor.load(SeqCst)));
        let mut wg = self.writer.write();
        wg.reset_with_filename(next_filename);
        check_and_create_filename(&wg.filename)?;
        Ok(())
    }

    pub async fn load(&self) -> Result<()> {
        if !check_exist(&self.dir) {
            return Ok(());
        }

        let rd = fs::read_dir(self.dir.to_str().unwrap())?;
        // u64::MAX: 922_3372_0368_5477_5807
        let mut max_factor = 0_u64;
        let file_iter = rd.into_iter();
        let mut cant_parse = vec![];
        for entry in file_iter {
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
        if !cant_parse.is_empty() {
            warn!("files{cant_parse:?} not standard message file, ignore them.")
        }

        // 初始化Manager.factor和Manager.writer，为下次写入消息时发生文件滚动做准备
        self.factor.store(max_factor, SeqCst);

        let mut wg = self.writer.write();
        wg.filename = self.dir.join(gen_filename(self.factor.load(SeqCst)));

        if check_exist(&wg.filename) {
            wg.load().await?;
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        if self.writer.read().msgs.read().is_empty() {
            return Ok(());
        }
        self.writer.write().persist().await?;
        Ok(())
    }

    pub async fn find_by(&self, record: MessageRecord) -> Result<Option<Message>> {
        let filename = self.dir.join(gen_filename(record.factor));
        let fd = self.fd_cache.get_or_create(&filename)?;

        let mut fwd = fd.write();

        let mut buf = BytesMut::new();
        buf.resize(record.length as _, 0);
        fwd.seek(SeekFrom::Start(record.offset)).await?;
        fwd.read_exact(&mut buf).await?;

        let msg = Message::parse_from(&buf).await?;
        Ok(Some(msg))
    }
}

/// 普通消息，一个消息对应一个文件
///
/// 格式为<8-bytes msg-size><8-bytes msg-num><msg1><msg2>...<msgn>
pub struct MessageDisk {
    /// 写入的文件
    filename: PathBuf,

    /// 校验文件中的消息是否合法
    validate: bool,

    /// 持久化文件的句柄
    fd_cache: FdCacheAync,

    /// push来的消存放在内存中
    msgs: RwLock<Vec<Message>>,

    /// 当前文件的消息大小
    msg_size: u64,

    /// 当前文件消息数量（8byte）
    msg_num: u64,

    /// 写入位置的offset
    write_offset: u64,
}

impl MessageDisk {
    pub fn new(filename: PathBuf, fd_cache: FdCacheAync) -> Self {
        MessageDisk {
            filename,
            msg_size: HEAD_SIZE_PER_FILE,
            msg_num: 0,
            write_offset: HEAD_SIZE_PER_FILE,
            msgs: RwLock::new(vec![]),
            // fd: None,
            fd_cache,
            validate: false,
        }
    }

    pub fn reset_with_filename(&mut self, filename: PathBuf) {
        self.filename = filename;
        // self.fd = None;
        self.msgs = RwLock::new(vec![]);
        self.write_offset = HEAD_SIZE_PER_FILE;
        self.msg_size = HEAD_SIZE_PER_FILE;
        self.msg_num = 0;
    }

    pub fn push(&mut self, msg: Message) {
        self.msg_size += msg.calc_len() as u64;
        self.msg_num += 1;
        let mut rg = self.msgs.write();
        rg.push(msg);
    }

    pub async fn load(&mut self) -> Result<()> {
        debug!("load from filename: {:?}", &self.filename);

        let fd = self.fd_cache.get_or_create(&self.filename)?;

        let mut fwd = fd.write();
        // init msg_size
        let meta = fwd.metadata().await?;
        self.msg_size = meta.size();

        let mut pfd = Pin::new(&mut *fwd);
        // load msg_num
        let mut num_buf = BytesMut::new();
        num_buf.resize(HEAD_SIZE_PER_FILE as _, 0);
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
            let head = ProtocolHead::parse_from(&mut pfd).await;
            if let Err(e) = head {
                if e.to_string().contains("eof") {
                    break;
                }
                return Err(anyhow!(e));
            }
            let head = head.unwrap();

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

        // 准备待写入的bts
        let mut msgs_wg = self.msgs.write();
        let iter = msgs_wg.iter();
        let mut bts = vec![];
        for mu in iter {
            bts.extend(mu.as_bytes());
        }

        let fd = self.fd_cache.get_or_create(&self.filename)?;
        let mut fwg = fd.write();

        // 写入msg_num
        fwg.seek(SeekFrom::Start(0)).await?;
        fwg.write_all(&self.msg_num.to_be_bytes()).await?;

        // 在offset位置写入内存中的msgs
        fwg.seek(SeekFrom::Start(self.write_offset)).await?;
        fwg.write_all(&bts).await?;

        fwg.sync_all().await?;
        self.write_offset = self.msg_size;

        msgs_wg.clear();

        Ok(())
    }
}
