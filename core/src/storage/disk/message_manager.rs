use super::gen_filename;
use super::record::{FdCache, MessageRecord};
use crate::compress::{CompressWrapper, COMPRESS_TYPE_NONE};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use common::util::{check_exist, is_debug, SwitcherVec};
use parking_lot::RwLock;
use protocol::message::{Message, MessageOperation as _};

use std::fs::{self};
use std::io::{Seek as _, SeekFrom, Write};
use std::os::unix::fs::MetadataExt as _;
use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::{path::PathBuf, sync::atomic::AtomicU64};
use tokio::fs::File as AsyncFile;
use tokio::io::AsyncReadExt;
use tracing::{debug, warn};

const SPLIT_UNIT: char = '\n';
const SPLIT_CELL: char = ',';
const HEAD_SIZE_PER_FILE: u64 = 8;

pub struct MessageManager {
    dir: PathBuf,

    /// 文件rorate因子
    factor: AtomicU64,

    fd_cache: FdCache,

    /// 每个文件最大消息数量
    max_msg_num_per_file: u64,

    /// 每个文件最多存放的大小
    max_size_per_file: u64,

    compress: CompressWrapper,
    /// 写入消息
    writer: MessageDisk,
}

impl MessageManager {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        let fd_cache = FdCache::new(10);
        MessageManager {
            dir: dir.clone(),
            factor: AtomicU64::new(0),
            fd_cache: fd_cache.clone(),
            max_msg_num_per_file,
            max_size_per_file,
            compress: CompressWrapper::with_type(COMPRESS_TYPE_NONE),
            writer: MessageDisk::new(dir.join(gen_filename(0)), fd_cache),
        }
    }

    pub async fn push(&self, msg: Message) -> Result<MessageRecord> {
        let defer_time = msg.defer_time();
        let id = msg.get_id().to_string();
        let compressed_msg = self.compress.compress(msg).await?;

        if self.writer.msg_num.load(Relaxed) >= self.max_msg_num_per_file
            || self.writer.msg_size.load(Relaxed) + compressed_msg.len() as u64
                > self.max_size_per_file
        {
            self.writer.persist().await?;
            self.rorate()?;
        }

        let record = MessageRecord {
            factor: self.factor.load(Relaxed),
            offset: self.writer.msg_size.load(Relaxed),
            length: compressed_msg.len() as _,
            id,
            defer_time,
            consume_time: 0,
            delete_time: 0,
        };

        self.writer.push(compressed_msg);

        Ok(record)
    }

    pub fn rorate(&self) -> Result<()> {
        self.factor.fetch_add(1, Relaxed);
        let next_filename = self.dir.join(gen_filename(self.factor.load(Relaxed)));
        self.writer.reset_with_filename(next_filename);
        // check_and_create_filename(&self.writer.filename)?;
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
        self.factor.store(max_factor, Relaxed);

        self.writer
            .reset_with_filename(self.dir.join(gen_filename(self.factor.load(Relaxed))));

        if check_exist(&*self.writer.filename.read()) {
            self.writer.load().await?;
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        self.writer.persist().await?;
        Ok(())
    }

    pub async fn find_by(&self, record: MessageRecord) -> Result<Option<Message>> {
        let filename = self.dir.join(gen_filename(record.factor));
        let buf = self.fd_cache.read_by_record(&filename, record)?;
        let msg = self.compress.decompress(&buf).await?;
        Ok(Some(msg))
    }
}

/// 普通消息，一个消息对应一个文件
///
/// 格式为<8-bytes msg-size><8-bytes msg-num><msg1><msg2>...<msgn>
pub struct MessageDisk {
    /// 写入的文件
    filename: RwLock<PathBuf>,

    /// 校验文件中的消息是否合法
    validate: bool,

    /// 持久化文件的句柄
    fd_cache: FdCache,

    /// push来的消存放在内存中
    // msgs: RwLock<Vec<Message>>,
    msgs: SwitcherVec<Vec<u8>>,

    /// 当前文件的消息大小
    msg_size: AtomicU64,

    /// 当前文件消息数量（8byte）
    msg_num: AtomicU64,

    /// 写入位置的offset
    write_offset: AtomicU64,
}

impl MessageDisk {
    pub fn new(filename: PathBuf, fd_cache: FdCache) -> Self {
        MessageDisk {
            filename: RwLock::new(filename),
            msg_size: AtomicU64::new(HEAD_SIZE_PER_FILE),
            msg_num: AtomicU64::new(0),
            write_offset: AtomicU64::new(HEAD_SIZE_PER_FILE),
            msgs: SwitcherVec::default(),
            // fd: None,
            fd_cache,
            validate: false,
        }
    }

    pub fn reset_with_filename(&self, filename: PathBuf) {
        let mut wg = self.filename.write();
        *wg = filename;
        // self.fd = None;
        // self.msgs.write().clear();
        self.write_offset.store(HEAD_SIZE_PER_FILE, Relaxed);
        self.msg_size.store(HEAD_SIZE_PER_FILE, Relaxed);
        self.msg_num.store(0, Relaxed);
    }

    pub fn push(&self, msg: Vec<u8>) {
        self.msg_size.fetch_add(msg.len() as u64, Relaxed);
        self.msg_num.fetch_add(1, Relaxed);
        self.msgs.push(msg);
    }

    pub async fn load(&self) -> Result<()> {
        debug!("load from filename: {:?}", &self.filename);

        let filename_rd = self.filename.read().clone();
        let filename = filename_rd.as_path();
        let mut fd = AsyncFile::open(filename).await?;
        drop(filename_rd);

        // init msg_size
        let meta = fd.metadata().await?;
        self.msg_size.store(meta.size(), Relaxed);

        let mut pfd = Pin::new(&mut fd);

        // load msg_num
        let mut num_buf = BytesMut::new();
        num_buf.resize(HEAD_SIZE_PER_FILE as _, 0);
        pfd.read_exact(&mut num_buf).await?;
        self.msg_num.store(
            u64::from_be_bytes(
                num_buf
                    .to_vec()
                    .try_into()
                    .expect("convert to u64 array failed"),
            ),
            Relaxed,
        );

        self.write_offset
            .store(self.msg_size.load(Relaxed), Relaxed);

        if !self.validate {
            return Ok(());
        }

        loop {
            let msg = Message::parse_from_reader(&mut pfd).await;
            if let Err(e) = msg {
                if e.to_string().contains("eof") {
                    break;
                }
                return Err(anyhow!(e));
            }
            if is_debug() {
                debug!("msg = {msg:?}");
            }
            // 不push到msgs中
        }

        Ok(())
    }

    pub async fn persist(&self) -> Result<()> {
        let msgs = self.msgs.pop();
        if msgs.is_empty() {
            return Ok(());
        }

        // 准备待写入的bts
        let iter = msgs.iter();
        let mut bts = vec![];
        for mu in iter {
            bts.extend(mu);
        }

        let fd = self.fd_cache.get_or_create(&self.filename.read())?;
        let mut fwg = fd.write();

        // 写入msg_num
        fwg.seek(SeekFrom::Start(0))?;
        fwg.write_all(&self.msg_num.load(Relaxed).to_be_bytes())?;

        // 在offset位置写入内存中的msgs
        fwg.seek(SeekFrom::Start(self.write_offset.load(Relaxed)))?;
        fwg.write_all(&bts)?;
        fwg.sync_all()?;
        self.write_offset
            .store(self.msg_size.load(Relaxed), Relaxed);

        Ok(())
    }
}
