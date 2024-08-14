use super::index::{Index, IndexCache};
use super::{gen_filename, FdCache, MessageRecord, RecordManagerStrategy};
use anyhow::Result;
use bytes::BytesMut;
use common::util::{check_exist, SwitcherVec};
use crossbeam::sync::ShardedLock;
use parking_lot::RwLock;
use std::io::{Read as _, Seek as _, Write as _};
use std::process::Command;
use std::sync::atomic::Ordering::Relaxed;
use std::{
    fs::{self},
    io::SeekFrom,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};
use tracing::error;

/// RecordManager普通策略：适用于instant record信息的记录
pub struct RecordManagerStrategyNormal {
    dir: PathBuf,
    factor: AtomicU64,
    validate: bool,
    record_num_per_file: u64,
    record_size_per_file: u64,
    fd_cache: FdCache,

    writer: RecordDisk,
    index: Box<dyn Index>,
}

impl RecordManagerStrategyNormal {
    pub fn new(
        dir: PathBuf,
        validate: bool,
        record_num_per_file: u64,
        record_size_per_file: u64,
        fd_cache_size: usize,
    ) -> Result<Self> {
        let fd_cache = FdCache::new(fd_cache_size);
        let record_disk = RecordDisk::new(dir.join(gen_filename(0)), validate, fd_cache.clone());

        Ok(RecordManagerStrategyNormal {
            dir: dir.clone(),
            validate,
            record_num_per_file,
            record_size_per_file,
            factor: AtomicU64::new(0),
            fd_cache,
            writer: record_disk,
            index: Box::new(IndexCache::new(dir.join("index"), 15000000)?),
        })
    }

    pub fn with_fd_cache(&mut self, fd_cache: FdCache) {
        self.fd_cache = fd_cache;
    }

    fn rorate(&self) {
        self.factor.fetch_add(1, Relaxed);
        self.writer.reset_with_filename(
            Path::new(&self.dir).join(gen_filename(self.factor.load(Relaxed)).as_str()),
        );
    }
}

impl RecordManagerStrategy for RecordManagerStrategyNormal {
    async fn load(&self) -> Result<()> {
        if !check_exist(&self.dir) {
            return Ok(());
        }
        let dir = fs::read_dir(self.dir.as_path()).expect("read dir failed");
        let iter = dir.into_iter();
        let mut max_factor = 0_u64;
        for entry in iter {
            let entry = entry?;
            if let Ok(factor) = entry.file_name().to_str().unwrap().parse::<u64>() {
                if factor > max_factor {
                    max_factor = factor;
                }
            }
        }
        self.factor.store(max_factor, Relaxed);
        self.writer.reset_with_filename(
            Path::new(&self.dir).join(gen_filename(self.factor.load(Relaxed))),
        );
        self.writer.load(false)?;
        Ok(())
    }

    async fn push(&self, record: MessageRecord) -> Result<(PathBuf, usize)> {
        self.index.push(record.clone())?;

        if self.writer.record_size.load(Relaxed) + record.calc_len() as u64
            > self.record_size_per_file
            || self.writer.record_num.load(Relaxed) + 1 > self.record_num_per_file
        {
            self.persist().await?;
            self.rorate();
        }
        let index = self.writer.push(record, false);
        Ok((PathBuf::from(&*self.writer.filename.read()), index))
    }

    async fn find(&self, id: &str) -> Result<Option<(PathBuf, MessageRecord)>> {
        return match self.index.find(id)? {
            Some(record) => Ok(Some((PathBuf::new(), record))),
            None => {
                let dir = self.dir.clone();
                let id = id.to_string();
                tokio::spawn(async move {
                    let result = Command::new("/bin/bash")
                        .arg("-c")
                        .arg(format!(
                            r#"if [[ -d {:?} ]]; then grep {id} -r {:?} else echo "notexist";fi"#,
                            dir, dir
                        ))
                        .output()
                        .expect("execute cmd error");
                    let result =
                        String::from_utf8(result.stdout).expect("convert to string failed");
                    if result != "notexist" {
                        error!("found record[id={id}] in dir[{:?}] success: {result}", dir);
                    }
                });

                Ok(None)
            }
        };
    }

    async fn delete(&self, id: &str) -> Result<()> {
        todo!()
    }

    async fn update_delete_flag(&self, id: &str, delete: bool) -> Result<()> {
        todo!()
    }

    async fn update_notready_flag(&self, id: &str, delete: bool) -> Result<()> {
        todo!()
    }

    async fn update_consume_flag(&self, id: &str, delete: bool) -> Result<()> {
        todo!()
    }

    async fn persist(&self) -> Result<()> {
        self.writer.persist()?;
        Ok(())
    }
}

struct RecordDisk {
    filename: RwLock<PathBuf>,
    validate: bool,

    fd_cache: FdCache,

    /// 该文件中records的大小
    record_size: AtomicU64,

    /// records数量
    record_num: AtomicU64,

    write_offset: AtomicU64,

    /// 该文件对应的records
    records: SwitcherVec<MessageRecord>,
}

// impl Default for RecordDisk {
//     fn default() -> Self {
//         Self {
//             filename: Default::default(),
//             validate: Default::default(),
//             fd: Default::default(),
//             record_size: Default::default(),
//             record_num: Default::default(),
//             write_offset: 21,
//             records: Default::default(),
//         }
//     }
// }

impl RecordDisk {
    fn new(filename: PathBuf, validate: bool, fd_cache: FdCache) -> Self {
        RecordDisk {
            filename: RwLock::new(filename),
            validate,
            fd_cache,
            record_size: AtomicU64::new(21),
            record_num: AtomicU64::new(0),
            write_offset: AtomicU64::new(21),
            records: SwitcherVec::default(),
        }
    }

    fn load(&self, load: bool) -> Result<()> {
        if !check_exist(&*self.filename.read()) {
            return Ok(());
        }

        let fd = self.fd_cache.get_or_create(&self.filename.read())?;

        let mut wfd = fd.write();
        let meta = wfd.metadata()?;
        let file_size = meta.size();
        self.record_size.store(file_size, Relaxed);
        self.write_offset.store(file_size, Relaxed);

        let mut buf = BytesMut::new();
        buf.resize(21, 0);
        wfd.read_exact(&mut buf)?;

        let record_num_str = String::from_utf8(buf.to_vec())?;
        self.record_num.store(
            record_num_str
                .trim_end()
                .parse::<u64>()
                .expect("convert to record_num failed"),
            Relaxed,
        );

        if !self.validate {
            return Ok(());
        }

        buf.resize(file_size as usize - 21, 0);
        wfd.read_exact(&mut buf)?;
        let buf_vec = buf.to_vec();
        let cells = buf_vec.split(|n| *n == b'\n');
        for cs in cells {
            let line = String::from_utf8(cs.to_vec())?;
            let record = MessageRecord::parse_from(&line)?;
            if load {
                self.records.push(record);
            }
        }
        // if load {
        //     self.records.write().sort_by_key(|c| c.defer_time);
        // }
        Ok(())
    }

    fn reset_with_filename(&self, filename: PathBuf) {
        let mut fn_wd = self.filename.write();
        *fn_wd = filename;
        self.record_size.store(0, Relaxed);
        self.record_num.store(0, Relaxed);
        self.write_offset.store(21, Relaxed);
        // self.records.write().clear();
    }

    fn push(&self, record: MessageRecord, _sort: bool) -> usize {
        self.record_num.fetch_add(1, Relaxed);
        self.record_size
            .fetch_add(record.calc_len() as u64, Relaxed);
        self.records.push(record);
        self.record_num.load(Relaxed) as usize
    }

    fn persist(&self) -> Result<()> {
        if self.record_num.load(Relaxed) == 0 {
            return Ok(());
        }

        let mut bts = BytesMut::new();

        let fd = self.fd_cache.get_or_create(&self.filename.read())?;
        let mut wfd = fd.write();
        // write record_num
        wfd.seek(SeekFrom::Start(0))?;
        wfd.write_all(format!("{}\n", gen_filename(self.record_num.load(Relaxed))).as_bytes())?;

        // write records
        wfd.seek(SeekFrom::Start(self.write_offset.load(Relaxed)))?;

        let records = self.records.pop();
        if records.is_empty() {
            return Ok(());
        }
        let iter = records.iter();
        for record in iter {
            let record_str = record.format();
            let record_bts = record_str.as_bytes();
            self.write_offset
                .fetch_add(record_bts.len() as u64, Relaxed);
            bts.extend(record_bts);
        }
        wfd.write_all(&bts)?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct NormalPtr {
    fd_cache: FdCache,

    /// 存放下列2行信息的文件名称（meta）
    filename: PathBuf,

    // 当前meta文件记录的filename
    record_filename: ShardedLock<PathBuf>,
    index: AtomicU64,
}

impl NormalPtr {
    pub fn new(filename: PathBuf, fd_cache: FdCache) -> Self {
        NormalPtr {
            fd_cache,
            filename,

            record_filename: ShardedLock::default(),
            index: AtomicU64::new(1),
        }
    }

    fn read(&self) -> Result<(bool, Option<MessageRecord>)> {
        let record_rd = self
            .record_filename
            .read()
            .expect("get sharedlock write failed");
        if !check_exist(&*record_rd) {
            return Ok((false, None));
        }
        let fd = self.fd_cache.get_or_create(&record_rd)?;
        let mut fwd = fd.write();
        let mut content = "".to_string();

        fwd.seek(SeekFrom::Start(0))?;
        fwd.read_to_string(&mut content)?;
        let lines: Vec<&str> = content.split('\n').collect();

        let index = self.index.load(Relaxed);
        if index < lines.len() as u64 {
            let line = lines[index as usize];
            if line.is_empty() {
                return Ok((true, None));
            }
            let mr = MessageRecord::parse_from(line)?;
            return Ok((false, Some(mr)));
        }
        Ok((true, None))
    }

    pub fn seek(&self) -> Result<Option<MessageRecord>> {
        match self.read() {
            Ok((_, msg)) => Ok(msg),
            Err(e) => Err(e),
        }
    }

    pub fn next(&self) -> Result<Option<MessageRecord>> {
        match self.read() {
            Ok((over_line, val)) => {
                if over_line {
                    let mut record_wd = self
                        .record_filename
                        .write()
                        .expect("get sharedlock write failed");

                    // 滚动至下一个record文件
                    let mut factor = record_wd
                        .file_name()
                        .as_ref()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<u64>()?;
                    factor += 1;
                    if let Some(parent) = record_wd.parent() {
                        *record_wd = parent.join(gen_filename(factor));
                    }
                    self.index.store(1, Relaxed);
                    drop(record_wd);
                } else {
                    self.index.fetch_add(1, Relaxed);
                }

                match val {
                    Some(msg) => Ok(Some(msg)),
                    None => Ok(None),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn load(&self) -> Result<()> {
        if !check_exist(&self.filename) {
            return Ok(());
        }
        let content = fs::read_to_string(&self.filename)?;
        let lines: Vec<&str> = content.split('\n').collect();
        if !lines.is_empty() {
            let record_filename = Path::new(lines.first().unwrap()).to_path_buf();
            let mut wd = self
                .record_filename
                .write()
                .expect("get sharedlock write failed");
            wd.clone_from(&record_filename);
        }
        if lines.len() >= 2 {
            self.index.store(
                lines
                    .get(1)
                    .unwrap()
                    .parse::<u64>()
                    .expect("convert to index failed"),
                Relaxed,
            )
        }
        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        if self.filename.eq(&PathBuf::new()) && self.index.load(Relaxed) == 1 {
            return Ok(());
        }
        fs::write(
            &self.filename,
            format!(
                "{}\n{}",
                &self
                    .record_filename
                    .read()
                    .expect("get sharedlock read failed")
                    .to_str()
                    .unwrap(),
                self.index.load(Relaxed)
            ),
        )?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.record_filename
            .read()
            .expect("get sharedlock read failed")
            .eq(&PathBuf::new())
    }

    pub fn set(&self, record_filename: PathBuf, index: usize) -> Result<()> {
        let mut wd = self
            .record_filename
            .write()
            .expect("get sharedlock write failed");
        wd.set_file_name(record_filename);
        self.index.store(index as _, Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::util::random_str;
    use rand::Rng as _;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_record_manager_strategy_normal_load() {
        let rmsc = RecordManagerStrategyNormal::new(
            PathBuf::new().join("../target/message/default/instant/record"),
            false,
            10,
            50000,
            2,
        )
        .unwrap();

        assert!(rmsc.load().await.is_ok());
        println!("factor = {}", rmsc.factor.load(Relaxed));
        println!("record_size = {}", rmsc.writer.record_size.load(Relaxed));
        println!("record_num = {}", rmsc.writer.record_num.load(Relaxed));
    }

    #[tokio::test]
    async fn test_record_manager_strategy_normal_push_and_persist() {
        let rmsc: RecordManagerStrategyNormal = RecordManagerStrategyNormal::new(
            PathBuf::new().join("../target/message/default/instant/record"),
            false,
            100000,
            5000000,
            2,
        )
        .unwrap();
        assert!(rmsc.load().await.is_ok());

        let mut rng = rand::thread_rng();
        let length = rng.gen_range(5..50);
        for _ in 0..100000 {
            let id_str = random_str(length as _);
            assert!(rmsc
                .push(MessageRecord {
                    factor: 0,
                    offset: 0,
                    length: 50,
                    id: id_str,
                    defer_time: 0,
                    consume_time: 0,
                    delete_time: 0,
                })
                .await
                .is_ok());
        }

        assert!(rmsc.persist().await.is_ok());
    }

    #[tokio::test]
    async fn test_record_manager_strategy_normal_find() {
        let rmsc = RecordManagerStrategyNormal::new(
            PathBuf::new().join("../target/message/default/instant/record"),
            false,
            10,
            50000,
            2,
        )
        .unwrap();

        let out = rmsc.find("0dqlRXBTJyTQA3BTlPq0QNclO3Vh0dEYuShhlG").await;
        assert!(out.is_ok());
        assert!(out.as_ref().unwrap().is_some());
        let value = out.as_ref().unwrap();
        println!("file={:?}", value.as_ref().unwrap().0);
        println!("record={:?}", value.as_ref().unwrap().1);
        let out = rmsc.find("0dqlRXBTJyTQA3BTlPq0QNclO3Vh0dEYuShhlG11").await;
        assert!(out.is_ok());
        assert!(out.unwrap().is_none());
    }
}
