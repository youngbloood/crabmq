use super::{gen_filename, FdCache, MessageRecord, RecordManagerStrategy};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use common::util::check_exist;
use crossbeam::sync::ShardedLock;
use parking_lot::RwLock;
use std::io::{Read as _, Seek as _, Write as _};
use std::process::Command;
use std::sync::atomic::Ordering::SeqCst;
use std::{
    fs::{self},
    io::SeekFrom,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

/// RecordManager普通策略：适用于instant record信息的记录
pub struct RecordManagerStrategyNormal {
    dir: PathBuf,
    factor: AtomicU64,
    validate: bool,
    record_num_per_file: u64,
    record_size_per_file: u64,
    fd_cache: FdCache,
    writer: RwLock<RecordDisk>,
}

unsafe impl Send for RecordManagerStrategyNormal {}
unsafe impl Sync for RecordManagerStrategyNormal {}

impl RecordManagerStrategyNormal {
    pub fn new(
        dir: PathBuf,
        validate: bool,
        record_num_per_file: u64,
        record_size_per_file: u64,
        fd_cache_size: usize,
    ) -> Self {
        let fd_cache = FdCache::new(fd_cache_size);
        let record_disk = RecordDisk::new(dir.join(gen_filename(0)), validate, fd_cache.clone());

        RecordManagerStrategyNormal {
            dir,
            validate,
            record_num_per_file,
            record_size_per_file,
            writer: RwLock::new(record_disk),
            factor: AtomicU64::new(0),
            fd_cache,
        }
    }

    pub fn with_fd_cache(&mut self, fd_cache: FdCache) {
        self.fd_cache = fd_cache;
    }

    fn rorate(&self) {
        self.factor.fetch_add(1, SeqCst);
        let mut wg = self.writer.write();
        wg.reset_with_filename(
            Path::new(&self.dir).join(gen_filename(self.factor.load(SeqCst)).as_str()),
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
        self.factor.store(max_factor, SeqCst);
        self.writer
            .write()
            .reset_with_filename(Path::new(&self.dir).join(gen_filename(self.factor.load(SeqCst))));
        self.writer.write().load(false)?;
        Ok(())
    }

    async fn push(&self, record: MessageRecord) -> Result<(PathBuf, usize)> {
        if self.writer.read().record_size + record.calc_len() as u64 > self.record_size_per_file
            || self.writer.read().record_num + 1 > self.record_num_per_file
        {
            self.persist().await?;
            self.rorate();
        }
        let mut wd = self.writer.write();
        let index = wd.push(record, false);
        Ok((PathBuf::from(&wd.filename), index))
    }

    async fn find(&self, id: &str) -> Result<Option<(PathBuf, MessageRecord)>> {
        let result = Command::new("/bin/bash")
            .arg("-c")
            .arg(format!(
                r#"if [[ -d {:?} ]]; then grep {id} -r {:?} else echo "notexist";fi"#,
                self.dir, self.dir
            ))
            .output()
            .expect("execute cmd error");
        let result = String::from_utf8(result.stdout).expect("convert to string failed");
        if result == "notexist" {
            return Ok(None);
        }
        let cells: Vec<&str> = result.split(':').collect();
        if cells.len() != 2 {
            return Ok(None);
        }
        Ok(Some((
            PathBuf::from(cells.first().unwrap()),
            MessageRecord::parse_from(cells.last().unwrap())?,
        )))
    }

    async fn persist(&self) -> Result<()> {
        self.writer.write().persist()?;
        Ok(())
    }
}

struct RecordDisk {
    filename: PathBuf,
    validate: bool,

    fd_cache: FdCache,

    /// 该文件中records的大小
    record_size: u64,

    /// records数量
    record_num: u64,

    write_offset: u64,

    /// 该文件对应的records
    records: Vec<MessageRecord>,
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
            filename,
            validate,
            fd_cache,
            record_size: 21,
            record_num: 0,
            write_offset: 21,
            records: Vec::new(),
        }
    }

    fn load(&mut self, load: bool) -> Result<()> {
        if !check_exist(&self.filename) {
            return Ok(());
        }

        let fd = self.fd_cache.get_or_create(&self.filename)?;

        let mut wfd = fd.write();
        let meta = wfd.metadata()?;
        self.record_size = meta.size();
        self.write_offset = self.record_size;

        let mut buf = BytesMut::new();
        buf.resize(21, 0);
        wfd.read_exact(&mut buf)?;

        let record_num_str = String::from_utf8(buf.to_vec())?;
        self.record_num = record_num_str
            .trim_end()
            .parse::<u64>()
            .expect("convert to record_num failed");

        if !self.validate {
            return Ok(());
        }

        buf.resize(self.record_size as usize - 21, 0);
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
        if load {
            self.records.sort_by_key(|c| c.defer_time);
        }
        Ok(())
    }

    fn reset_with_filename(&mut self, filename: PathBuf) {
        self.filename = filename;
        self.record_size = 0;
        self.record_num = 0;
        self.write_offset = 21;
        self.records = vec![];
    }

    fn push(&mut self, record: MessageRecord, sort: bool) -> usize {
        self.record_num += 1;
        self.record_size += record.calc_len() as u64;
        self.records.push(record);
        self.record_num as usize
    }

    fn persist(&mut self) -> Result<()> {
        if self.record_num == 0 {
            return Ok(());
        }

        let mut bts = BytesMut::new();

        let fd = self.fd_cache.get_or_create(&self.filename)?;
        let mut wfd = fd.write();
        // write record_num
        wfd.seek(SeekFrom::Start(0))?;
        wfd.write_all(format!("{}\n", gen_filename(self.record_num)).as_bytes())?;

        // write records
        wfd.seek(SeekFrom::Start(self.write_offset))?;
        let iter = self.records.iter();
        for record in iter {
            let record_str = record.format();
            let record_bts = record_str.as_bytes();
            self.write_offset += record_bts.len() as u64;
            bts.extend(record_bts);
        }
        wfd.write_all(&bts)?;
        self.records.clear();
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

    fn read(&self) -> Result<Option<MessageRecord>> {
        let record_rd = self
            .record_filename
            .read()
            .expect("get sharedlock write failed");
        if !check_exist(&*record_rd) {
            return Ok(None);
        }
        let fd = self.fd_cache.get_or_create(&record_rd)?;
        let mut fwd = fd.write();
        let mut content = "".to_string();

        fwd.seek(SeekFrom::Start(0))?;
        fwd.read_to_string(&mut content)?;
        let lines: Vec<&str> = content.split('\n').collect();

        let index = self.index.load(SeqCst);

        if index < lines.len() as u64 {
            self.index.fetch_add(1, SeqCst);
            let line = lines[index as usize];
            if line.is_empty() {
                return Ok(None);
            }
            let mr = MessageRecord::parse_from(line)?;
            return Ok(Some(mr));
        }
        Ok(None)
    }

    pub fn next(&self) -> Result<Option<MessageRecord>> {
        match self.read() {
            Ok(val) => match val {
                Some(msg) => Ok(Some(msg)),
                None => {
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
                        self.index.store(1, SeqCst);
                    }
                    drop(record_wd);
                    self.read()
                }
            },
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
                SeqCst,
            )
        }
        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        println!("index = {}", self.index.load(SeqCst));
        if self.filename.eq(&PathBuf::new()) && self.index.load(SeqCst) == 1 {
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
                self.index.load(SeqCst)
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
        self.index.store(index as _, SeqCst);
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
        );

        assert!(rmsc.load().await.is_ok());
        println!("factor = {}", rmsc.factor.load(SeqCst));
        println!("record_size = {}", rmsc.writer.read().record_size);
        println!("record_num = {}", rmsc.writer.read().record_num);
    }

    #[tokio::test]
    async fn test_record_manager_strategy_normal_push_and_persist() {
        let rmsc: RecordManagerStrategyNormal = RecordManagerStrategyNormal::new(
            PathBuf::new().join("../target/message/default/instant/record"),
            false,
            100000,
            5000000,
            2,
        );
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
        );

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
