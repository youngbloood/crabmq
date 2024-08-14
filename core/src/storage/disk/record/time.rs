use super::index::{Index, IndexCache};
use super::{FdCache, MessageRecord, RecordManagerStrategy};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use chrono::{Datelike, FixedOffset, Timelike};
use common::util::{check_exist, dir_recursive};
use crossbeam::sync::ShardedLock;
use lru::LruCache;
use regex::Regex;
use std::cmp::Ordering;
use std::fs::{self, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::{
    num::NonZeroUsize,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::AtomicBool,
};
use tracing::error;

/// RecordManager时间片策略：适用于defer record信息的记录
pub struct RecordManagerStrategyTime {
    dir: PathBuf,

    validate: bool,

    /// 子目录的模版，如：xxx/{daily:1}/{hourly:2}/{minutely:5}，表示每天一个文件夹，每2h一个文件夹，每5min一个文件
    template: String,

    fd_cache: FdCache,

    writers: ShardedLock<LruCache<PathBuf, RecordDisk>>,
    index: Box<dyn Index>,
}

impl RecordManagerStrategyTime {
    pub fn new(dir: PathBuf, validate: bool, template: &str, fd_cache_size: usize) -> Result<Self> {
        let regs = [r#"\{daily.*?\}"#, r#"\{hourly.*?\}"#, r#"\{minutely.*?\}"#];
        let mut daily_pos = 0_usize;
        let mut hourly_pos = 0_usize;
        let mut minutely_pos = 0_usize;

        for (i, reg) in regs.iter().enumerate() {
            if let Some(pos) = Regex::new(reg).unwrap().shortest_match(template) {
                if pos == 0 {
                    continue;
                }
                if i == 0 {
                    daily_pos = pos;
                } else if i == 1 {
                    hourly_pos = pos;
                } else {
                    minutely_pos = pos;
                }
            }
        }
        // println!(
        //     "template={}, daily_pos={}, hourly_pos={}, minutely_pos={}",
        //     template, daily_pos, hourly_pos, minutely_pos
        // );

        if (daily_pos != 0 && hourly_pos != 0 && daily_pos > hourly_pos)
            || (daily_pos != 0 && minutely_pos != 0 && daily_pos > minutely_pos)
            || (hourly_pos != 0 && minutely_pos != 0 && hourly_pos > minutely_pos)
        {
            // corrent should like: xxx/{daily:1}/{hourly:2}/{minutely:5}
            return Err(anyhow!("error sequence of the template"));
        }

        let rmst = RecordManagerStrategyTime {
            dir: dir.clone(),
            validate,
            fd_cache: FdCache::new(10),
            template: template.to_string(),
            writers: ShardedLock::new(LruCache::new(NonZeroUsize::new(fd_cache_size).unwrap())),
            index: Box::new(IndexCache::new(dir.join("index"), 15000000)?),
        };
        let (daily, hourly, minutely) = rmst.parse_template();
        // if daily.1 != 0 && daily.1  != 1 {
        //     return Err(anyhow!("daily only support 1"));
        // }
        if hourly.1 != 0 && 24 % hourly.1 != 0 {
            return Err(anyhow!("hourly must be divisible by 24"));
        }
        if minutely.1 != 0 && 60 % minutely.1 != 0 {
            return Err(anyhow!("minutely must be divisible by 60"));
        }

        Ok(rmst)
    }

    fn parse_template(&self) -> ((String, u64), (String, u64), (String, u64)) {
        let mut daily = 0_u64;
        let mut daily_conf = "";
        let mut hourly = 0_u64;
        let mut hourly_conf = "";
        let mut minutely = 0_u64;
        let mut minutely_conf = "";

        let regs = [r#"\{daily.*?\}"#, r#"\{hourly.*?\}"#, r#"\{minutely.*?\}"#];
        for (i, reg) in regs.iter().enumerate() {
            let captures = Regex::new(reg).unwrap().captures(&self.template);
            if captures.is_none() {
                continue;
            }
            let c = captures.unwrap();

            if i == 0 {
                daily = 1;
                daily_conf = c.get(0).unwrap().as_str();
            } else if i == 1 {
                hourly = 1;
                hourly_conf = c.get(0).unwrap().as_str();
            } else {
                minutely = 1;
                minutely_conf = c.get(0).unwrap().as_str();
            }

            let str = c
                .get(0)
                .unwrap()
                .as_str()
                .trim_matches('{')
                .trim_matches('}');
            if !str.contains(':') {
                continue;
            }
            let cells: Vec<&str> = str.split(':').collect();
            if cells.len() != 2 {
                continue;
            }

            if i == 0 {
                daily = cells
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .expect("convert to u64 failed");
            } else if i == 1 {
                hourly = cells
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .expect("convert to u64 failed");
            } else {
                minutely = cells
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .expect("convert to u64 failed");
            }
        }
        (
            (daily_conf.to_string(), daily),
            (hourly_conf.to_string(), hourly),
            (minutely_conf.to_string(), minutely),
        )
    }

    fn deduce_filename(&self, defer_time: u64) -> String {
        let mut temp = self.template.as_str().to_string();
        let (daily, hourly, minutely) = self.parse_template();

        let ts = chrono::DateTime::from_timestamp(defer_time as _, 0)
            .unwrap()
            .with_timezone(&FixedOffset::east_opt(8 * 3600).unwrap());

        // println!("now = {}", ts.format("%d/%m/%Y %H:%M"));
        // 处理daily
        let mut daily_shared = "".to_string();
        if daily.1 != 0 {
            if daily.1 == 1 {
                daily_shared = ts.format("%Y_%m_%d").to_string();
            } else {
                let upper = (ts.ordinal0() as u64 / daily.1 + 1) * daily.1 - 1;
                let lower = (ts.ordinal0() as u64 / daily.1) * daily.1;
                let upper_ts = ts.clone().with_ordinal0(upper as _).unwrap();
                let lower_ts = ts.clone().with_ordinal0(lower as _).unwrap();
                daily_shared = format!(
                    "{}-{}",
                    lower_ts.format("%Y_%m_%d"),
                    upper_ts.format("%Y_%m_%d")
                );
            }
        }
        if !daily_shared.is_empty() {
            temp = temp.replace(&daily.0, &daily_shared);
        }

        // 处理hourly
        let mut hourly_shared = "".to_string();
        if hourly.1 != 0 {
            if hourly.1 == 1 {
                if daily_shared.is_empty() {
                    hourly_shared = ts.format("%Y_%m_%d_%H").to_string();
                } else {
                    hourly_shared = ts.hour().to_string();
                }
            } else {
                let upper = (ts.hour() as u64 / hourly.1 + 1) * hourly.1 - 1;
                let lower = (ts.hour() as u64 / hourly.1) * hourly.1;
                // println!("hour={}, upper={}, lower={}", ts.hour(), upper, lower);
                let upper_ts = ts.clone().with_hour((upper) as _).unwrap();
                let lower_ts = ts.clone().with_hour((lower) as _).unwrap();
                if daily_shared.is_empty() {
                    hourly_shared = format!(
                        "{}-{}",
                        lower_ts.format("%Y_%m_%d_%H"),
                        upper_ts.format("%Y_%m_%d_%H")
                    );
                } else {
                    hourly_shared = format!("{}-{}", lower_ts.hour(), upper_ts.hour());
                }
            }
        }
        if !hourly_shared.is_empty() {
            temp = temp.replace(&hourly.0, &hourly_shared);
        }

        // 处理minutely
        let mut minutely_shared = "".to_string();
        if minutely.1 != 0 {
            if minutely.1 == 1 {
                if daily_shared.is_empty() && hourly_shared.is_empty() {
                    minutely_shared = ts.format("%Y_%m_%d_%H_%M").to_string();
                } else if !daily_shared.is_empty() && hourly_shared.is_empty() {
                    minutely_shared = ts.format("%H_%M").to_string();
                } else {
                    minutely_shared = ts.minute().to_string();
                }
            } else {
                let upper = (ts.minute() as u64 / minutely.1 + 1) * minutely.1 - 1;
                let lower = (ts.minute() as u64 / minutely.1) * minutely.1;
                // println!("lower = {lower}, upper = {upper}");
                let upper_ts = ts.clone().with_minute(upper as _).unwrap();
                let lower_ts = ts.clone().with_minute(lower as _).unwrap();

                if daily_shared.is_empty() && hourly_shared.is_empty() {
                    minutely_shared = format!(
                        "{}-{}",
                        lower_ts.format("%Y_%m_%d_%H_%M"),
                        upper_ts.format("%Y_%m_%d_%H_%M")
                    );
                } else if !daily_shared.is_empty() && hourly_shared.is_empty() {
                    minutely_shared =
                        format!("{}-{}", lower_ts.format("%H_%M"), upper_ts.format("%H_%M"));
                } else {
                    minutely_shared = format!("{}-{}", lower_ts.minute(), upper_ts.minute());
                }
            }
        }
        if !minutely_shared.is_empty() {
            temp = temp.replace(&minutely.0, &minutely_shared);
        }

        temp
    }
}

impl RecordManagerStrategy for RecordManagerStrategyTime {
    async fn load(&self) -> Result<()> {
        Ok(())
    }

    async fn push(&self, record: MessageRecord) -> Result<(PathBuf, usize)> {
        self.index.push(record.clone())?;

        let filename = self.dir.join(self.deduce_filename(record.defer_time));
        let mut wd = self.writers.write().expect("get sharedlock write failed");
        if let Some(record_disk) = wd.get(&filename) {
            record_disk.load()?;
            let index = record_disk.push(record, true);
            return Ok((filename, index));
        }

        let record_disk: RecordDisk =
            RecordDisk::new(filename.clone(), self.validate, self.fd_cache.clone());
        let record_disk = wd.get_or_insert_mut(filename.clone(), || record_disk);
        record_disk.load()?;
        let index = record_disk.push(record, true);
        Ok((filename, index))
    }

    async fn find(&self, id: &str) -> Result<Option<(PathBuf, MessageRecord)>> {
        match self.index.find(id)? {
            Some(record) => Ok(Some((PathBuf::new(), record))),
            None => {
                let dir: PathBuf = self.dir.clone();
                let id = id.to_string();
                tokio::spawn(async move {
                    let result = Command::new("/bin/bash")
                        .arg("-c")
                        .arg(format!(
                            r#"if [[ -d {:?} ]]; then grep {id} -r {:?} else echo "notexist";fi"#,
                            dir, dir
                        ))
                        .output()
                        .expect("execute grep failed");
                    let result = String::from_utf8(result.stdout).expect("convert to string");
                    if result != "notexist" {
                        error!("found record[id={id}] in dir[{:?}] success: {result}", dir);
                    }
                });

                Ok(None)
            }
        }
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
        let rd = self.writers.read().expect("get sharedlock read failed");
        for (_, record_disk) in rd.iter() {
            record_disk.persist()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct RecordDisk {
    filename: PathBuf,
    validate: bool,
    fd_cache: FdCache,
    has_change: AtomicBool,

    loaded: AtomicBool,

    last_defer_time: AtomicU64,
    /// 该文件对应的records
    records: ShardedLock<Vec<MessageRecord>>,
}

impl Drop for RecordDisk {
    fn drop(&mut self) {
        if let Err(e) = self.persist() {
            let filename = &self.filename;
            error!("persist filename[{filename:?}] failed: {e:?}");
        }
    }
}

impl RecordDisk {
    fn new(filename: PathBuf, validate: bool, fd_cache: FdCache) -> Self {
        RecordDisk {
            filename,
            validate,
            fd_cache,
            loaded: AtomicBool::new(false),
            has_change: AtomicBool::new(false),
            last_defer_time: AtomicU64::new(0),
            records: ShardedLock::new(Vec::new()),
        }
    }

    fn load(&self) -> Result<()> {
        if !check_exist(&self.filename) {
            self.loaded.store(true, Relaxed);
            return Ok(());
        }
        if self.loaded.load(Relaxed) {
            return Ok(());
        }
        self.loaded.store(true, Relaxed);

        let fd = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.filename)?;
        let fd = self.fd_cache.get_or_insert(Path::new(&self.filename), fd);

        let rfd = fd.read();
        let meta = rfd.metadata()?;
        drop(rfd);

        let mut buf = BytesMut::new();
        buf.resize(meta.size() as _, 0);
        let mut wfd = fd.write();
        wfd.read_exact(&mut buf)?;
        let buf_vec = buf.to_vec();
        let cells = buf_vec.split(|n| *n == b'\n');
        for cs in cells {
            let line = String::from_utf8(cs.to_vec())?;
            if line.is_empty() {
                continue;
            }
            let record = MessageRecord::parse_from(&line)?;
            self.records
                .write()
                .expect("get sharedlock write failed")
                .push(record);
        }
        self.records
            .write()
            .expect("get sharedlock write failed")
            .sort_by_key(|c| c.defer_time);
        Ok(())
    }

    fn reset_with_filename(&mut self, filename: PathBuf) {
        self.filename = filename;
        self.records
            .write()
            .expect("get sharedlock write failed")
            .clear();
    }

    fn push(&self, record: MessageRecord, sort: bool) -> usize {
        self.has_change.store(true, Relaxed);
        let defer_time = record.defer_time;
        self.records
            .write()
            .expect("get sharedlock write failed")
            .push(record);
        if defer_time < self.last_defer_time.load(Relaxed) {
            self.records
                .write()
                .expect("get sharedlock write failed")
                .sort_by(|a, b| a.defer_time.cmp(&b.defer_time));
        } else {
            self.last_defer_time.store(defer_time, Relaxed);
        }

        0
    }

    fn persist(&self) -> Result<()> {
        if !self.has_change.load(Relaxed) {
            return Ok(());
        }
        let fd = self
            .fd_cache
            .get_or_create(&Path::new(&self.filename).to_path_buf())?;

        let mut bts = BytesMut::new();
        let mut wfd = fd.write();
        let read_records = self.records.read().expect("get sharedlock read failed");
        let iter = read_records.iter();
        for record in iter {
            // println!("record = {}", record.format());
            bts.extend(record.format().as_bytes());
        }
        drop(read_records);
        wfd.seek(SeekFrom::Start(0))?;
        wfd.write_all(&bts)?;
        drop(wfd);
        self.has_change.store(false, Relaxed);
        Ok(())
    }
}

#[derive(Debug)]
pub struct TimePtr {
    /// 存放defer_record的dir
    defer_dir: PathBuf,

    fd_cache: FdCache,

    /// 存放下列2行信息的文件名称（meta）
    filename: PathBuf,

    // 当前meta文件记录的filename
    record_filename: ShardedLock<PathBuf>,

    index: AtomicU64,
}

impl TimePtr {
    pub fn new(defer_dir: PathBuf, filename: PathBuf) -> Self {
        TimePtr {
            defer_dir,
            filename,
            fd_cache: FdCache::new(10),
            record_filename: ShardedLock::default(),
            index: AtomicU64::default(),
        }
    }

    fn read(&self) -> Result<(bool, Option<MessageRecord>)> {
        let record_rd = self
            .record_filename
            .read()
            .expect("get sharedlock read failed");
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

        // println!("index={}, line.len={}", index, lines.len());
        if index < lines.len() as u64 {
            let line = lines[index as usize];
            if line.is_empty() {
                // println!("index={} is empty", index);
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
                    let mut filenames = dir_recursive(self.defer_dir.clone()).unwrap();
                    filenames.sort();
                    for filename in filenames {
                        let mut record_wd = self
                            .record_filename
                            .write()
                            .expect("get sharedlock write failed");
                        if filename.cmp(&record_wd) == Ordering::Greater {
                            *record_wd = filename;
                            self.index.store(0, Relaxed);
                            break;
                        }
                    }
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
        if let Some(parent) = self.filename.parent() {
            if !check_exist(parent) {
                return Ok(());
            }
        }
        fs::write(
            &self.filename,
            format!(
                "{}\n{}",
                self.record_filename
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
        wd.clone_from(&record_filename);
        self.index.store(index as _, Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;
    use common::util::random_str;
    use rand::Rng as _;
    use std::path::{Path, PathBuf};

    fn new_time(template: &str) -> Result<RecordManagerStrategyTime> {
        RecordManagerStrategyTime::new(Path::new("").join(""), false, template, 10)
    }

    #[test]
    fn test_record_manager_strategy_time_new() {
        assert!(new_time("{daily:1}/{hourly:2}/{minutely:5}").is_ok());
        assert!(new_time("xxx/{daily:1}/{hourly:2}/{minutely:5}").is_ok());

        assert!(new_time("xxx/{hourly:2}/{minutely:5}").is_ok());
        assert!(new_time("xxx/{daily:1}/{minutely:5}").is_ok());
        assert!(new_time("xxx/{daily:1}/{hourly:2}").is_ok());

        assert!(new_time("xxx/{daily:1}").is_ok());
        assert!(new_time("xxx/{hourly:1}").is_ok());
        assert!(new_time("xxx/{minutely:1}").is_ok());

        // err
        assert!(new_time("xxx/{daily:1}/{minutely:5}/{hourly:2}").is_err());

        assert!(new_time("xxx/{hourly:2}/{daily:1}/{minutely:5}").is_err());
        assert!(new_time("xxx/{minutely:5}/{daily:1}/{hourly:2}").is_err());

        assert!(new_time("xxx/{hourly:2}/{minutely:5}/{daily:1}").is_err());
        assert!(new_time("xxx/{minutely:5}/{hourly:2}/{daily:1}").is_err());

        assert!(new_time("xxx/{minutely:5}/{hourly:2}").is_err());

        assert!(new_time("xxx/{minutely:5}/{daily:2}").is_err());

        assert!(new_time("xxx/{hourly:5}/{daily:2}").is_err());
    }

    #[test]
    fn test_record_manager_strategy_time_parse_template() {
        struct TestCase {
            rmst: RecordManagerStrategyTime,
            expect: ((String, u64), (String, u64), (String, u64)),
        }
        let cases = vec![
            TestCase {
                rmst: RecordManagerStrategyTime::new(
                    PathBuf::new(),
                    false,
                    "xxx/{minutely:5}/{hourly:2}",
                    10,
                )
                .expect("create rmst failed"),
                expect: (
                    ("".to_string(), 0),
                    ("{hourly:2}".to_string(), 2),
                    ("{minutely:5}".to_string(), 5),
                ),
            },
            TestCase {
                rmst: RecordManagerStrategyTime::new(
                    PathBuf::new(),
                    false,
                    "xxx/{daily}/{hourly:2}/{minutely:5}",
                    10,
                )
                .expect("create rmst failed"),
                expect: (
                    ("{daily}".to_string(), 1),
                    ("{hourly:2}".to_string(), 2),
                    ("{minutely:5}".to_string(), 5),
                ),
            },
        ];
        for case in cases {
            assert_eq!(case.rmst.parse_template(), case.expect);
        }
    }

    #[test]
    fn test_record_manager_strategy_time_deduce_filename() {
        let rmst = new_time("xxx/{daily:1}/{hourly:2}/{minutely:5}").unwrap();
        let filename = rmst.deduce_filename(Local::now().timestamp() as u64);
        println!("filename = {}", filename);

        let rmst = new_time("xxx/{daily:3}/{hourly:2}/{minutely:5}").unwrap();
        let filename = rmst.deduce_filename(Local::now().timestamp() as u64);
        println!("filename = {}", filename);

        let rmst = new_time("xxx/{hourly}/{minutely}").unwrap();
        let filename = rmst.deduce_filename(Local::now().timestamp() as u64);
        println!("filename = {}", filename);

        let rmst = new_time("xxx/{minutely:5}").unwrap();
        let filename = rmst.deduce_filename(Local::now().timestamp() as u64);
        println!("filename = {}", filename);

        let rmst = new_time("{daily:1}/{hourly:2}/{minutely:5}").unwrap();
        let filename =
            rmst.deduce_filename(Local::now().with_minute(0).unwrap().timestamp() as u64);
        println!("filename = {}", filename);
    }

    #[tokio::test]
    async fn test_record_manager_strategy_time_push_and_persist() {
        let rmst =
            new_time("../target/message/default/defer/record/{daily:1}/{hourly:2}/{minutely:1}")
                .unwrap();
        assert!(rmst.load().await.is_ok());

        let mut rng = rand::thread_rng();
        let length = rng.gen_range(5..50);
        let now: i64 = Local::now().timestamp();

        for i in 0..1000 {
            let id_str = random_str(length as _);
            assert!(rmst
                .push(MessageRecord {
                    factor: 0,
                    offset: 0,
                    length: 50,
                    id: id_str,
                    defer_time: now as u64 + rng.gen_range(10..50),
                    consume_time: 0,
                    delete_time: 0,
                })
                .await
                .is_ok());
            if i % 100 == 0 {
                assert!(rmst.persist().await.is_ok());
            }
        }

        assert!(rmst.persist().await.is_ok());
    }
}
