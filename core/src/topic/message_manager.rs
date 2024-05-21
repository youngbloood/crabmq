use crate::protocol::ProtocolBody;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use common::util::{check_and_create_dir, check_and_create_filename, check_exist, is_debug};
use parking_lot::RwLock;
use std::{
    cmp::Ordering,
    fs::{self, read_to_string, write},
    path::PathBuf,
    pin::Pin,
    vec,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

pub struct MessageManager {
    dir: PathBuf,

    instant: Manager<ImmediateMessageMeta>,

    defer: Manager<DeferMessageMeta>,
}

impl MessageManager {
    pub fn new(dir: PathBuf, max_msg_num_per_file: u64, max_size_per_file: u64) -> Self {
        let parent = dir.clone();
        MessageManager {
            dir: dir,
            instant: Manager::new(
                parent.join("instant"),
                ImmediateMessageMeta::new(),
                max_msg_num_per_file,
                max_size_per_file,
            ),
            defer: Manager::new(
                parent.join("defer"),
                DeferMessageMeta::new(),
                max_msg_num_per_file,
                max_size_per_file,
            ),
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        self.instant.load().await?;
        self.defer.load().await?;
        Ok(())
    }

    pub async fn push(&mut self, msg: ProtocolBody) -> Result<()> {
        if msg.is_defer() {
            self.defer.push(msg).await?;
            return Ok(());
        }
        self.instant.push(msg).await?;
        Ok(())
    }

    pub fn read(&mut self) -> Option<ProtocolBody> {
        None
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

    pub async fn push(&mut self, msg: ProtocolBody) -> Result<()> {
        if self.writer.msg_num >= self.max_msg_num_per_file
            || self.writer.msg_size + msg.calc_len() as u64 > self.max_size_per_file
        {
            let dir = self.dir.join(&self.prefix);
            self.writer.persist(dir.to_str().unwrap()).await?;
            self.rorate()?;
        }

        if msg.is_defer() {
            let id = msg.id.as_str();
            self.meta.update((
                self.factor,
                self.writer.msg_size,
                msg.calc_len() as _,
                id,
                msg.defer_time(),
            ));
        }
        self.writer.push(msg);
        Ok(())
    }

    pub fn read(&mut self) -> Option<ProtocolBody> {
        None
    }

    pub fn rorate(&mut self) -> Result<()> {
        self.factor += 1;
        self.writer = MessageDisk::new();
        self.writer.filename = self
            .dir
            .join(self.gen_filename())
            .to_str()
            .unwrap()
            .to_string();
        check_and_create_filename(self.writer.filename.as_str())?;
        Ok(())
    }

    fn gen_filename(&self) -> String {
        format!("{:0>15}", self.factor)
    }

    pub async fn load(&mut self) -> Result<()> {
        check_and_create_dir(self.dir.to_str().unwrap())?;
        let dir = self.dir.join(&self.prefix);
        let metafilename = T::meta_filename(dir.to_str().unwrap());
        check_and_create_filename(metafilename.as_str())?;
        self.meta.load(metafilename.as_str())?;

        let rd = fs::read_dir(self.dir.to_str().unwrap())?;
        // u64::MAX: 922_3372_0368_5477_5807
        let mut max_factor = 0_u64;
        let mut file_iter = rd.into_iter();
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
                Err(_) => continue,
            }
        }
        self.factor = max_factor;

        self.writer.filename = self
            .dir
            .join(self.gen_filename())
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
        let metafilename = T::meta_filename(dir.to_str().unwrap());
        self.meta.persist(metafilename.as_str())?;
        self.writer.persist(dir.to_str().unwrap()).await?;
        Ok(())
    }
}

/// 普通消息，一个消息对应一个文件
pub struct MessageDisk {
    filename: String,
    msg_size: u64,
    msg_num: u64,
    msgs: RwLock<Vec<ProtocolBody>>,

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

    pub fn push(&mut self, msg: ProtocolBody) {
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
            match ProtocolBody::parse_from(&mut pfd).await {
                Ok(pb) => {
                    self.msg_size += pb.calc_len() as u64;
                    self.msg_num += 1;
                    if is_debug() {
                        println!("pb = {pb:?}");
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
    fn new() -> Self;
    fn load(&mut self, filename: &str) -> Result<()>;
    fn update(&mut self, args: (u64, u64, u64, &str, u64));
    fn persist(&self, filename: &str) -> Result<()>;
    fn meta_filename(prefix: &str) -> String;
}

pub struct ImmediateMessageMeta {
    factor: u64,
    offset: u64,
    length: u64,
}

impl MetaManager for ImmediateMessageMeta {
    fn new() -> Self {
        ImmediateMessageMeta {
            factor: 0,
            offset: 0,
            length: 0,
        }
    }

    fn meta_filename(prefix: &str) -> String {
        format!("{prefix}meta")
    }

    fn persist(&self, filename: &str) -> Result<()> {
        let content = format!("{}\n{}\n{}", self.factor, self.offset, self.length);
        write(filename, content)?;
        Ok(())
    }

    fn load(&mut self, filename: &str) -> Result<()> {
        let content = read_to_string(filename)?;
        if content.len() == 0 {
            return Ok(());
        }
        let lines: Vec<&str> = content.split('\n').collect();
        if lines.len() < 3 {
            return Err(anyhow!("not standard instant meta file"));
        }
        self.factor = lines
            .get(0)
            .unwrap()
            .parse::<u64>()
            .expect("parse factor failed");
        self.offset = lines
            .get(1)
            .unwrap()
            .parse::<u64>()
            .expect("parse offset failed");
        self.length = lines
            .get(2)
            .unwrap()
            .parse::<u64>()
            .expect("parse length failed");

        Ok(())
    }

    fn update(&mut self, args: (u64, u64, u64, &str, u64)) {
        self.factor = args.0;
        self.offset = args.1;
        self.length = args.2;
    }
}

pub struct DeferMessageMeta {
    list: Vec<DeferMessageMetaUnit>,
}

#[derive(Default)]
struct DeferMessageMetaUnit {
    factor: u64,
    offset: u64,
    length: u64,
    id: String,
    defer_time: u64,
}

impl MetaManager for DeferMessageMeta {
    fn new() -> Self {
        DeferMessageMeta { list: vec![] }
    }

    fn meta_filename(prefix: &str) -> String {
        format!("{prefix}meta")
    }

    fn persist(&self, filename: &str) -> Result<()> {
        let mut content = BytesMut::new();
        let mut iter = self.list.iter();
        while let Some(unit) = iter.next() {
            content.extend(
                format!(
                    "{},{},{},{},{}\n",
                    unit.factor, unit.offset, unit.length, unit.id, unit.defer_time
                )
                .as_bytes(),
            );
        }
        write(filename, content)?;

        Ok(())
    }

    fn load(&mut self, filename: &str) -> Result<()> {
        let content = read_to_string(filename)?;
        if content.len() == 0 {
            return Ok(());
        }
        let lines: Vec<&str> = content.split('\n').collect();
        let mut iter = lines.iter();
        while let Some(line) = iter.next() {
            if line.len() == 0 {
                continue;
            }
            let cells: Vec<&str> = line.split(",").collect();
            if cells.len() < 5 {
                return Err(anyhow!("not standard defer meta file"));
            }
            let mut unit: DeferMessageMetaUnit = DeferMessageMetaUnit::default();
            unit.factor = cells
                .get(0)
                .unwrap()
                .parse::<u64>()
                .expect("parse factor failed");
            unit.offset = cells
                .get(1)
                .unwrap()
                .parse::<u64>()
                .expect("parse offset failed");
            unit.length = cells
                .get(2)
                .unwrap()
                .parse::<u64>()
                .expect("parse length failed");
            unit.id = cells.get(3).unwrap().to_string();
            unit.defer_time = cells
                .get(4)
                .unwrap()
                .parse::<u64>()
                .expect("parse length failed");
            self.list.push(unit);
        }

        Ok(())
    }

    fn update(&mut self, args: (u64, u64, u64, &str, u64)) {
        let mut unit = DeferMessageMetaUnit::default();
        unit.factor = args.0;
        unit.offset = args.1;
        unit.length = args.2;
        unit.id = args.3.to_string();
        unit.defer_time = args.4;

        self.list.push(unit);
        self.list.sort_by_key(|u| u.defer_time);
    }
}

#[cfg(test)]
pub mod tests {
    use super::MessageManager;
    use crate::protocol::ProtocolBody;
    use bytes::Bytes;
    use std::path::Path;

    #[test]
    fn test_1() {
        let c = format!("{:0>15}", 111);
        println!("{c}");
    }

    async fn load_mm() -> MessageManager {
        let p: &Path = Path::new("../target");
        let mut mm = MessageManager::new(p.join("message"), 10, 299);
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
    async fn test_message_manager_push_defer_and_persist() {
        let mut mm = load_mm().await;

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
            let _ = mm.push(body).await;
        }

        if let Err(e) = mm.persist().await {
            panic!("{e:?}");
        }
    }

    #[tokio::test]
    async fn test_message_manager_push_instant_and_persist() {
        let mut mm = load_mm().await;

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
            let _ = mm.push(body).await;
        }

        if let Err(e) = mm.persist().await {
            panic!("{e:?}");
        }
    }
}
