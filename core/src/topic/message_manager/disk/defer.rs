use super::{calc_cache_length, gen_filename, MetaManager, SPLIT_CELL, SPLIT_UNIT};
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use common::util::check_and_create_filename;
use std::{
    collections::{HashMap, HashSet},
    fs::{read_to_string, write},
    path::Path,
    pin::Pin,
    vec,
};
use tokio::{fs::File, io::AsyncSeekExt};
use tracing::debug;

pub struct DeferMessageMeta {
    dir: String,

    /// 读取的起始位置
    read_start: usize,

    /// 读取的长度
    read_length: usize,

    /// 剩余未消费消息的位置信息
    pub list: Vec<DeferMessageMetaUnit>,
}

#[derive(Default)]
pub struct DeferMessageMetaUnit {
    factor: u64,
    offset: u64,
    length: u64,
    id: String,
    defer_time: u64,
}

impl DeferMessageMetaUnit {
    pub fn format(&self) -> String {
        format!(
            "{}{SPLIT_CELL}{}{SPLIT_CELL}{}{SPLIT_CELL}{}{SPLIT_CELL}{}\n",
            self.factor, self.offset, self.length, self.id, self.defer_time
        )
    }

    pub fn parse_from(line: &str) -> Result<Self> {
        let cells: Vec<&str> = line.split(SPLIT_CELL).collect();
        if cells.len() < 5 {
            return Err(anyhow!("not standard defer meta record"));
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

        Ok(unit)
    }
}

impl MetaManager for DeferMessageMeta {
    fn new(dir: &str) -> Self {
        DeferMessageMeta {
            dir: dir.to_string(),
            read_start: 0,
            read_length: 0,
            list: vec![],
        }
    }

    async fn consume(&mut self) -> Result<()> {
        if self.list.len() == 0 {
            return Ok(());
        }
        self.list.remove(0);
        if self.read_start != 0 {
            self.read_start -= 1;
            self.read_length += 1;
        }

        Ok(())
    }

    async fn read_to_cache(&mut self) -> Result<Vec<Message>> {
        // 查出所有的factor
        let cache_length = self.read_length;
        debug!("read {cache_length} defer message to cache");
        if cache_length == 0 {
            return Ok(vec![]);
        }
        let wait = &self.list[self.read_start..self.read_start + cache_length];
        let mut file_set = HashSet::new();
        wait.iter().for_each(|u| {
            file_set.insert(u.factor);
        });

        // 使用factor打开文件句柄
        let parent = Path::new(self.dir.as_str());
        let mut file_map = HashMap::new();
        let mut iter = file_set.iter();
        while let Some(f) = iter.next() {
            let filename = parent.join(gen_filename(*f));
            let fd = File::open(filename).await?;
            file_map.insert(f, fd);
        }
        // 遍历list并从file_map中拿到文件句柄解析数据
        let mut iter = wait.iter();
        let mut list = vec![];
        while let Some(u) = iter.next() {
            let fd = file_map.get_mut(&u.factor).unwrap();
            fd.seek(std::io::SeekFrom::Start(u.offset)).await?;
            let mut p = Pin::new(fd);

            let head = ProtocolHead::parse_from(&mut p).await?;
            let body = ProtocolBody::parse_from(&mut p).await?;

            let msg = Message::with_one(head, body);
            debug!("read to cache msg: {msg:?}");
            list.push(msg);
            self.read_length -= 1;
        }
        self.read_start = cache_length - 1;

        Ok(list)
    }

    fn meta_filename(&self) -> String {
        let parent = Path::new(self.dir.as_str());
        parent.join("meta").to_str().unwrap().to_string()
    }

    fn persist(&self) -> Result<()> {
        let mut content = BytesMut::new();
        let mut iter = self.list.iter();
        while let Some(unit) = iter.next() {
            content.extend(unit.format().as_bytes());
        }
        write(self.meta_filename(), content)?;

        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        let filename = self.meta_filename();
        check_and_create_filename(filename.as_str())?;
        let content = read_to_string(filename)?;
        if content.len() == 0 {
            return Ok(());
        }

        let lines: Vec<&str> = content.split(SPLIT_UNIT).collect();
        let mut iter = lines.iter();
        while let Some(line) = iter.next() {
            if line.len() == 0 {
                continue;
            }
            let unit = DeferMessageMetaUnit::parse_from(*line)?;
            self.list.push(unit);
        }
        self.read_length = calc_cache_length(self.list.len());
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
