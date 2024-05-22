use super::{calc_cache_length, gen_filename, MetaManager};
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use std::{
    collections::{HashMap, HashSet},
    fs::{read_to_string, write},
    path::Path,
    pin::Pin,
    vec,
};
use tokio::{fs::File, io::AsyncSeekExt};

pub struct DeferMessageMeta {
    read_start: usize,
    read_end: usize,

    // 从disk中解析
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
        DeferMessageMeta {
            read_start: 0,
            read_end: 0,
            list: vec![],
        }
    }

    fn consume(&mut self, _dir: &str) -> Result<()> {
        if self.list.len() == 0 {
            return Ok(());
        }
        self.list.remove(0);
        if self.read_start != 0 {
            self.read_start -= 1;
        }
        Ok(())
    }

    async fn read_to_cache(&mut self, dir: &str) -> Result<Vec<Message>> {
        // 查出所有的factor
        let cache_length = calc_cache_length(self.list.len() as _);
        self.read_end = cache_length;
        let wait = &self.list[self.read_start..cache_length];
        let mut file_set = HashSet::new();
        wait.iter().for_each(|u| {
            file_set.insert(u.factor);
        });

        // 使用factor打开文件句柄
        let parent = Path::new(dir);
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

            list.push(Message::with_one(head, body));
        }

        self.read_start = cache_length;

        Ok(list)
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
        self.read_end = calc_cache_length(self.list.len());
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
