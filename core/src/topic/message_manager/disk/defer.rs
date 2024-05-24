use super::{calc_cache_length, gen_filename, FileHandler, MetaManager, SPLIT_CELL, SPLIT_UNIT};
use crate::message::Message;
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use chrono::Local;
use common::global::Guard;
use common::{global::CANCEL_TOKEN, util::check_and_create_filename};
use parking_lot::RwLock;
use std::time::Duration;
use std::{
    collections::HashMap,
    fs::{read_to_string, write},
    path::Path,
    vec,
};
use tokio::time::interval;
use tokio::{fs::File, io::AsyncSeekExt, select, sync::mpsc::Sender};

pub fn write_defer_to_cache(guard: Guard<DeferMessageMeta>, sender: Sender<Message>) {
    // 循环读取
    tokio::spawn(async move {
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }

                result = guard.get_mut().next() => {
                    match result {
                        Ok(msg_opt) => {
                            if let Some(msg) = msg_opt {
                                // TODO: 这里的消息写到DeferMessageMeta自身缓存中，可以多缓存消息数量，提高性能
                                let defer_time = msg.defer_time() - Local::now().timestamp() as u64;
                                if defer_time > 0 {
                                    let mut ticker = interval(Duration::from_secs(defer_time));
                                    ticker.tick().await;
                                    let _ = sender.send(msg).await;
                                }
                            }
                        }
                        Err(_) => todo!(),
                    }
                }
            }
        }
    });
}

pub struct DeferMessageMeta {
    dir: String,

    /// 读取的起始位置
    read_start: usize,

    /// 缓存打开的msg文件句柄
    cache_fds: RwLock<HashMap<String, FileHandler>>,

    /// 读取的长度
    read_length: usize,
    cache: Vec<Message>,

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

impl DeferMessageMeta {
    pub async fn next(&mut self) -> Result<Option<Message>> {
        let u = self.list.get(self.read_start).unwrap();
        let filename = gen_filename(u.factor);
        let filename_str = gen_filename(u.factor);
        let mut wg = self.cache_fds.write();
        if !wg.contains_key(filename_str.as_str()) {
            let fd = File::open(filename.as_str()).await?;
            wg.insert(filename, FileHandler::new(fd));
        }
        let handler = wg.get_mut(filename_str.as_str()).unwrap();
        handler.fd.seek(std::io::SeekFrom::Start(u.offset)).await?;
        let (msg_opt, _) = handler.parse_message().await?;
        if msg_opt.is_some() {
            self.read_start += 1;
        }
        return Ok(msg_opt);
    }
}

impl MetaManager for DeferMessageMeta {
    fn new(dir: &str) -> Self {
        DeferMessageMeta {
            dir: dir.to_string(),
            read_start: 0,
            read_length: 0,
            list: vec![],
            cache: vec![],
            cache_fds: RwLock::new(HashMap::new()),
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
        // (self.cache_tx, self.cache_rx) = mpsc::channel(self.read_length);
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
