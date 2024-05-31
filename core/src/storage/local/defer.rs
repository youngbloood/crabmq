use super::record::MessageRecord;
use super::{calc_cache_length, FileHandler, SPLIT_UNIT};
use super::{gen_filename, MetaManager};
use crate::message::Message;
use anyhow::Result;
use bytes::BytesMut;
use common::util::check_and_create_filename;
use parking_lot::RwLock;
use std::fs::OpenOptions;
use std::io::{SeekFrom, Write};
use std::{
    collections::HashMap,
    fs::{read_to_string, write},
    path::Path,
    vec,
};
use tokio::{fs::File, io::AsyncSeekExt};
use tracing::debug;

#[derive(Debug)]
pub struct DeferMessageMeta {
    dir: String,

    /// 读取的起始位置
    read_start: usize,

    /// 缓存打开的msg文件句柄
    cache_fds: RwLock<HashMap<String, FileHandler>>,

    /// 读取的长度
    read_length: usize,

    cache: Vec<Message>,

    /// defer中，record记录在meta文件中，剩余未消费消息的位置信息
    pub list: Vec<MessageRecord>,
}

impl DeferMessageMeta {
    pub async fn next(&mut self) -> Result<Option<Message>> {
        let read_start = self.list.get(self.read_start);
        if read_start.is_none() {
            return Ok(None);
        }
        let u = read_start.unwrap();
        let parent = Path::new(self.dir.as_str());
        let filename = parent.join(gen_filename(u.factor));
        let filename_str = filename.to_str().unwrap();

        let mut wg = self.cache_fds.write();
        if !wg.contains_key(filename_str) {
            let fd = File::open(filename_str).await?;
            wg.insert(filename_str.to_string(), FileHandler::new(fd));
        }

        let handler = wg.get_mut(filename_str).unwrap();
        let offset = u.offset;
        handler.fd.seek(SeekFrom::Start(u.offset)).await?;
        let (msg_opt, _) = handler.parse_message().await?;
        self.read_start += 1;
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

    fn load(&mut self) -> Result<()> {
        let metafile = self.meta_filename();
        check_and_create_filename(metafile.as_str())?;
        let content = read_to_string(metafile)?;
        if content.len() == 0 {
            return Ok(());
        }

        let lines: Vec<&str> = content.split(SPLIT_UNIT).collect();
        let mut iter = lines.iter();
        while let Some(line) = iter.next() {
            if line.len() == 0 {
                continue;
            }
            let unit = MessageRecord::parse_from(*line)?;
            self.list.push(unit);
        }
        self.read_length = calc_cache_length(self.list.len());
        Ok(())
    }

    fn update(&mut self, args: (u64, u64, u64, &str, u64)) {
        let mut unit = MessageRecord::default();
        unit.factor = args.0;
        unit.offset = args.1;
        unit.length = args.2;
        unit.id = args.3.to_string();
        unit.defer_time = args.4;

        self.list.push(unit);
        self.list.sort_by_key(|u| u.defer_time);
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

    fn meta_filename(&self) -> String {
        let parent = Path::new(self.dir.as_str());
        parent.join("meta").to_str().unwrap().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_defer_message_meta_next() {
        let mut defer = DeferMessageMeta::new("../target/message/defer");
        // let mut inst = InstantMessageMeta::new("../tsuixuqd/message/default/instant");
        if let Err(e) = defer.load() {
            panic!("{e}");
        }

        loop {
            match defer.next().await {
                Ok(msg_opt) => {
                    if msg_opt.is_none() {
                        break;
                    }
                    let msg = msg_opt.unwrap();
                    println!("msg = {msg:?}");
                }
                Err(e) => {
                    panic!("{e}");
                }
            }
        }
    }
}
