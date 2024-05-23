use super::{calc_cache_length, gen_filename, MetaManager, SPLIT_CELL};
use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use common::util::{check_and_create_filename, check_exist};
use std::{
    fs::{read_to_string, write},
    os::unix::fs::MetadataExt,
    path::Path,
    pin::Pin,
    vec,
};
use tokio::{fs::File, io::AsyncSeekExt};

pub struct InstantMessageMeta {
    dir: String,

    /// 从disk解析
    pub now: InstantMessageMetaUnit,

    pub read_start_pos: Option<InstantMessageMetaUnit>,

    pub read_num: usize,
}

#[derive(Default)]
pub struct InstantMessageMetaUnit {
    /// 总消息数量
    pub msg_num: u64,

    /// 剩余未消费数量
    pub left_num: u64,

    /// 剩余未消费消息的起始factor值
    pub factor: u64,

    /// 剩余未消费消息的offset
    pub offset: u64,
    pub length: u64,
}

impl InstantMessageMetaUnit {
    fn format(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.msg_num, self.left_num, self.factor, self.offset, self.length
        )
    }

    fn parse_from(&mut self, filename: &str) -> Result<()> {
        let content = read_to_string(filename)?;
        if content.len() == 0 {
            return Ok(());
        }
        let lines: Vec<&str> = content.split(SPLIT_CELL).collect();
        if lines.len() < 5 {
            return Err(anyhow!("not standard instant meta file"));
        }
        self.msg_num = lines
            .get(0)
            .unwrap()
            .parse::<u64>()
            .expect("parse msg_num failed");
        self.left_num = lines
            .get(1)
            .unwrap()
            .parse::<u64>()
            .expect("parse left_num failed");
        self.factor = lines
            .get(2)
            .unwrap()
            .parse::<u64>()
            .expect("parse factor failed");
        self.offset = lines
            .get(3)
            .unwrap()
            .parse::<u64>()
            .expect("parse offset failed");
        self.length = lines
            .get(4)
            .unwrap()
            .parse::<u64>()
            .expect("parse length failed");

        Ok(())
    }
}

impl InstantMessageMeta {
    async fn read_next(&mut self) -> Result<(Option<Message>, bool)> {
        let dir = self.dir.as_str();
        let parent = Path::new(dir);
        let mut read_factor = self.now.factor;

        let mut fd = File::open(parent.join(gen_filename(read_factor))).await?;
        let metadata = fd.metadata().await?;
        let mut offset = self.now.offset;
        let mut rolling = false;

        // 当前offset已经位于文件末，该滚动寻找下一个文件的消息
        if metadata.size() == self.now.offset {
            read_factor += 1;
            let next_filename = parent.join(gen_filename(read_factor));
            if !check_exist(next_filename.to_str().unwrap()) {
                return Ok((None, rolling));
            }
            rolling = true;
            fd = File::open(next_filename.to_str().unwrap()).await?;
            offset = 0;
        }
        fd.seek(std::io::SeekFrom::Start(offset as _)).await?;
        let mut fdp = Pin::new(&mut fd);
        let head = ProtocolHead::parse_from(&mut fdp).await?;
        let body = ProtocolBody::parse_from(&mut fdp).await?;

        Ok((Some(Message::with_one(head, body)), rolling))
    }
}

impl MetaManager for InstantMessageMeta {
    fn new(dir: &str) -> Self {
        InstantMessageMeta {
            dir: dir.to_string(),
            now: InstantMessageMetaUnit::default(),
            read_start_pos: None,
            read_num: 0,
        }
    }

    async fn consume(&mut self) -> Result<()> {
        let (next_msg, rolling) = self.read_next().await?;
        if rolling {
            self.now.factor += 1;
            self.now.offset = 0;
        } else {
            if let Some(nm) = next_msg.as_ref() {
                self.now.offset += nm.calc_len() as u64;
            }
        }
        if self.now.left_num != 0 {
            self.now.left_num -= 1;
        }
        if next_msg.is_some() {
            self.read_num += 1;
        }

        Ok(())
    }

    async fn read_to_cache(&mut self) -> Result<Vec<Message>> {
        if self.read_num == 0 {
            return Ok(vec![]);
        }
        let mut read_factor = self.now.factor;
        let parent = Path::new(self.dir.as_str());
        let mut fd = File::open(parent.join(gen_filename(read_factor))).await?;
        fd.seek(std::io::SeekFrom::Start(self.now.offset as _))
            .await?;
        let mut fdp = Pin::new(&mut fd);
        let mut list = vec![];

        while self.read_num != 0 {
            let head: ProtocolHead;
            match ProtocolHead::parse_from(&mut fdp).await {
                Ok(h) => head = h,
                Err(e) => {
                    if e.to_string().contains("eof") {
                        read_factor += 1;
                        fd = File::open(parent.join(gen_filename(read_factor))).await?;
                        fdp = Pin::new(&mut fd);
                        continue;
                    } else {
                        return Err(anyhow!(e));
                    }
                }
            }
            match ProtocolBody::parse_from(&mut fdp).await {
                Ok(body) => list.push(Message::with_one(head, body)),
                Err(e) => {
                    if e.to_string().contains("eof") {
                        read_factor += 1;
                        fd = File::open(parent.join(gen_filename(read_factor))).await?;
                        fdp = Pin::new(&mut fd);
                    } else {
                        return Err(anyhow!(e));
                    }
                }
            }
            self.read_num -= 1;
        }

        Ok(list)
    }

    fn meta_filename(&self) -> String {
        let parent = Path::new(self.dir.as_str());
        parent.join("meta").to_str().unwrap().to_string()
    }

    fn persist(&self) -> Result<()> {
        let content = self.now.format();
        write(self.meta_filename(), content)?;
        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        let filename = self.meta_filename();
        check_and_create_filename(filename.as_str())?;
        self.now.parse_from(filename.as_str())?;
        self.read_num = calc_cache_length(self.now.left_num as _);

        Ok(())
    }

    fn update(&mut self, args: (u64, u64, u64, &str, u64)) {
        if args.0 != 0 {
            self.now.factor = args.0;
        }
        if args.0 != 0 {
            self.now.offset = args.1;
        }
        if args.0 != 0 {
            self.now.length = args.2;
        }
        self.now.msg_num += args.4;
        self.now.left_num += args.4;
    }
}
