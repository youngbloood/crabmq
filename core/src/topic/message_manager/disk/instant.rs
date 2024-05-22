use crate::{
    message::Message,
    protocol::{ProtocolBody, ProtocolHead},
};
use anyhow::{anyhow, Result};
use futures::executor::block_on;
use std::{
    fs::{read_to_string, write},
    os::unix::fs::MetadataExt,
    pin::Pin,
    vec,
};
use tokio::{fs::File, io::AsyncSeekExt};

use super::{calc_cache_length, gen_filename, MetaManager};

pub struct InstantMessageMeta {
    /// 从disk解析
    now: InstantMessageMetaUnit,

    read_start_pos: Option<InstantMessageMetaUnit>,

    read_num: usize,
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

impl InstantMessageMeta {
    async fn read_next(&mut self) -> Result<(ProtocolBody, bool)> {
        let mut read_factor = self.now.factor;
        let mut fd = File::open(gen_filename(read_factor)).await?;
        let metadata = fd.metadata().await?;
        let mut offset = self.now.offset;
        let mut rolling = false;
        if metadata.size() == self.now.offset {
            read_factor += 1;
            rolling = true;
            fd = File::open(gen_filename(read_factor)).await?;
            offset = 0;
        }
        fd.seek(std::io::SeekFrom::Start(offset as _));

        let mut fdp = Pin::new(&mut fd);
        match ProtocolBody::parse_from(&mut fdp).await {
            Ok(msg) => Ok((msg, rolling)),
            Err(e) => return Err(anyhow!(e)),
        }
    }
}

impl MetaManager for InstantMessageMeta {
    fn new() -> Self {
        InstantMessageMeta {
            now: InstantMessageMetaUnit::default(),
            read_start_pos: None,
            read_num: 0,
        }
    }

    fn consume(&mut self) -> Result<()> {
        let (next_msg, rolling) = block_on(self.read_next())?;
        if rolling {
            self.now.factor += 1;
            self.now.offset = 0;
        } else {
            self.now.offset += next_msg.calc_len() as u64;
        }
        self.now.left_num -= 1;

        self.read_num += 1;

        Ok(())
    }

    async fn read_to_cache(&mut self) -> Result<Vec<Message>> {
        let mut read_factor = self.now.factor;
        let mut fd = File::open(gen_filename(read_factor)).await?;
        fd.seek(std::io::SeekFrom::Start(self.now.offset as _));
        let mut fdp = Pin::new(&mut fd);
        let mut list = vec![];
        self.read_num = calc_cache_length(self.now.left_num as _);
        while self.read_num != 0 {
            self.read_num -= 1;

            let head: ProtocolHead = ProtocolHead::parse_from(&mut fdp).await?;
            match ProtocolBody::parse_from(&mut fdp).await {
                Ok(body) => list.push(Message::with_one(head, body)),
                Err(e) => {
                    if e.to_string().contains("eof") {
                        read_factor += 1;
                        fd = File::open(gen_filename(read_factor)).await?;
                        fdp = Pin::new(&mut fd);
                    } else {
                        return Err(anyhow!(e));
                    }
                }
            }
        }

        Ok(list)
    }

    fn meta_filename(prefix: &str) -> String {
        format!("{prefix}meta")
    }

    fn persist(&self, filename: &str) -> Result<()> {
        let content = format!(
            "{}\n{}\n{}\n{}",
            self.now.msg_num, self.now.factor, self.now.offset, self.now.length
        );
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
        self.now.factor = lines
            .get(0)
            .unwrap()
            .parse::<u64>()
            .expect("parse factor failed");
        self.now.offset = lines
            .get(1)
            .unwrap()
            .parse::<u64>()
            .expect("parse offset failed");
        self.now.length = lines
            .get(2)
            .unwrap()
            .parse::<u64>()
            .expect("parse length failed");

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
    }
}
