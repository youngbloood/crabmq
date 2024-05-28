use super::{
    calc_cache_length, gen_filename, record::MessageRecordFile, FileHandler, MetaManager,
    SPLIT_CELL,
};
use crate::message::Message;
use anyhow::{anyhow, Result};
use common::{
    global::{Guard, CANCEL_TOKEN},
    util::{check_and_create_filename, check_exist},
};
use std::{
    fs::{read_to_string, write},
    os::unix::fs::MetadataExt,
    path::Path,
};
use tokio::{fs::File, io::AsyncSeekExt, select, sync::mpsc::Sender};
use tracing::{error, info};

pub fn write_instant_to_cache(guard: Guard<InstantMessageMeta>, sender: Sender<Message>) {
    // 查出所有的factor
    tokio::spawn(async move {
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return;
                }

                result = guard.get_mut().next() => {
                    match result {
                        Ok((msg_opt,_)) => {
                             // TODO: 这里的消息写到InstantMessageMeta自身缓存中，可以多缓存消息数量，提高性能
                            if let Some(msg) = msg_opt {
                                info!("从instant获取到消息111:{msg:?}");
                                let _ = sender.send(msg).await;
                            }
                        }
                        Err(e) => {
                            error!("{e}");
                        },
                    }
                }
            }
        }
    });
}

pub struct InstantMessageMeta {
    dir: String,

    /// 内存中已经加载了的消息，因此从该位置继续加载
    pub read_ptr: InstantMessageMetaUnit,

    /// 读取的数量
    pub read_num: usize,

    /// 从disk解析：meta文件，消费的位置
    pub now: InstantMessageMetaUnit,

    /// instant消息的record文件
    pub record: Option<MessageRecordFile>,
}

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

impl Default for InstantMessageMetaUnit {
    fn default() -> Self {
        Self {
            msg_num: 0,
            left_num: 0,
            factor: 0,
            offset: 16,
            length: 0,
        }
    }
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
    async fn next(&mut self) -> Result<(Option<Message>, bool)> {
        let dir = self.dir.as_str();
        let parent = Path::new(dir);
        let mut read_factor = self.read_ptr.factor;

        let mut fd = File::open(parent.join(gen_filename(read_factor))).await?;
        let metadata = fd.metadata().await?;
        let mut offset = self.read_ptr.offset;
        let mut rolling = false;

        // 当前offset已经位于文件末，该滚动寻找下一个文件的消息
        if metadata.size() == self.read_ptr.offset {
            read_factor += 1;
            let next_filename = parent.join(gen_filename(read_factor));
            if !check_exist(next_filename.to_str().unwrap()) {
                return Ok((None, false));
            }
            rolling = true;
            fd = File::open(next_filename.to_str().unwrap()).await?;
            offset = 16;
        }

        let mut handler = FileHandler::new(fd);
        handler
            .fd
            .seek(std::io::SeekFrom::Start(offset as _))
            .await?;

        match handler.parse_message().await {
            Ok((msg_opt, _rolling)) => {
                if rolling {
                    self.read_ptr.factor += 1;
                    if msg_opt.is_some() {
                        self.read_ptr.offset = 16 + msg_opt.as_ref().unwrap().calc_len() as u64;
                    }
                } else if msg_opt.is_some() {
                    self.read_ptr.offset += msg_opt.as_ref().unwrap().calc_len() as u64;
                }
                return Ok((msg_opt, rolling));
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
}

impl MetaManager for InstantMessageMeta {
    fn new(dir: &str) -> Self {
        InstantMessageMeta {
            dir: dir.to_string(),
            now: InstantMessageMetaUnit::default(),
            read_ptr: InstantMessageMetaUnit::default(),
            read_num: 0,
            record: None,
        }
    }

    async fn consume(&mut self) -> Result<()> {
        let (next_msg, rolling) = self.next().await?;
        if rolling {
            self.now.factor += 1;
            self.now.offset = 16;
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
        let metafile = self.meta_filename();
        check_and_create_filename(metafile.as_str())?;
        self.now.parse_from(metafile.as_str())?;
        self.read_ptr.parse_from(metafile.as_str())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_instant_message_meta_next() {
        let mut inst = InstantMessageMeta::new("../target/message/instant");
        // let mut inst = InstantMessageMeta::new("../tsuixuqd/message/default/instant");
        if let Err(e) = inst.load() {
            panic!("{e}");
        }

        while let Ok((msg_opt, _)) = inst.next().await {
            if msg_opt.is_none() {
                break;
            }
            let msg = msg_opt.unwrap();
            println!("msg = {msg:?}");
        }
    }
}
