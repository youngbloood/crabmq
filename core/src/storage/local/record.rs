use super::{SPLIT_CELL, SPLIT_UNIT};
use anyhow::{anyhow, Result};
use common::util::check_and_create_filename;
use std::{
    cmp::Ordering,
    fmt::format,
    fs::{self, read_to_string},
    path::PathBuf,
};
use tokio::fs::File;

pub struct MessageRecordManager {
    dir: PathBuf,

    factor: u64,

    // 每个文件记录的record大小
    record_num_per_file: u64,

    // 每个文件记录的record记录大小
    record_size_per_file: u64,

    writer: MessageRecordDisk,
}

impl MessageRecordManager {
    pub fn new(dir: PathBuf, record_num_per_file: u64, record_size_per_file: u64) -> Self {
        MessageRecordManager {
            dir,
            factor: 0,
            record_num_per_file,
            record_size_per_file,
            writer: MessageRecordDisk::new(""),
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        let dir = fs::read_dir(&self.dir)?;

        let iter = dir.into_iter();
        let mut max_record = 0_u64;
        for entry in iter {
            let entry = entry?;
            let filename = entry.file_name().into_string().unwrap();
            if filename.contains(".record") {
                if let Ok(record) = filename.as_str().trim_end_matches(".record").parse::<u64>() {
                    if record > max_record {
                        max_record = record;
                    }
                }
            }
        }
        self.factor = max_record;
        self.writer = MessageRecordDisk::new(gen_record_filename(self.factor).as_str());

        Ok(())
    }

    pub async fn push(&mut self, record: MessageRecord) -> Result<()> {
        if record.calc_len() as u64 + self.writer.size > self.record_size_per_file
            || self.writer.lines + 1 > self.record_num_per_file
        {
            self.persist().await?;
            self.rorate()
        }
        Ok(())
    }

    fn rorate(&mut self) {}

    async fn persist(&self) -> Result<()> {
        Ok(())
    }
}

struct MessageRecordDisk {
    filename: String,

    fd: Option<File>,

    lines: u64,
    size: u64,
    records: Vec<MessageRecord>,
}

impl MessageRecordDisk {
    fn new(filename: &str) -> Self {
        MessageRecordDisk {
            filename: filename.to_string(),
            fd: None,
            records: vec![],
            lines: 0,
            size: 0,
        }
    }
}

#[derive(Default, Debug)]
pub struct MessageRecord {
    pub factor: u64,
    pub offset: u64,
    pub length: u64,
    pub id: String,
    pub defer_time: u64,
}

impl MessageRecord {
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
        let unit = MessageRecord {
            factor: cells
                .first()
                .unwrap()
                .parse::<u64>()
                .expect("parse factor failed"),
            offset: cells
                .get(1)
                .unwrap()
                .parse::<u64>()
                .expect("parse offset failed"),
            length: cells
                .get(2)
                .unwrap()
                .parse::<u64>()
                .expect("parse length failed"),
            id: cells.get(3).unwrap().to_string(),
            defer_time: cells
                .get(4)
                .unwrap()
                .parse::<u64>()
                .expect("parse length failed"),
        };

        Ok(unit)
    }

    fn calc_len(&self) -> usize {
        self.format().len()
    }
}

/// instant消息的record单独存放在一个文件中
#[derive(Debug)]
pub struct MessageRecordFile {
    filename: String,
    write_offset: u64,
    list: Vec<MessageRecord>,
}

impl MessageRecordFile {
    pub fn new(filename: &str) -> Self {
        MessageRecordFile {
            filename: filename.to_string(),
            list: vec![],
            write_offset: 0,
        }
    }

    pub async fn load(&mut self) -> Result<()> {
        let filename = self.filename.as_str();
        check_and_create_filename(filename)?;
        let content = read_to_string(filename)?;
        if content.is_empty() {
            return Ok(());
        }

        let lines: Vec<&str> = content.split(SPLIT_UNIT).collect();
        let iter = lines.iter();
        for line in iter {
            if line.is_empty() {
                continue;
            }
            let unit = MessageRecord::parse_from(line)?;
            self.list.push(unit);
        }
        Ok(())
    }

    // TODO:
    pub async fn push(&mut self, record: MessageRecord) -> Result<()> {
        Ok(())
    }
}

fn gen_record_filename(factor: u64) -> String {
    format!("{:0>15}.record", factor)
}
