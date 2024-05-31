use super::{SPLIT_CELL, SPLIT_UNIT};
use anyhow::{anyhow, Result};
use common::util::check_and_create_filename;
use std::fs::read_to_string;

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
        let mut unit: MessageRecord = MessageRecord::default();
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
        Ok(())
    }

    // TODO:
    pub async fn push(&mut self, record: MessageRecord) -> Result<()> {
        Ok(())
    }
}
