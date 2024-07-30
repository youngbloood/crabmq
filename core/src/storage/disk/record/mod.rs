mod fd_cache;
mod index;
mod normal;
mod time;

pub use fd_cache::*;
pub use normal::*;
pub use time::*;

use super::{gen_filename, SPLIT_CELL};
use anyhow::{anyhow, Result};
use std::{ops::Deref, path::PathBuf};

pub struct RecordManager<T>
where
    T: RecordManagerStrategy,
{
    // dir: PathBuf,
    pub strategy: T,
}

impl<T> RecordManager<T>
where
    T: RecordManagerStrategy,
{
    pub fn new(t: T) -> Self {
        RecordManager { strategy: t }
    }
}

impl<T> Deref for RecordManager<T>
where
    T: RecordManagerStrategy,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.strategy
    }
}

pub trait RecordManagerStrategy {
    /// load when process started
    async fn load(&self) -> Result<()>;

    /// push a MessageRecord into RecordManager, and return the file path and index of this record.
    async fn push(&self, record: MessageRecord) -> Result<(PathBuf, usize)>;

    /// find a record accord id, and return the file path and the record value.
    async fn find(&self, id: &str) -> Result<Option<(PathBuf, MessageRecord)>>;

    /// persist the records to the storage.
    async fn persist(&self) -> Result<()>;
}

#[derive(Default, Debug, Clone)]
pub struct MessageRecord {
    pub factor: u64,
    pub offset: u64,
    pub length: u64,
    pub id: String,
    pub defer_time: u64,
    pub consume_time: u64,
    pub delete_time: u64,
}

impl MessageRecord {
    pub fn format(&self) -> String {
        format!(
            "{}{SPLIT_CELL}{}{SPLIT_CELL}{}{SPLIT_CELL}{}{SPLIT_CELL}{}{SPLIT_CELL}{:0>16}{SPLIT_CELL}{:0>16}\n",
            self.factor, self.offset, self.length, self.id, self.defer_time, self.consume_time,self.delete_time
        )
    }

    pub fn parse_from(line: &str) -> Result<Self> {
        let line = line.trim_end();
        let cells: Vec<&str> = line.split(SPLIT_CELL).collect();
        if cells.len() < 7 {
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
            consume_time: cells
                .get(5)
                .unwrap()
                .parse::<u64>()
                .expect("parse length failed"),
            delete_time: cells
                .get(6)
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

#[cfg(test)]
mod tests {
    use super::MessageRecord;

    #[test]
    fn test_message_record_clone() {
        let record = MessageRecord {
            factor: 1,
            offset: 2,
            length: 3,
            id: "1111111".to_string(),
            defer_time: 4,
            consume_time: 5,
            delete_time: 6,
        };

        let mut record_clone = record.clone();
        println!("{:p}", &record.id);
        println!("{:p}", &record_clone.id);

        record_clone.id = "222222".to_string();
        record_clone.factor = 11;
        println!("{:?}", &record);
        println!("{:?}", &record_clone);
    }
}
