mod fd_cache;
mod normal;
mod time;

use bytes::BytesMut;
pub use fd_cache::*;
pub use normal::*;
pub use time::*;

use super::{gen_filename, SPLIT_CELL};
use anyhow::{anyhow, Result};
use chrono::Local;
use std::{
    fs::File,
    ops::{Deref, DerefMut},
    path::PathBuf,
};

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

#[derive(Default, Debug)]
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

// #[derive(Debug)]
// struct FileHandler {
//     fd: File,

//     /// 最后使用的时间戳
//     last: u64,
// }

// impl FileHandler {
//     fn new(fd: File) -> Self {
//         FileHandler {
//             fd,
//             last: Local::now().timestamp() as u64,
//         }
//     }
// }

// impl Deref for FileHandler {
//     type Target = File;
//     fn deref(&self) -> &Self::Target {
//         &self.fd
//     }
// }

// impl DerefMut for FileHandler {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.fd
//     }
// }
