mod compress;
pub mod config;
mod fd_cache;
mod meta;
mod prealloc;
pub mod reader;
pub mod writer;

use anyhow::Error;
pub use config::*;
pub use reader::*;
pub use writer::*;

const READER_PTR_FILENAME: &str = ".reader.ptr.group.";
const COMMIT_PTR_FILENAME: &str = ".commit.ptr.group.";

pub enum StorageError {
    TopicNotFound,
    PartitionNotFound,
    RecordNotFound,
    PathNotExist,
}

impl ToString for StorageError {
    fn to_string(&self) -> String {
        match self {
            StorageError::TopicNotFound => "topic not found".to_string(),
            StorageError::PartitionNotFound => "partition not found".to_string(),
            StorageError::RecordNotFound => "record not found".to_string(),
            StorageError::PathNotExist => "path not exist".to_string(),
        }
    }
}
