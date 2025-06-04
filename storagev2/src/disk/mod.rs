mod compress;
pub mod config;
mod fd_cache;
mod meta;
mod prealloc;
pub mod reader;
pub mod writer;

pub use config::*;
pub use reader::*;
pub use writer::*;

const READER_PTR_FILENAME: &str = ".reader.ptr.group.";
const COMMIT_PTR_FILENAME: &str = ".commit.ptr.group.";

pub enum StorageError {
    TopicNotFound(String),
    PartitionNotFound(String),
    RecordNotFound(String),
    PathNotExist(String),
}

impl ToString for StorageError {
    fn to_string(&self) -> String {
        match self {
            StorageError::TopicNotFound(key) => format!("[{}]: topic not found", key),
            StorageError::PartitionNotFound(key) => format!("[{}]: partition not found", key),
            StorageError::RecordNotFound(key) => format!("[{}]: record not found", key),
            StorageError::PathNotExist(key) => format!("[{}]: path not exist", key),
        }
    }
}
