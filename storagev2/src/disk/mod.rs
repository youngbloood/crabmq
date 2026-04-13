pub mod config;
mod fd;
mod fd_cache;
mod flusher;
mod index;
mod partition;
mod record;
mod switch_queue;
// mod partition_index;
mod prealloc;

pub mod reader;
pub mod writer;

use std::ffi::OsString;

pub use config::*;
pub use reader::*;
pub use writer::*;

const READER_PTR_FILENAME: &str = ".reader.ptr.group.";
const COMMIT_PTR_FILENAME: &str = ".commit.ptr.group.";
const ROCKSDB_INDEX_DIR: &str = "index";
const RECORD_FILE_SUFFIX: &str = ".record";
const INDEX_FILE_SUFFIX: &str = ".index";

fn gen_filename(factor: u64) -> String {
    format!("{:0>20}", factor)
}

fn gen_index_filename(factor: u64) -> String {
    format!("{}{}", gen_filename(factor), INDEX_FILE_SUFFIX)
}

fn gen_record_filename(factor: u64) -> String {
    format!("{}{}", gen_filename(factor), RECORD_FILE_SUFFIX)
}

fn parse_factor(f: &str) -> u64 {
    if f.ends_with(INDEX_FILE_SUFFIX) {
        let factor = f.strip_suffix(INDEX_FILE_SUFFIX).unwrap();
        return factor.parse::<u64>().unwrap();
    }

    if f.ends_with(RECORD_FILE_SUFFIX) {
        let factor = f.strip_suffix(RECORD_FILE_SUFFIX).unwrap();
        return factor.parse::<u64>().unwrap();
    }
    0
}

fn is_record_filename(f: &str) -> bool {
    return f.ends_with(RECORD_FILE_SUFFIX);
}

fn is_index_filename(f: &str) -> bool {
    return f.ends_with(INDEX_FILE_SUFFIX);
}
