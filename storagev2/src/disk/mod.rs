mod compress;
pub mod config;
pub mod fd;
mod fd_cache;
mod index;
mod meta;
// mod partition_index;
mod prealloc;

pub mod reader;
pub mod writer;

pub use config::*;
pub use reader::*;
pub use writer::*;

const READER_PTR_FILENAME: &str = ".reader.ptr.group.";
const COMMIT_PTR_FILENAME: &str = ".commit.ptr.group.";
const ROCKSDB_INDEX_DIR: &str = "index";

fn gen_filename(factor: u64) -> String {
    format!("{:0>20}", factor)
}

pub fn gen_record_filename(factor: u64) -> String {
    format!("{}.record", gen_filename(factor))
}

pub fn gen_index_filename(factor: u64) -> String {
    format!("{}.index", gen_filename(factor))
}

/// 从文件名提取 segment_id
/// 例如："/path/to/00000000000000000123.record" -> 123
fn extract_segment_id_from_filename(filename: &std::path::Path) -> u64 {
    filename
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|s| s.strip_suffix(".record"))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}
