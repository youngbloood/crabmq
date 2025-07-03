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
