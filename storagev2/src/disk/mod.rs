mod compress;
pub mod config;
mod fd_cache;
mod meta;
mod partition_index;
mod prealloc;
pub mod reader;
pub mod writer;

pub use config::*;
pub use reader::*;
pub use writer::*;

use once_cell::sync::OnceCell;
pub use partition_index::PartitionIndexManager;
use std::{path::PathBuf, sync::Arc};

const READER_PTR_FILENAME: &str = ".reader.ptr.group.";
const COMMIT_PTR_FILENAME: &str = ".commit.ptr.group.";
const ROCKSDB_INDEX_DIR: &str = "index";

// static GLOBAL_PARTITION_INDEX: OnceCell<Arc<PartitionIndexManager>> = OnceCell::new();

// pub fn init_global_partition_index(storage_dir: &PathBuf, index_num_per_topic: usize) {
//     GLOBAL_PARTITION_INDEX.get_or_init(|| {
//         Arc::new(PartitionIndexManager::new(
//             storage_dir.clone(),
//             index_num_per_topic,
//         ))
//     });
// }

// // 获取全局索引
// fn get_global_partition_index() -> Arc<PartitionIndexManager> {
//     GLOBAL_PARTITION_INDEX
//         .get()
//         .expect("GlobalPartitionIndex not initial")
//         .clone()
// }
