/*!
 * Persist Messages into Storage and parse Messages from Storage
 */

mod disk;
mod dummy;
mod local;
pub mod storage;
pub use storage::*;

pub const STORAGE_TYPE_DUMMY: &str = "dummy";
pub const STORAGE_TYPE_LOCAL: &str = "local";
