pub mod global;
pub mod util;

pub use util::*;

use anyhow::{Result, anyhow};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

/// ArcMux is std::sycn::Arc<parking_lot::Mutex<T>>
pub type ArcMux<T> = Arc<Mutex<T>>;

/// ArcRwMux is std::sycn::Arc<parking_lot::RwLock<T>>
pub type ArcRwMux<T> = Arc<RwLock<T>>;

pub struct Name(String);

impl Name {
    pub fn new(name: &str) -> Self {
        Name(name.to_string())
    }

    pub fn validate(&self) -> Result<()> {
        if self.0.is_empty() {
            return Err(anyhow!("illigal name"));
        }
        if self.0.len() >= 128 {
            return Err(anyhow!("too lang name"));
        }
        // TODO:

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_ligal(c: u8) -> bool {
    if c.is_ascii_lowercase() || c.is_ascii_uppercase() || c.is_ascii_digit() {
        return true;
    }
    if c == b'_' || c == b'-' {
        return true;
    }
    true
}
