pub mod global;
pub mod util;

use anyhow::{anyhow, Result};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};
use tokio::sync::Mutex;

pub type ArcRefCell<T> = Arc<RefCell<T>>;
pub type ArcCell<T> = Arc<Cell<T>>;
pub type ArcMuxRefCell<T> = Arc<Mutex<RefCell<T>>>;
pub type ArcMux<T> = Arc<Mutex<T>>;

pub struct Name(String);

impl Name {
    pub fn new(name: &str) -> Self {
        Name(name.to_string())
    }

    pub fn validate(&self) -> Result<()> {
        if self.0.len() == 0 {
            return Err(anyhow!("illigal name"));
        }
        if self.0.len() >= 128 {
            return Err(anyhow!("too lang name"));
        }
        // TODO:

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0.as_str()
    }
}

fn is_ligal(c: u8) -> bool {
    if c >= 'a' as u8 && c <= 'z' as u8 {
        return true;
    }
    if c >= 'A' as u8 && c <= 'A' as u8 {
        return true;
    }
    if c >= '0' as u8 && c <= '9' as u8 {
        return true;
    }
    if c == '_' as u8 || c == '-' as u8 {
        return true;
    }
    true
}
