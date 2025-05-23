use anyhow::Error;
use lazy_static::*;
use std::{
    collections::HashMap,
    fmt::{write, Display},
};

// ================== COMMON ERR CODE 0-60 ================
pub const OK: u8 = 0;
pub const E_PROT_NOT_SUPPORT_VERSION: u8 = 1;
pub const E_ACTION_NOT_SUPPORT: u8 = 2;
pub const E_BAD_CRC: u8 = 3;
pub const E_NEED_IDENTITY: u8 = 4;
pub const E_NEED_AUTH: u8 = 5;
// ================== COMMON ERR CODE 0-60 ================

// ================== BUSNIESS ERR CODE 61-256 ================
// ======================== PUBLISH ===========================
pub const E_TOPIC_PROHIBIT_TYPE: u8 = 61;
pub const E_TOPIC_PROHIBIT_DEFER: u8 = 62;
pub const E_TOPIC_PROHIBIT_INSTANT: u8 = 63;
// 超过topic上限
pub const E_TOPIC_EXCESS_UPPER_LIMIT: u8 = 64;
pub const E_TOPIC_GET_FAILED: u8 = 65;
pub const E_TOPIC_CREATE_FAILED: u8 = 66;

pub const E_PUBLISH_FAILED: u8 = 67;
// ================== BUSNIESS ERR CODE 61-256 ================

lazy_static! {
    static ref REASON_MAP: HashMap<u8, String> = {
        let mut m = HashMap::new();
        m.insert(OK, "ok".to_string());
        m.insert(
            E_PROT_NOT_SUPPORT_VERSION,
            "not support protocol version".to_string(),
        );
        m.insert(E_ACTION_NOT_SUPPORT, "not support action".to_string());

        m
    };
}

#[derive(Debug)]
pub struct ProtError {
    pub code: u8,
    pub reason: String,
}

impl Display for ProtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.code;
        let reason = self.reason.as_str();
        write(
            f,
            format_args!("Invalid Message, Code[{code}], Reason:[{reason}]"),
        )
    }
}

/// 将[`anyhow::Error`]转成[`MessageError`]
impl From<Error> for ProtError {
    fn from(value: Error) -> Self {
        todo!()
    }
}

/// 将[`MessageError`]转成[`anyhow::Error`]
impl From<ProtError> for Error {
    fn from(value: ProtError) -> Self {
        Error::msg(format!("code: {}, msg: {}", value.code, value.reason))
    }
}

impl ProtError {
    pub fn new(code: u8) -> Self {
        Self {
            code,
            reason: REASON_MAP
                .get(&code)
                .unwrap_or(&"unknown reason".to_string())
                .to_string(),
        }
    }
}
