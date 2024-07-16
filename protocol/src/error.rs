use anyhow::Error;
use lazy_static::*;
use std::{
    collections::HashMap,
    fmt::{write, Display},
};

pub const ERR_ZERO: u8 = 0;
pub const ERR_TIMEOUT: u8 = 1;
pub const ERR_NOT_SUPPORT_VERSION: u8 = 1;
pub const RR_NEED_MSG: u8 = 1;
pub const ERR_SHOULD_NOT_MSG: u8 = 2;
pub const ERR_EXCEED_MAX_NUM: u8 = 3;
pub const ERR_EXCEED_MAX_LEN: u8 = 4;
pub const ERR_SHOULD_NOT_REJECT_CODE: u8 = 5;
pub const ERR_SHOULD_HAS_ID: u8 = 6;
pub const ERR_MSG_NUM_NOT_EQUAL: u8 = 7;

lazy_static! {
    static ref REASON_MAP: HashMap<u8, String> = {
        let mut m = HashMap::new();
        m.insert(ERR_ZERO, "not expect error".to_string());
        m.insert(ERR_TIMEOUT, "".to_string());
        m.insert(
            ERR_NOT_SUPPORT_VERSION,
            "not support protocol version".to_string(),
        );
        m.insert(RR_NEED_MSG, "need message in protocol".to_string());
        m.insert(
            ERR_SHOULD_NOT_MSG,
            "should not have message in protocol".to_string(),
        );
        m.insert(
            ERR_EXCEED_MAX_NUM,
            "message number exceed maxnium number".to_string(),
        );
        m.insert(
            ERR_EXCEED_MAX_LEN,
            "message number exceed maxnium length".to_string(),
        );
        m.insert(
            ERR_SHOULD_NOT_REJECT_CODE,
            "should not have reject code or flag in protocol head".to_string(),
        );
        m.insert(ERR_SHOULD_HAS_ID, "should has id in message".to_string());
        m.insert(
            ERR_MSG_NUM_NOT_EQUAL,
            "message num in head and bodys not equal".to_string(),
        );

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
