use anyhow::Error;
use lazy_static::*;
use std::{
    collections::HashMap,
    fmt::{write, Display},
};

pub const PROT_ERR_CODE_ZERO: u8 = 0;
pub const PROT_ERR_CODE_TIMEOUT: u8 = 1;
pub const PROT_ERR_CODE_NOT_SUPPORT_VERSION: u8 = 1;
pub const PROT_ERR_CODE_NEED_MSG: u8 = 1;
pub const PROT_ERR_CODE_SHOULD_NOT_MSG: u8 = 2;
pub const PROT_ERR_CODE_EXCEED_MAX_NUM: u8 = 3;
pub const PROT_ERR_CODE_EXCEED_MAX_LEN: u8 = 4;
pub const PROT_ERR_CODE_SHOULD_NOT_REJECT_CODE: u8 = 5;

lazy_static! {
    static ref REASON_MAP: HashMap<u8, String> = {
        let mut m = HashMap::new();
        m.insert(PROT_ERR_CODE_ZERO, "not expect error".to_string());
        m.insert(PROT_ERR_CODE_TIMEOUT, "".to_string());
        m.insert(
            PROT_ERR_CODE_NOT_SUPPORT_VERSION,
            "not support protocol version".to_string(),
        );
        m.insert(
            PROT_ERR_CODE_NEED_MSG,
            "need message in protocol".to_string(),
        );
        m.insert(
            PROT_ERR_CODE_SHOULD_NOT_MSG,
            "should not have message in protocol".to_string(),
        );
        m.insert(
            PROT_ERR_CODE_EXCEED_MAX_NUM,
            "message number exceed maxnium number".to_string(),
        );
        m.insert(
            PROT_ERR_CODE_EXCEED_MAX_LEN,
            "message number exceed maxnium length".to_string(),
        );
        m.insert(
            PROT_ERR_CODE_SHOULD_NOT_REJECT_CODE,
            "should not have reject code or flag in protocol head".to_string(),
        );

        m
    };
}

#[derive(Debug)]
pub struct ProtocolError {
    pub code: u8,
    pub reason: String,
}

impl Display for ProtocolError {
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
impl From<Error> for ProtocolError {
    fn from(value: Error) -> Self {
        todo!()
    }
}

/// 将[`MessageError`]转成[`anyhow::Error`]
impl From<ProtocolError> for Error {
    fn from(value: ProtocolError) -> Self {
        todo!()
    }
}

impl ProtocolError {
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
