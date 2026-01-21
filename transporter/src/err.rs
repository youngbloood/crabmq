use std::{collections::HashMap, fmt::Display, sync::LazyLock};

static ERROR_MESSAGE: LazyLock<HashMap<ErrorCode, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(ErrorCode::ConnectionClosed, "Connection closed");
    m
});

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    ServiceShutdown = 999,
    ConnectionClosed = 1000,
    ConnectError = 1001,
    AcceptError = 1002,
    WriteError = 1003,
    WriteTimeoutError = 1010,
    ReadError = 1004,
    DecodeError = 1005,
    UnknownMessageTypeError = 1006,
    SendError = 1007,
    MaxIncomingReached = 1008,
    MaxOutgoingReached = 1009,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u16)
    }
}

pub struct TransporterError {
    pub code: ErrorCode,
    pub message: String,
}

impl TransporterError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn from_code(code: ErrorCode) -> Self {
        let message = ERROR_MESSAGE.get(&code).unwrap_or(&"Unknown error");
        Self {
            code,
            message: message.to_string(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<std::io::Error> for TransporterError {
    fn from(err: std::io::Error) -> Self {
        Self {
            code: ErrorCode::ConnectionClosed,
            message: err.to_string(),
        }
    }
}

impl Display for TransporterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TransporterError {{ code: {:?}, message: {} }}",
            self.code, self.message
        )
    }
}

impl Into<anyhow::Error> for TransporterError {
    fn into(self) -> anyhow::Error {
        anyhow::anyhow!("{} {}", self.code, self.message)
    }
}
