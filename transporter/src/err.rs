use std::{collections::HashMap, fmt::Display, sync::LazyLock};

static ERROR_MESSAGE: LazyLock<HashMap<ErrorCode, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(ErrorCode::DecodeError, "Failed to decode message");
    m.insert(ErrorCode::ReadError, "Failed to read from connection");
    m.insert(
        ErrorCode::ReadTimeoutError,
        "Read from connection timed out",
    );
    m.insert(ErrorCode::WriteError, "Failed to write to connection");
    m.insert(
        ErrorCode::WriteTimeoutError,
        "Write to connection timed out",
    );
    m.insert(ErrorCode::SendError, "Failed to send message");
    m.insert(
        ErrorCode::ExceedMaxMessageSize,
        "Message size exceeds the maximum allowed",
    );
    m.insert(ErrorCode::UnknownMessageTypeError, "Unknown message type");
    m.insert(ErrorCode::ConnectionClosed, "Connection closed");
    m.insert(ErrorCode::ConnectTimeout, "Connection timed out");
    m.insert(ErrorCode::ConnectError, "Failed to connect to server");
    m.insert(
        ErrorCode::MaxOutgoingReached,
        "Maximum outgoing connections reached",
    );
    m.insert(ErrorCode::ServiceShutdown, "Service is shutting down");
    m.insert(ErrorCode::AcceptError, "Failed to accept connection");
    m.insert(
        ErrorCode::MaxIncomingReached,
        "Maximum incoming connections reached",
    );

    m
});

/**
 * 错误码定义
 * 20xx: 公共错误码
 * 21xx: client 相关错误码
 * 22xx: service 相关错误码
 */
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    DecodeError = 2001,
    ReadError = 2002,
    ReadTimeoutError = 2003,
    WriteError = 2004,
    WriteTimeoutError = 2005,
    SendError = 2006,
    ExceedMaxMessageSize = 2007,
    UnknownMessageTypeError = 2008,
    ConnectionClosed = 2009,

    ConnectTimeout = 2101,
    ConnectError = 2102,
    MaxOutgoingReached = 2103,

    ServiceShutdown = 2201,
    AcceptError = 2202,
    MaxIncomingReached = 2203,
}

impl Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Code: {:?}", *self as u16)
    }
}

#[derive(Debug)]
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
        write!(f, "Code: {:?}, Message: {} }}", self.code, self.message)
    }
}

impl std::error::Error for TransporterError {}


