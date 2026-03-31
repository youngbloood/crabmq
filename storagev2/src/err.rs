use std::{collections::HashMap, fmt::Display, sync::LazyLock};

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq)]
pub struct StorageError {
    pub code: ErrorCode,
    pub message: String,
}

impl StorageError {
    pub fn new(code: ErrorCode) -> Self {
        let message = ERROR_MESSAGE
            .get(&code)
            .unwrap_or(&"Unknown error code")
            .to_string();
        Self { code, message }
    }

    pub fn with_message(code: ErrorCode, message: String) -> Self {
        Self { code, message }
    }
}

impl Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[StorageError] code: {:?}, message: {}",
            self.code, self.message
        )
    }
}

/**
 * 错误码设计：
 * 10xx: 校验和参数错误
 * 11xx: 主题和分区相关错误
 * 12xx: 记录和数据相关错误
 * 13xx: IO 和文件系统相关错误
 * 14xx: 序列化相关错误
 * 15xx: 权限和安全相关错误
 * 16xx: 消息消费相关错误
 * 17xx: 其他未知错误
 */
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub enum ErrorCode {
    MsgIDTooShort = 1001,
    MsgIDTooLong = 1002,
    MetadataTooMany = 1003,
    MetadataKeyTooLong = 1004,
    MetadataValueTooLong = 1005,
    TimestampInvalid = 1006,
    PayloadTooShort = 1007,
    PayloadTooLong = 1008,

    TopicNotFound = 1101,
    PartitionNotFound = 1102,

    RecordNotFound = 1201,
    EmptyData = 1202,

    PathNotExist = 1301,
    IoError = 1302,
    DiskFull = 1303,

    SerializeError = 1401,

    PermissionDenied = 1501,
    NoMoreMessages = 1601,
    OffsetMismatch = 1602,
    Unknown = 1701,
}

static ERROR_MESSAGE: LazyLock<HashMap<ErrorCode, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert(ErrorCode::MsgIDTooLong, "Message ID length exceeds maximum");
    m.insert(ErrorCode::MetadataTooMany, "Too many metadata entries");
    m.insert(
        ErrorCode::MetadataKeyTooLong,
        "Metadata key length exceeds maximum",
    );
    m.insert(
        ErrorCode::MetadataValueTooLong,
        "Metadata value length exceeds maximum",
    );
    m.insert(ErrorCode::PayloadTooLong, "Payload length exceeds maximum");
    m.insert(ErrorCode::TopicNotFound, "Topic not found");
    m.insert(ErrorCode::PartitionNotFound, "Partition not found");
    m.insert(ErrorCode::RecordNotFound, "Record not found");
    m.insert(ErrorCode::PathNotExist, "Path does not exist");
    m.insert(ErrorCode::EmptyData, "Data is empty");
    m.insert(ErrorCode::IoError, "IO error occurred");
    m.insert(ErrorCode::SerializeError, "Serialization error occurred");
    m.insert(ErrorCode::DiskFull, "Disk is full");
    m.insert(ErrorCode::PermissionDenied, "Permission denied");
    m.insert(ErrorCode::NoMoreMessages, "No more messages available");
    m.insert(ErrorCode::OffsetMismatch, "Offset mismatch");
    m.insert(ErrorCode::Unknown, "Unknown error occurred");

    m
});
