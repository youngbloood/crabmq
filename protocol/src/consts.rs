pub const X25: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

// ========= PROTOCOL VERSION =========
pub const PROPTOCOL_V1: u8 = 1;
// ========= PROTOCOL VERSION =========

// ========= PROTOCOL ACTION =========
// even number denote the Client -> Server
// odd number denote the Server -> Client
pub const ACTION_IDENTITY: u8 = 0;
pub const ACTION_IDENTITY_REPLY: u8 = 1;

pub const ACTION_AUTH: u8 = 2;
pub const ACTION_AUTH_REPLY: u8 = 3;

/// reply is [`ACTION_COMMON_REPLY`]
pub const ACTION_TOUCH: u8 = 4;

/// reply is [`ACTION_COMMON_REPLY`]
pub const ACTION_PUBLISH: u8 = 6;

/// reply is [`ACTION_COMMON_REPLY`]
pub const ACTION_SUBSCRIBE: u8 = 8;

/// subs重置超时时间
///
/// no reply
pub const ACTION_RESET: u8 = 11;

/// pubs更新msg的元信息
///
/// reply is [`ACTION_COMMON_REPLY`]
pub const ACTION_UPDATE: u8 = 12;

/// 客户端主动关闭链接
///
/// no reply
pub const ACTION_CLOSE: u8 = 14;

/// subs端标记一个消息被完整处理
///
/// no reply
pub const ACTION_FIN: u8 = 16;

// server给subs发送消息
pub const ACTION_MSG: u8 = 17;

// Server 通用响应
pub const ACTION_REPLY: u8 = 19;
// ========= PROTOCOL ACTION =========

pub const COMPRESS_TYPE_NONE: u8 = 0;

/// send message to broadcast all channel
pub const SUBSCRIBE_TYPE_BROADCAST_IN_CHANNEL: u8 = 0;

/// send message to round-robin a channel
pub const SUBSCRIBE_TYPE_ROUNDROBIN_IN_CHANNEL: u8 = 1;

/// send message to rand a channel
pub const SUBSCRIBE_TYPE_RAND_IN_CHANNEL: u8 = 2;

/// send message to rand-property a channel
pub const SUBSCRIBE_TYPE_RAND_PROPERTY_IN_CHANNEL: u8 = 3;

/// send message to broadcast all client
pub const SUBSCRIBE_TYPE_BROADCAST_IN_CLIENT: u8 = 0;

/// send message to round-robin a client
pub const SUBSCRIBE_TYPE_ROUNDROBIN_IN_CLIENT: u8 = 1;

/// send message to rand a client
pub const SUBSCRIBE_TYPE_RAND_IN_CLIENT: u8 = 2;

/// send message to rand-property a client
pub const SUBSCRIBE_TYPE_RAND_PROPERTY_IN_CLIENT: u8 = 3;

pub fn split_subscribe_type(t: u8) -> (u8, u8) {
    let sub_channel = t >> 4;
    let sub_client = t << 4 >> 4;
    (sub_channel, sub_client)
}
