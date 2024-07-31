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
