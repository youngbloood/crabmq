use semver::Version;
pub struct MetadataTsuixuq {
    version: Version,
    data_dir: String,
}

pub struct MetadataTopic {
    name: String,
    rorate_num: u64,
    consumed_msg_offset: u64,       // 消费了的消息偏移量
    consumed_defer_msg_offset: u64, // 消费了的延时消息偏移量

    msg_size_per_file: u64,
    defer_msg_size_per_file: u64,

    channels: Vec<MetadataChannel>,
}

pub struct MetadataChannel {
    name: String,
}

// impl MetadataTopic{
//     pub fn write_metadate(&self)
// }
