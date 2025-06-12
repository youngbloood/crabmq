use client::SubscriberConfig;
use common::random_str;
use std::sync::{Arc, atomic::AtomicU64};
use structopt::StructOpt;
use tokio::signal;

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "coo-addr", short = "c", default_value = "127.0.0.1:16001")]
    coo_addr: String,

    #[structopt(long = "group-id")]
    group_id: u32,

    #[structopt(long = "max-partition-fetch-bytes", default_value = "1048576")]
    max_partition_fetch_bytes: u64,

    #[structopt(long = "max-partition-fetch-count", default_value = "10")]
    max_partition_fetch_count: u64,

    // sub_topics 作为剩余参数，解析为 Vec<SubTopic>
    #[structopt(long = "sub-topics", parse(try_from_str = parse_sub_topic, ))]
    sub_topics: Vec<SubTopic>,
}
// 定义 SubTopic 结构体
#[derive(Debug)]
struct SubTopic {
    topic: String,
    offset: u8,
}

// 按最后一个 `:` 分割
fn parse_sub_topic(s: &str) -> Result<SubTopic, String> {
    // 找到最后一个 `:` 的位置
    match s.rsplit_once(':') {
        Some((topic, offset_str)) => {
            // 解析 offset
            let offset = offset_str
                .parse::<u8>()
                .map_err(|e| format!("Invalid offset in '{}': {}", s, e))?;
            Ok(SubTopic {
                topic: topic.to_string(),
                offset,
            })
        }
        None => Err(format!(
            "Invalid sub_topic format: '{}'. Expected 'topic:offset'",
            s
        )),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("Starting producer test with config: {:?}", args);

    // 创建客户端和发布者
    let cli = client::Client::new(random_str(6), vec![args.coo_addr.clone()]);
    let subscriber = cli.subscriber(SubscriberConfig {
        group_id: args.group_id,
        topics: args
            .sub_topics
            .iter()
            .map(|v| (v.topic.clone(), v.offset as u32))
            .collect(),
        break_when_broker_disconnect: false,
        max_partition_fetch_bytes: Arc::new(AtomicU64::new(args.max_partition_fetch_bytes)),
        max_partition_fetch_count: Arc::new(AtomicU64::new(args.max_partition_fetch_count)),
    })?;

    subscriber.clone().join().await?;
    subscriber
        .clone()
        .subscribe(|broker_info, messages, callback| async move {
            println!(
                "消费 broker[{}:{}] topic-partition[{}-{}] 消息: {} 条, last_offset: {:?}",
                broker_info.1,
                broker_info.0,
                messages.topic,
                messages.partition_id,
                messages.messages.len(),
                messages.messages.last().unwrap().offset.clone().unwrap(),
            );
            if let Err(e) = callback.ack().await {
                eprintln!("ack err: {e:?}");
            }
        })?;

    // 等待 Ctrl+C 信号
    println!("Running... Press Ctrl+C to stop.");
    signal::ctrl_c().await?;
    subscriber.close();
    println!("Received Ctrl+C, shutting down...");
    Ok(())
}
