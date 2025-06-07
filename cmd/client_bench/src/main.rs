use bytes::Bytes;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::{Barrier, mpsc};
use tokio::time;

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "coo-addr", short = "c", default_value = "127.0.0.1:16001")]
    coo_addr: String,

    /// The number of producer
    #[structopt(long = "num-producers", short = "p", default_value = "100")]
    num_producers: usize,

    /// Every producer product message number
    #[structopt(long = "num-messages", short = "m", default_value = "10000")]
    num_messages: usize,

    /// Every message size
    #[structopt(long = "message-size", short = "s", default_value = "1024")]
    message_size: usize,

    /// Topic name
    #[structopt(long = "topic", short = "t")]
    topic: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    println!("Starting producer test with config: {:?}", args);
    // 创建消息内容（一次性创建，避免重复分配）
    let message_content: Arc<Vec<Bytes>> = Arc::new(
        (0..26)
            .map(|v| Bytes::from(vec![b'a' + v; args.message_size]))
            .collect(),
    );

    // 模拟 num_producers 个客户端
    let mut txs = vec![];
    let mut conn = vec![];
    for producer_id in 0..args.num_producers {
        // 创建客户端和发布者
        let cli = client::Client::new(producer_id.to_string(), vec![args.coo_addr.clone()]);
        let publisher = cli.publisher(args.topic.clone()).await;

        publisher.new_topic(0).await?;
        // 创建通道
        let (tx, rx) = mpsc::channel(1);
        publisher
            .publish(
                rx,
                |res| async {
                    if let Err(e) = res {
                        eprintln!("Publish error: {:?}", e);
                    }
                },
                true,
            )
            .await?;

        conn.push((cli, publisher));
        txs.push((producer_id, tx));
    }

    println!("准备进入消息发送阶段");
    // 创建生产者任务

    let mut handles = Vec::with_capacity(args.num_producers);
    let barrier = Arc::new(Barrier::new(txs.len() + 1));
    for tx in txs {
        let message_content = message_content.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for msg_id in 0..args.num_messages {
                // 创建带唯一标识的键
                // let key = format!("prod-{}-msg-{}", producer_id, msg_id);

                if let Err(e) =
                    tx.1.send((
                        "".to_string(),
                        message_content[msg_id % message_content.len()].clone(),
                    ))
                    .await
                {
                    eprintln!("Failed to send message: {e:?}");
                    break;
                }

                // 添加少量延迟以模拟真实场景
                // if msg_id % 100 == 0 {
                //     time::sleep(Duration::from_micros(10)).await;
                // }
            }
            println!("Producer[{}] finished", tx.0);
        }));
    }

    let start_time = time::Instant::now();
    barrier.wait().await;
    // 等待所有生产者完成
    join_all(handles).await;

    for (cli, pulisher) in conn {
        drop(pulisher);
        drop(cli);
    }

    // 计算并打印性能指标
    let elapsed = start_time.elapsed();
    let total_messages = args.num_producers * args.num_messages;
    let rate = total_messages as f64 / elapsed.as_secs_f64();

    println!("\nSummary:");
    println!("Test completed in {:.2?}", elapsed);
    println!("Total messages: {}", total_messages);
    println!("Throughput: {:.2} msg/sec", rate);
    println!(
        "Data rate: {:.2} MB/sec",
        (rate * args.message_size as f64) / (1024.0 * 1024.0)
    );

    // 关闭发布者
    time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
