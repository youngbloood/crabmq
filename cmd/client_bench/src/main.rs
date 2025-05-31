use bytes::Bytes;
use futures::future::join_all;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::time;

#[derive(StructOpt, Debug)]
struct Args {
    #[structopt(long = "coo-addr", short = "c", default_value = "127.0.0.1:16001")]
    coo_addr: String,

    #[structopt(long = "num-producers", short = "p", default_value = "100")]
    num_producers: usize,

    #[structopt(long = "num-messages", short = "m", default_value = "10000")]
    num_messages: usize,

    #[structopt(long = "message-size", short = "s", default_value = "1024")]
    message_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    println!("Starting producer test with config: {:?}", args);

    // 创建客户端和发布者
    let cli = client::Client::new(vec![args.coo_addr.clone()]);
    let publisher = cli.publisher("mytopic".to_string());

    // 创建通道
    let (tx, rx) = mpsc::channel(10000);
    publisher.publish(
        rx,
        |res| async {
            if let Err(e) = res {
                eprintln!("Publish error: {:?}", e);
            }
        },
        true,
    );

    // 等待发布者初始化
    time::sleep(Duration::from_millis(500)).await;

    // 创建消息内容（一次性创建，避免重复分配）
    let message_content = Bytes::from(vec![b'x'; args.message_size]);

    // 创建生产者任务
    let start_time = time::Instant::now();
    let mut handles = Vec::with_capacity(args.num_producers);

    for producer_id in 0..args.num_producers {
        let tx_clone = tx.clone();
        let message_content = message_content.clone();

        handles.push(tokio::spawn(async move {
            for msg_id in 0..args.num_messages {
                // 创建带唯一标识的键
                // let key = format!("prod-{}-msg-{}", producer_id, msg_id);

                if let Err(e) = tx_clone
                    .send(("".to_string(), message_content.clone()))
                    .await
                {
                    eprintln!("Failed to send message: {:?}", e);
                    break;
                }

                // 添加少量延迟以模拟真实场景
                if msg_id % 100 == 0 {
                    time::sleep(Duration::from_micros(10)).await;
                }
            }
            println!("Producer {} finished", producer_id);
        }));
    }

    // 等待所有生产者完成
    join_all(handles).await;

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
    drop(tx);
    time::sleep(Duration::from_secs(10)).await;
    Ok(())
}
