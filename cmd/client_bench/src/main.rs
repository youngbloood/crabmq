use bytes::Bytes;
use futures::future::join_all;
use grpcx::clientbrokersvc::{MessageReq, PublishAckType};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use structopt::StructOpt;
use tokio::{
    sync::{Barrier, Mutex},
    time,
};

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
    #[structopt(long = "message-size", short = "s", default_value = "10240")] // 10KB default
    message_size: usize,

    /// Topic name
    #[structopt(long = "topic", short = "t")]
    topic: String,

    /// Hot key ratio (0.0-1.0)
    #[structopt(long = "hot-ratio", default_value = "0.8")]
    hot_ratio: f64,

    /// Number of hot keys
    #[structopt(long = "hot-keys", default_value = "10")]
    hot_keys: usize,

    /// Warm-up messages per producer
    #[structopt(long = "warmup-count", default_value = "1000")]
    warmup_count: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("Starting producer test with config: {:?}", args);

    // 创建热点key集合
    let hot_keys: Vec<String> = (0..args.hot_keys)
        .map(|i| format!("hot_key_{}", i))
        .collect();

    // 创建共享统计对象
    let total_messages = Arc::new(AtomicU64::new(0));
    let latency_hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
    let error_count = Arc::new(AtomicU64::new(0));

    // 创建生产者任务
    let mut producers = vec![];
    let mut rngs = vec![];

    for producer_id in 0..args.num_producers {
        // 为每个生产者创建独立的随机数生成器
        let mut rng = StdRng::seed_from_u64(producer_id as u64);
        rngs.push(rng);

        // 创建客户端和发布者
        let cli = client::Client::new(producer_id.to_string(), vec![args.coo_addr.clone()]);
        let publisher = cli.publisher(args.topic.clone()).await;
        publisher.new_topic(0).await?;

        // 创建通道
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        publisher
            .publish(
                rx,
                {
                    let latency_hist = latency_hist.clone();
                    let error_count = error_count.clone();

                    move |res| {
                        let latency_hist = latency_hist.clone();
                        let error_count = error_count.clone();

                        async move {
                            match res {
                                Ok(res) => {
                                    let _ = res;
                                    // let micros = duration.as_micros() as u64;
                                    // latency_hist.lock().await.record(micros).unwrap();
                                }
                                Err(e) => {
                                    eprintln!("Publish error: {:?}", e);
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                },
                true,
            )
            .await?;

        producers.push((producer_id, tx, publisher, cli));
    }

    // ========================
    // 预热阶段
    // ========================
    println!("Starting warm-up phase...");
    let warmup_barrier = Arc::new(Barrier::new(args.num_producers + 1));
    let mut warmup_handles = Vec::with_capacity(args.num_producers);

    for (producer_id, tx, _, _) in &producers {
        let tx = tx.clone();
        let warmup_barrier = warmup_barrier.clone();
        let warmup_count = args.warmup_count;
        let message_size = args.message_size;
        let producer_id = *producer_id;
        warmup_handles.push(tokio::spawn(async move {
            warmup_barrier.wait().await;

            for i in 0..warmup_count {
                let key = if i < (warmup_count as f64 * 0.8) as usize {
                    // 80% 热点key
                    format!("hot_key_{}", i % 10)
                } else {
                    // 20% 普通key
                    format!("key_{}_{}", producer_id, i)
                };

                let content = vec![b'a' + (i % 26) as u8; message_size];
                if let Err(e) = tx
                    .send((
                        key,
                        PublishAckType::None,
                        vec![MessageReq {
                            message_id: nanoid::nanoid!(),
                            payload: content,
                            metadata: HashMap::new(),
                        }],
                    ))
                    .await
                {
                    eprintln!("Warm-up send error: {:?}", e);
                    break;
                }
            }
        }));
    }

    warmup_barrier.wait().await;
    join_all(warmup_handles).await;

    println!("Warm-up completed. Waiting for system to stabilize...");
    time::sleep(Duration::from_secs(5)).await;

    // ========================
    // 正式压测阶段
    // ========================
    println!("Starting main benchmark phase...");

    let barrier = Arc::new(Barrier::new(args.num_producers + 1));
    let mut handles = Vec::with_capacity(args.num_producers);

    for (i, (producer_id, tx, _, _)) in producers.into_iter().enumerate() {
        let barrier = barrier.clone();
        let total_messages = total_messages.clone();
        let message_size = args.message_size;
        let num_messages = args.num_messages;
        let hot_ratio = args.hot_ratio;
        let hot_keys = hot_keys.clone();

        // 使用预初始化的RNG
        let mut rng = rngs.remove(0);

        handles.push(tokio::spawn(async move {
            barrier.wait().await;

            for msg_id in 0..num_messages {
                // 热点key处理
                let key = if rng.random_bool(hot_ratio) {
                    // 从热点key中随机选择
                    hot_keys[rng.random_range(0..hot_keys.len())].clone()
                } else {
                    // 生成普通key
                    format!("key_{}_{}", producer_id, msg_id)
                };

                // 惰性生成消息内容
                let byte = b'a' + (msg_id % 26) as u8;
                let content = Bytes::from(vec![byte; message_size]);

                if let Err(e) = tx
                    .send((
                        key,
                        PublishAckType::None,
                        vec![MessageReq {
                            message_id: nanoid::nanoid!(),
                            payload: content.into(),
                            metadata: HashMap::new(),
                        }],
                    ))
                    .await
                {
                    eprintln!("Producer {} send error: {:?}", producer_id, e);
                    break;
                }

                total_messages.fetch_add(1, Ordering::Relaxed);
            }
            println!("Producer[{}] finished", producer_id);
        }));
    }
    let start_time = time::Instant::now();
    barrier.wait().await;
    join_all(handles).await;

    // ========================
    // 结果统计
    // ========================
    let elapsed = start_time.elapsed();
    let total_sent = total_messages.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let rate = total_sent as f64 / elapsed.as_secs_f64();

    let hist = latency_hist.lock().await;
    println!("\n{:=^80}", " BENCHMARK RESULTS ");
    println!(
        "Message size per request: {:.2} k/q",
        args.message_size as f64 / 1024.0
    );
    println!("Test completed in {:.2?}", elapsed);
    println!("Total messages sent: {}", total_sent);
    println!("Errors: {}", errors);
    println!("Throughput: {:.2} msg/sec", rate);
    println!(
        "Data rate: {:.2} MB/sec",
        (rate * args.message_size as f64) / (1024.0 * 1024.0)
    );

    println!("\n{:-^80}", " LATENCY DISTRIBUTION (us) ");
    println!(
        "| {:<15} | {:<15} | {:<15} | {:<15} |",
        "Percentile", "Value", "Count", "Total%"
    );
    println!("| {:-<15} | {:-<15} | {:-<15} | {:-<15} |", "", "", "", "");

    let percentiles = [50.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999];
    for p in percentiles {
        let value = hist.value_at_percentile(p);
        let count = hist.count_between(0, value);
        let total_pct = (count as f64 / hist.len() as f64) * 100.0;

        println!(
            "| {:<15} | {:<15} | {:<15} | {:<14.2}% |",
            format!("P{:.3}", p),
            value,
            count,
            total_pct
        );
    }

    println!("\n{:-^80}", " HOT KEY DISTRIBUTION ");
    println!(
        "Hot keys: {} keys handled {:.1}% of traffic",
        args.hot_keys,
        args.hot_ratio * 100.0
    );

    Ok(())
}
