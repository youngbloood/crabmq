use anyhow::Result;
use bytes::Bytes;
use governor::{Quota, RateLimiter};
use nanoid::nanoid;
use std::{num::NonZeroU32, path::PathBuf, sync::Arc, time::Duration};
use storagev2::{
    MessagePayload, StorageWriter as _,
    disk::{DiskStorageWriter, DiskStorageWriterWrapper, default_config},
};
use tokio::sync::{Barrier, oneshot};

#[tokio::main]
async fn main() -> Result<()> {
    struct TestArgs {
        partition_count: u32,      // 分区数量
        message_size: usize,       // 单个消息大小
        warmup_duration: Duration, // 预热时长
        current_rate: usize,       // 起始速率 (MB/s)
        rate_step: usize,          // 速率步长 (MB/s)
        test_duration: Duration,   // 每个速率级别的测试时长
        max_rate_mbps: usize,      // 最大测试速率
    }

    // 测试时修改参数
    let args = vec![
        //
        // 1k message_size
        // TestArgs {
        //     partition_count: 10000,
        //     message_size: 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 1000,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 2500,
        // },
        // TestArgs {
        //     partition_count: 5000,
        //     message_size: 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 1000,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 2500,
        // },
        // TestArgs {
        //     partition_count: 2000,
        //     message_size: 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 600,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 1000,
        //     message_size: 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 600,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 500,
        //     message_size: 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 600,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // //
        // 10k message_size
        TestArgs {
            partition_count: 10000,
            message_size: 10 * 1024,
            warmup_duration: Duration::from_secs(20),
            current_rate: 450,
            rate_step: 150,
            test_duration: Duration::from_secs(20),
            max_rate_mbps: 1500,
        },
        TestArgs {
            partition_count: 5000,
            message_size: 10 * 1024,
            warmup_duration: Duration::from_secs(20),
            current_rate: 450,
            rate_step: 150,
            test_duration: Duration::from_secs(20),
            max_rate_mbps: 1500,
        },
        TestArgs {
            partition_count: 2000,
            message_size: 10 * 1024,
            warmup_duration: Duration::from_secs(15),
            current_rate: 450,
            rate_step: 150,
            test_duration: Duration::from_secs(15),
            max_rate_mbps: 1500,
        },
        TestArgs {
            partition_count: 1000,
            message_size: 10 * 1024,
            warmup_duration: Duration::from_secs(15),
            current_rate: 450,
            rate_step: 150,
            test_duration: Duration::from_secs(15),
            max_rate_mbps: 1500,
        },
        TestArgs {
            partition_count: 500,
            message_size: 10 * 1024,
            warmup_duration: Duration::from_secs(15),
            current_rate: 450,
            rate_step: 150,
            test_duration: Duration::from_secs(15),
            max_rate_mbps: 1500,
        },
        //
        // // 100k message_size
        // TestArgs {
        //     partition_count: 10000,
        //     message_size: 100 * 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 5000,
        //     message_size: 100 * 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 2000,
        //     message_size: 100 * 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 1000,
        //     message_size: 100 * 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        TestArgs {
            partition_count: 500,
            message_size: 100 * 1024,
            warmup_duration: Duration::from_secs(15),
            current_rate: 300,
            rate_step: 150,
            test_duration: Duration::from_secs(15),
            max_rate_mbps: 1500,
        },
        // //
        // // 1m message_size
        // TestArgs {
        //     partition_count: 10000,
        //     message_size: 1000 * 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 5000,
        //     message_size: 1000 * 1024,
        //     warmup_duration: Duration::from_secs(20),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(20),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 2000,
        //     message_size: 1000 * 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 1000,
        //     message_size: 1000 * 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
        // TestArgs {
        //     partition_count: 500,
        //     message_size: 1000 * 1024,
        //     warmup_duration: Duration::from_secs(15),
        //     current_rate: 300,
        //     rate_step: 150,
        //     test_duration: Duration::from_secs(15),
        //     max_rate_mbps: 1500,
        // },
    ];

    // 测试不同分区配置
    for arg in args {
        test_flush_speed_with_dynamic_rate_multi_partition(
            arg.partition_count,
            arg.message_size,
            arg.warmup_duration,
            arg.current_rate,
            arg.rate_step,
            arg.test_duration,
            arg.max_rate_mbps,
        )
        .await?;
    }
    Ok(())
}

async fn test_flush_speed_with_dynamic_rate_multi_partition(
    partition_count: u32,      // 分区数量
    message_size: usize,       // 单个消息大小
    warmup_duration: Duration, // 预热时长
    current_rate: usize,       // 起始速率 (MB/s)
    rate_step: usize,          // 速率步长 (MB/s)
    test_duration: Duration,   // 每个速率级别的测试时长
    max_rate_mbps: usize,      // 最大测试速率
) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::{Duration, Instant};

    // 准备测试环境
    let mut config = default_config();
    let storage_dir = "./data/flush_bench_data";
    config.storage_dir = PathBuf::from(storage_dir);
    config.fd_cache_size = 10000;
    config.partition_writer_buffer_size = 10000; // 大缓冲区防止阻塞
    config.with_metrics = true;

    let store = DiskStorageWriterWrapper::new(config)?;
    // 测试参数
    let topic = format!("flush_speed_test_{}", partition_count);

    println!(
        "起始速度: {current_rate} MB/s, 步长增速: {rate_step} MB/s, 单步持续时长: {}s, 最大速率: {max_rate_mbps} MB/s",
        test_duration.as_secs()
    );
    // 创建消息内容池
    let message_pool: Vec<Bytes> = (0..26)
        .map(|v| Bytes::from(vec![b'a' + v; message_size]))
        .collect();
    let message_pool = Arc::new(message_pool);

    // 预热阶段
    println!("[预热] 开始预热 {} 个分区...", partition_count);
    let warmup_start = Instant::now();
    let mut warmup_handles = vec![];

    for partition in 0..partition_count {
        let store = store.clone();
        let message_pool = message_pool.clone();
        let topic = topic.clone();
        warmup_handles.push(tokio::spawn(async move {
            while warmup_start.elapsed() < warmup_duration {
                let msg = message_pool[rand::random::<u32>() as usize % message_pool.len()].clone();
                let payload = MessagePayload {
                    msg_id: nanoid!(),
                    timestamp: chrono::Utc::now().timestamp_millis() as u64,
                    metadata: Default::default(),
                    payload: msg.to_vec(),
                };
                let (notify_tx, notify_rx) = oneshot::channel();
                if let Err(e) = store
                    .store(&topic, partition, vec![payload], Some(notify_tx))
                    .await
                {
                    eprintln!("store.store err: {e:?}");
                }
                if let Err(e) = notify_rx.await {
                    eprintln!("store err: {e:?}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    futures::future::join_all(warmup_handles).await;
    println!("[预热] 完成");

    // 获取初始刷盘指标 - 确保分区已初始化
    println!("进入压测阶段...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    // 速率测试控制
    let mut results = vec![];

    println!("\n{:-^72}", " 开始动态速率测试 (多分区) ");
    println!("|速率(MB/s)|测试时长(s)|发送量(MB)|落盘量(MB)|墙钟耗时(ms)|墙钟吞吐量(MB/s)|分区数|",);
    println!(
        "|{:-<10}|{:-<11}|{:-<10}|{:-<10}|{:-<12}|{:-<16}|{:-<6}|",
        "", "", "", "", "", "", "",
    );

    for target_rate in (current_rate..=max_rate_mbps).step_by(rate_step) {
        // println!("\n=== 测试速率: {} MB/s ===", target_rate);
        // 系统稳定期
        // println!("等待系统稳定 (5秒)...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // 重置指标
        store.reset_metrics().await?;

        // 准备测试
        let total_messages_per_second =
            (target_rate as f64 * 1024.0 * 1024.0 / message_size as f64).ceil() as u64;
        let total_messages = total_messages_per_second * test_duration.as_secs();
        let messages_per_partition = (total_messages as f64 / partition_count as f64).ceil() as u64;
        // 创建发送任务
        let store_clone = store.clone();
        let message_pool_clone = message_pool.clone();
        let sent_counter = Arc::new(AtomicU64::new(0));
        let mut send_handles = vec![];

        let barrier = Arc::new(Barrier::new(partition_count as usize + 1));
        // 循环分区写消息
        for partition in 0..partition_count {
            let store = store_clone.clone();
            let message_pool = message_pool_clone.clone();
            let sent_counter = sent_counter.clone();
            let topic = topic.clone();

            // 每个分区独立限流（总吞吐量 = 分区数 × 单分区速率）
            let partition_rate = total_messages_per_second as f64 / partition_count as f64;

            let rate_limiter = Arc::new(RateLimiter::direct(Quota::per_second(
                NonZeroU32::new(partition_rate.max(1.0).ceil() as u32).unwrap(),
            )));
            let barrier = barrier.clone();
            send_handles.push(tokio::spawn(async move {
                barrier.wait().await;
                // 每个分区有自己的发送间隔
                let start = Instant::now();
                let mut sent = 0;
                while sent < messages_per_partition && start.elapsed() < test_duration {
                    // 等待速率许可
                    rate_limiter.until_ready().await;
                    if sent >= messages_per_partition || start.elapsed() >= test_duration {
                        break;
                    }

                    let msg_idx = rand::random::<u32>() as usize % message_pool.len();
                    let msg = message_pool[msg_idx].clone();
                    let payload = MessagePayload {
                        msg_id: nanoid!(),
                        timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        metadata: Default::default(),
                        payload: msg.to_vec(),
                    };
                    if let Err(e) = store.store(&topic, partition, vec![payload], None).await {
                        eprintln!("store.store err: {e:?}");
                        break;
                    }
                    sent_counter.fetch_add(1, Ordering::Relaxed);
                    sent += 1;
                }
            }));
        }

        let start_time = Instant::now();
        store.reset_metrics().await?;
        barrier.wait().await;
        // 等待测试结束
        futures::future::join_all(send_handles).await;

        // 发送数据耗时
        let elapsed = start_time.elapsed();

        // 发送数据完毕后，直接开始统计
        let end_metrics = store.get_metrics();
        // 计算总指标 - 使用分区路径匹配确保正确
        let sent_count = sent_counter.load(Ordering::Relaxed);
        let sent_bytes = sent_count * message_size as u64;

        // 墙钟时间(wall-clock time)
        let min_start_timestamp = end_metrics.min_start_timestamp.load(Ordering::Relaxed);
        let max_end_timestamp = end_metrics.max_end_timestamp.load(Ordering::Relaxed);
        let total_flushed_bytes = end_metrics.flush_bytes.load(Ordering::Relaxed);
        let _total_flush_count = end_metrics.flush_count.load(Ordering::Relaxed);

        // 计算性能指标
        let actual_rate = (sent_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
        // NOTE: 单分区平均吞吐量 = 总落盘字节数 / 总的落盘耗时
        // NOTE: 如果公式：总吞吐量 = 单分区平均吞吐量 * 分区数 : 这种计算方式不准确。因为墙钟时间原因: 即1个分区刷盘耗时1s, 10个分区刷盘可能还是1s
        // let avg_partition_flush_throughput = (total_flushed_bytes as f64 / 1024.0 / 1024.0)
        //     / Duration::from_micros(total_flush_latency_us).as_secs_f64();
        // let avg_flush_latency = if total_flush_count > 0 {
        //     // 微秒 转 毫秒
        //     (total_flush_latency_us as f64 / total_flush_count as f64) / 1000.0
        // } else {
        //     0.0
        // };

        // 计算基于墙钟时间的吞吐量（该吞吐量才是真实的数据落盘吞吐量）
        // wall_clock_time_us: 墙钟耗时，单位：微秒
        let wall_clock_time_us = max_end_timestamp.saturating_sub(min_start_timestamp);
        let wall_clock_throughput = if wall_clock_time_us > 0 {
            (total_flushed_bytes as f64 / 1024.0 / 1024.0)
                / (wall_clock_time_us as f64 / 1_000_000.0)
        } else {
            0.0
        };

        // 存储结果
        results.push((target_rate, actual_rate, wall_clock_throughput));

        // 打印结果
        println!(
            "|{:>10.1}|{:>11.1}|{:>10.1}|{:>10.1}|{:>12.1}|{:>16.1}|{:>6}|",
            target_rate as f64,
            test_duration.as_secs_f64(),
            sent_bytes as f64 / 1024.0 / 1024.0,
            total_flushed_bytes as f64 / 1024.0 / 1024.0,
            wall_clock_time_us as f64 / 1_000.0,
            wall_clock_throughput,
            // avg_partition_flush_throughput,
            partition_count
        );
    }

    // 分析结果找到瓶颈点
    let mut max_wall_clock_throughput = 0.0;
    let mut optimal_rate = 0.0;

    for (target_rate, _, throughput) in &results {
        if *throughput > max_wall_clock_throughput {
            max_wall_clock_throughput = *throughput;
            optimal_rate = *target_rate as f64;
        }
    }

    println!("\n{:-^78}", " 测试结果摘要 ");
    println!("最大落盘吞吐量: {:.2} MB/s", max_wall_clock_throughput);
    println!("最佳发送速率: {:.2} MB/s", optimal_rate);
    println!("分区数量: {}", partition_count);
    println!("消息大小: {} KB", message_size / 1024);

    // 可视化结果
    println!("\n速率与吞吐量关系:");
    for (target_rate, _, wall_clock_throughput) in &results {
        let bar_len = (wall_clock_throughput / 50.0).round() as usize;
        let bar = "█".repeat(bar_len);
        println!(
            "{:>5} MB/s: {:.1} MB/s |{}",
            target_rate, wall_clock_throughput, bar
        );
    }

    println!("\n清除测试数据...");
    drop(store);
    // 清理测试数据
    tokio::fs::remove_dir_all(storage_dir).await?;

    Ok(())
}

#[test]
fn format_form() {
    println!("\n{:-^72}", " 开始动态速率测试 (多分区) ");
    println!("|速率(MB/s)|测试时长(s)|发送量(MB)|落盘量(MB)|墙钟耗时(ms)|墙钟吞吐量(MB/s)|分区数|",);
    println!(
        "|{:-<10}|{:-<11}|{:-<10}|{:-<10}|{:-<12}|{:-<16}|{:-<6}|",
        "", "", "", "", "", "", "",
    );
    println!(
        "|{:>10.1}|{:>11.1}|{:>10.1}|{:>10.1}|{:>12.1}|{:>16.1}|{:>6}|",
        50.0, 5.0, 250.0, 300.2, 53.1, 60.0, 1
    );
    println!("\n{:-^78}", " 测试结果摘要 ");
}
