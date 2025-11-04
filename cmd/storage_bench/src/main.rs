use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use governor::{Quota, RateLimiter};
use nanoid::nanoid;
use std::{num::NonZeroU32, path::PathBuf, sync::Arc, time::Duration};
use storagev2::{
    MessagePayload, StorageWriter as _,
    disk::{DiskStorageWriterWrapper, default_config},
};
use tokio::sync::{Barrier, oneshot};

// 火焰图相关导入
use pprof::{ProfilerGuard, ProfilerGuardBuilder};
use std::fs;
use std::path::Path;

// 火焰图控制结构
struct FlamegraphController {
    profiler_guard: Option<ProfilerGuard<'static>>,
    current_sample_name: Option<String>,
}

impl FlamegraphController {
    fn new() -> Self {
        Self {
            profiler_guard: None,
            current_sample_name: None,
        }
    }

    fn start_recording(&mut self, output_dir: &str, partition_count: u32) -> Result<()> {
        if self.profiler_guard.is_some() {
            return Ok(());
        }

        // 根据分区数量调整采样频率
        let frequency = if partition_count >= 5000 {
            30 // 高分区数时使用适中频率
        } else if partition_count >= 2000 {
            50 // 中等分区数
        } else if partition_count >= 1000 {
            80 // 中等分区数
        } else {
            100 // 低分区数时可以使用更高频率
        };

        // 确保输出目录存在
        let output_path = Path::new(output_dir);
        if !output_path.exists() {
            fs::create_dir_all(output_path)?;
        }

        // 启动性能分析器
        match ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread", "vdso", "tokio", "async"])
            .build()
        {
            Ok(guard) => {
                self.profiler_guard = Some(guard);
            }
            Err(e) => {
                println!(
                    "[火焰图] 警告：无法启动性能分析器: {}. 继续运行但不生成火焰图。",
                    e
                );
            }
        }

        Ok(())
    }

    fn stop_recording(
        &mut self,
        output_dir: &str,
        partition_count: u32,
        message_size: usize,
        rate: Option<usize>, // 添加可选的速率参数
    ) -> Result<()> {
        if self.profiler_guard.is_none() {
            return Ok(());
        }

        // 获取性能数据
        let guard = self.profiler_guard.take().unwrap();
        let report = guard.report().build()?;

        // 生成文件名
        let filename = if let Some(rate_value) = rate {
            format!(
                "rate-test-{}-{}-{}MBps.svg",
                partition_count,
                message_size / 1024,
                rate_value
            )
        } else {
            // 使用当前采样名称或默认名称
            let sample_name = self.current_sample_name.as_deref().unwrap_or("final");
            format!(
                "{}-{}-{}-final.svg",
                sample_name,
                partition_count,
                message_size / 1024
            )
        };

        // 保存火焰图
        self.save_flamegraph(&report, output_dir, &filename)?;

        // 重置采样名称
        self.current_sample_name = None;

        Ok(())
    }

    fn save_flamegraph(
        &self,
        report: &pprof::Report,
        output_dir: &str,
        filename: &str,
    ) -> Result<()> {
        let output_path = Path::new(output_dir);
        if !output_path.exists() {
            fs::create_dir_all(output_path)?;
        }
        let output_file = output_path.join(filename);
        let file = fs::File::create(&output_file)?;
        report.flamegraph(file)?;
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// 消息大小列表，单位KB，用逗号分隔 (默认: 10)
    #[arg(short, long, value_delimiter = ',', default_value = "10")]
    message_sizes: Vec<u32>,

    /// 分区数量列表，用逗号分隔 (默认: 2000)
    #[arg(short, long, value_delimiter = ',', default_value = "2000")]
    partition_counts: Vec<u32>,

    /// 预热时长，单位秒 (默认: 15)
    #[arg(long, default_value = "15")]
    warmup_duration: u64,

    /// 起始速率，单位MB/s (默认: 450)
    #[arg(long, default_value = "450")]
    current_rate: usize,

    /// 速率步长，单位MB/s (默认: 150)
    #[arg(long, default_value = "150")]
    rate_step: usize,

    /// 每个速率级别的测试时长，单位秒 (默认: 20)
    #[arg(long, default_value = "20")]
    test_duration: u64,

    /// 最大测试速率，单位MB/s (默认: 1500)
    #[arg(long, default_value = "1500")]
    max_rate_mbps: usize,

    /// 数据存储路径 (默认: ./data/flush_bench_data)
    #[arg(short, long, default_value = "./data/flush_bench_data")]
    dir: String,

    /// 是否在单次测试完成后删除数据 (默认: true)
    #[arg(long, default_value = "true")]
    cleanup: bool,

    /// 是否启用火焰图生成 (默认: false)
    #[arg(long, default_value = "false")]
    enable_flamegraph: bool,

    /// 火焰图输出文件夹路径 (默认: ./flamegraphs)
    /// 注意：请指定一个文件夹路径，火焰图将保存为以下格式：
    /// - rate-test-{分区数}-{消息大小KB}-{速率}MBps.svg (每个速率级别的独立采样)
    /// 例如：rate-test-2000-10-450MBps.svg, rate-test-2000-10-600MBps.svg 等
    #[arg(long, default_value = "./flamegraphs")]
    flamegraph_output: String,
}

struct TestArgs {
    partition_count: u32,      // 分区数量
    message_size: usize,       // 单个消息大小
    warmup_duration: Duration, // 预热时长
    current_rate: usize,       // 起始速率 (MB/s)
    rate_step: usize,          // 速率步长 (MB/s)
    test_duration: Duration,   // 每个速率级别的测试时长
    max_rate_mbps: usize,      // 最大测试速率
    storage_dir: String,       // 存储路径
    cleanup: bool,             // 是否清理数据
    enable_flamegraph: bool,   // 是否启用火焰图
    flamegraph_output: String, // 火焰图输出路径
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // 验证参数
    validate_args(&args)?;

    // 生成测试配置
    let test_configs = generate_test_configs(&args);

    println!("生成的测试配置数量: {}", test_configs.len());
    println!("测试配置:");
    for (i, config) in test_configs.iter().enumerate() {
        println!(
            "  {}. 分区数: {}, 消息大小: {}KB",
            i + 1,
            config.partition_count,
            config.message_size / 1024
        );
    }
    println!();
    if args.enable_flamegraph {
        println!("[火焰图] 进入正式压测阶段将会为每个速率级别生成火焰图");
    }
    println!();

    // 测试不同分区配置
    for arg in test_configs {
        bench_flush_speed_with_dynamic_rate_multi_partition(
            arg.partition_count,
            arg.message_size,
            arg.warmup_duration,
            arg.current_rate,
            arg.rate_step,
            arg.test_duration,
            arg.max_rate_mbps,
            &arg.storage_dir,
            arg.cleanup,
            arg.enable_flamegraph,
            &arg.flamegraph_output,
        )
        .await?;
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    // 验证消息大小范围
    for &size in &args.message_sizes {
        if size < 1 || size > 2048 {
            return Err(anyhow::anyhow!(
                "消息大小必须在 1KB 到 2048KB (2MB) 之间，当前值: {}KB",
                size
            ));
        }
    }

    // 验证分区数量范围
    for &count in &args.partition_counts {
        if count < 500 || count > 50000 {
            return Err(anyhow::anyhow!(
                "分区数量必须在 500 到 50000 之间，当前值: {}",
                count
            ));
        }
    }

    // 验证其他参数
    if args.current_rate == 0 {
        return Err(anyhow::anyhow!("起始速率必须大于 0"));
    }

    if args.rate_step == 0 {
        return Err(anyhow::anyhow!("速率步长必须大于 0"));
    }

    if args.max_rate_mbps < args.current_rate {
        return Err(anyhow::anyhow!("最大速率必须大于等于起始速率"));
    }

    if args.warmup_duration == 0 {
        return Err(anyhow::anyhow!("预热时长必须大于 0"));
    }

    if args.test_duration == 0 {
        return Err(anyhow::anyhow!("测试时长必须大于 0"));
    }

    // 验证火焰图输出路径
    if args.enable_flamegraph {
        let output_path = Path::new(&args.flamegraph_output);
        if output_path.exists() && !output_path.is_dir() {
            return Err(anyhow::anyhow!(
                "火焰图输出路径必须是一个文件夹，当前路径已存在且不是文件夹: {}",
                args.flamegraph_output
            ));
        }

        // 如果目录不存在，尝试创建
        if !output_path.exists() {
            println!("[验证] 创建火焰图输出目录: {}", output_path.display());
            if let Err(e) = fs::create_dir_all(output_path) {
                return Err(anyhow::anyhow!(
                    "无法创建火焰图输出目录 {}: {}",
                    args.flamegraph_output,
                    e
                ));
            }
        }
    }

    Ok(())
}

fn generate_test_configs(args: &Args) -> Vec<TestArgs> {
    let mut configs = Vec::new();

    for &message_size_kb in &args.message_sizes {
        for &partition_count in &args.partition_counts {
            configs.push(TestArgs {
                partition_count,
                message_size: message_size_kb as usize * 1024, // 转换为字节
                warmup_duration: Duration::from_secs(args.warmup_duration),
                current_rate: args.current_rate,
                rate_step: args.rate_step,
                test_duration: Duration::from_secs(args.test_duration),
                max_rate_mbps: args.max_rate_mbps,
                storage_dir: args.dir.clone(),
                cleanup: args.cleanup,
                enable_flamegraph: args.enable_flamegraph,
                flamegraph_output: args.flamegraph_output.clone(),
            });
        }
    }

    configs
}

async fn bench_flush_speed_with_dynamic_rate_multi_partition(
    partition_count: u32,      // 分区数量
    message_size: usize,       // 单个消息大小
    warmup_duration: Duration, // 预热时长
    current_rate: usize,       // 起始速率 (MB/s)
    rate_step: usize,          // 速率步长 (MB/s)
    test_duration: Duration,   // 每个速率级别的测试时长
    max_rate_mbps: usize,      // 最大测试速率
    storage_dir: &str,         // 存储路径
    cleanup: bool,             // 是否清理数据
    enable_flamegraph: bool,   // 是否启用火焰图
    flamegraph_output: &str,   // 火焰图输出路径
) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::{Duration, Instant};

    // 准备测试环境
    let mut config = default_config();
    config.storage_dir = PathBuf::from(storage_dir);
    config.with_metrics = true;

    let store = DiskStorageWriterWrapper::new(config)?;
    // 测试参数
    let topic = format!("flush_speed_test_{}", partition_count);

    println!(
        "起始速度: {current_rate} MB/s, 步长增速: {rate_step} MB/s, 单步持续时长: {}s, 最大速率: {max_rate_mbps} MB/s, 消息大小: {message_size}b",
        test_duration.as_secs()
    );
    // 创建消息内容池
    let message_pool: Vec<Bytes> = (0..26)
        .map(|v| Bytes::from(vec![b'a' + v; message_size]))
        .collect();
    let message_pool = Arc::new(message_pool);

    // 预热阶段
    println!("[预热] 开始预热 {} 个分区...", partition_count);

    // 限制预热并发，每批 32 个分区
    const WARMUP_BATCH_SIZE: usize = 32;
    for partition_batch in (0..partition_count).step_by(WARMUP_BATCH_SIZE) {
        let mut batch_handles = vec![];
        let batch_end = (partition_batch + WARMUP_BATCH_SIZE as u32).min(partition_count as u32);
        println!("[预热] 开始预热分区 {}-{}", partition_batch, batch_end);

        for partition in partition_batch..batch_end {
            let store = store.clone();
            let message_pool = message_pool.clone();
            let topic = topic.clone();
            batch_handles.push(tokio::spawn(async move {
                // 每个分区只写入 10 条消息
                for _ in 0..10 {
                    let msg =
                        message_pool[rand::random::<u32>() as usize % message_pool.len()].clone();
                    let payload = MessagePayload {
                        msg_id: nanoid!(),
                        timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        metadata: Default::default(),
                        payload: msg.to_vec(),
                    };
                    let (notify_tx, notify_rx) = oneshot::channel();
                    if let Err(e) = store
                        .store(&topic, partition as u32, vec![payload], Some(notify_tx))
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

        // 等待当前批次完成
        futures::future::join_all(batch_handles).await;
        println!("[预热] 完成分区 {}-{}", partition_batch, batch_end);
    }

    println!("[预热] 完成");

    // 初始化火焰图控制器
    let mut flamegraph_controller = FlamegraphController::new();

    // 获取初始刷盘指标 - 确保分区已初始化
    println!("进入压测阶段...");
    // 等待5秒，确保预热阶段完成
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

        // 开始火焰图采样（精确覆盖压测阶段）
        if enable_flamegraph {
            flamegraph_controller.start_recording(&flamegraph_output, partition_count)?;
        }
        // 等待测试结束
        futures::future::join_all(send_handles).await;
        // 计算耗时
        let elapsed = start_time.elapsed();
        // 发送数据完毕后，直接开始统计
        let end_metrics = store.get_metrics();

        // 结束火焰图采样并保存
        if enable_flamegraph {
            flamegraph_controller.stop_recording(
                &flamegraph_output,
                partition_count,
                message_size,
                Some(target_rate), // 传入当前速率
            )?;
        }

        // 计算总指标 - 使用分区路径匹配确保正确
        let sent_count = sent_counter.load(Ordering::Relaxed);
        let sent_bytes = sent_count * message_size as u64;

        // 墙钟时间(wall-clock time)
        let min_start_timestamp = end_metrics.data_min_start_timestamp.load(Ordering::Relaxed);
        let max_end_timestamp = end_metrics.data_max_end_timestamp.load(Ordering::Relaxed);
        let total_flushed_bytes = end_metrics.data_flush_bytes.load(Ordering::Relaxed);
        let _total_flush_count = end_metrics.data_flush_count.load(Ordering::Relaxed);

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

    // 火焰图已在每个速率级别保存完成
    if enable_flamegraph {
        println!("[火焰图] 所有速率级别的火焰图已生成完成");
        println!("[火焰图] 生成的文件包括：");
        for rate in (current_rate..=max_rate_mbps).step_by(rate_step) {
            println!(
                "  - 速率 {}MB/s: rate-test-{}-{}-{}MBps.svg",
                rate,
                partition_count,
                message_size / 1024,
                rate
            );
        }
    }

    if cleanup {
        println!("\n清除测试数据...");
        drop(store);
        tokio::time::sleep(Duration::from_secs(10)).await;
        // 清理测试数据
        tokio::fs::remove_dir_all(storage_dir).await?;
    } else {
        println!("\n保留测试数据在: {}", storage_dir);
        drop(store);
    }

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
