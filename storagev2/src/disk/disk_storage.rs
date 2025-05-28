use super::DiskConfig;
use super::fd_cache::FdCacheAync;
use super::flusher::Flusher;
use super::meta::{META_NAME, PartitionMeta, PositionPtr, TopicMeta, gen_record_filename};
use super::writer::PartitionWriter;
use crate::Storage;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use common::check_exist;
use dashmap::DashMap;
use log::error;
use notify::{Config, FsEventWatcher, RecursiveMode};
use notify::{Event, Watcher};
use std::path::Path;
use std::result::Result as StdResult;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::select;
use tokio::time::{Interval, interval};
use tokio::{
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    sync::{RwLock, mpsc},
};
use tokio_util::sync::CancellationToken;

const READER_SESSION_INTERVAL: u64 = 400; // 单位：ms

#[derive(Clone)]
struct PartitionDetail {
    dir: PathBuf,
    writer: Arc<PartitionWriter>,
    meta: PartitionMeta,
}

impl PartitionDetail {
    pub async fn write(&self, data: Bytes) -> Result<()> {
        self.writer.write(data).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct TopicStorage {
    dir: PathBuf,
    partitions: Arc<DashMap<u32, PartitionDetail>>,
    meta: TopicMeta,
}

#[derive(Clone)]
pub struct DiskStorage {
    cfg: DiskConfig,
    topics: Arc<DashMap<String, TopicStorage>>,
    fd_cache: Arc<FdCacheAync>,
    flusher: Flusher,

    flush_sender: mpsc::Sender<()>,
}

impl DiskStorage {
    pub fn new(cfg: DiskConfig) -> Self {
        let fd_cache = Arc::new(FdCacheAync::new(cfg.fd_cache_size as usize));

        // 启动刷盘守护任务
        let flusher = Flusher::new(Duration::from_millis(cfg.flusher_period));
        let mut _flusher = flusher.clone();
        let (flush_sender, flusher_signal) = mpsc::channel(1);
        tokio::spawn(async move { _flusher.run(flusher_signal).await });

        Self {
            cfg,
            topics: Arc::new(DashMap::new()),
            fd_cache,
            flusher,
            flush_sender,
        }
    }

    /// get_topic_storage
    ///
    /// 先从内存 DashMap 中获取是否有该 topic
    ///
    /// 在检查本地存储中是否有 topic
    ///
    /// 都无，初始化 topic
    async fn get_topic_storage(&self, topic: &str) -> Result<TopicStorage> {
        if let Some(ts) = self.topics.get(topic) {
            return Ok(ts.clone());
        }

        // 创建新 topic 存储
        let dir = self.cfg.storage_dir.join(topic);
        tokio::fs::create_dir_all(&dir).await?;

        let meta_path = dir.join(META_NAME);
        let topic_meta = if meta_path.exists() {
            TopicMeta::load(&meta_path).await?
        } else {
            TopicMeta {
                keys: Arc::new(DashMap::new()),
            }
        };

        let ts = TopicStorage {
            dir: dir.clone(),
            partitions: Arc::new(DashMap::new()),
            meta: topic_meta,
        };
        self.flusher.add_topic_meta(meta_path, ts.meta.clone());
        self.topics.insert(topic.to_string(), ts.clone());
        Ok(ts)
    }

    async fn get_partition_writer(&self, topic: &str, partition: u32) -> Result<PartitionDetail> {
        let ts = self.get_topic_storage(topic).await?;

        if let Some(pd) = ts.partitions.get(&partition) {
            return Ok(pd.clone());
        }

        // 创建新 partition
        let dir = ts.dir.join(format!("{}", partition));
        tokio::fs::create_dir_all(&dir).await?;

        let meta_path = dir.join(META_NAME);
        let partition_meta = if meta_path.exists() {
            PartitionMeta::load(&meta_path).await?
        } else {
            PartitionMeta::new(dir.join(gen_record_filename(0)))
        };

        let writer = Arc::new(
            PartitionWriter::new(
                dir.clone(),
                &self.cfg,
                self.fd_cache.clone(),
                partition_meta.get_write_ptr(),
            )
            .await?,
        );
        self.flusher
            .add_partition_writer(dir.clone(), writer.clone());
        let pd = PartitionDetail {
            dir,
            writer,
            meta: partition_meta,
        };
        self.flusher.add_partition_meta(meta_path, pd.meta.clone());
        ts.partitions.insert(partition, pd.clone());
        Ok(pd)
    }
}

#[async_trait::async_trait]
impl Storage for DiskStorage {
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Err(anyhow!("can't store empty data"));
        }
        let pd = self.get_partition_writer(topic, partition).await?;
        pd.write(data).await?;
        Ok(())
    }

    async fn next(&self, topic: &str, partition: u32, stop: CancellationToken) -> Result<Bytes> {
        let pd: PartitionDetail = self.get_partition_writer(topic, partition).await?;
        let mut reader_session =
            PartitionReaderSession::new(pd.dir, self.fd_cache.clone(), pd.meta.get_read_ptr())?;
        reader_session.next(stop).await
    }

    async fn commit(&self, topic: &str, partition: u32) -> Result<()> {
        todo!("commit 一次 commit_ptr向后递推一次");
        let pd = self.get_partition_writer(topic, partition).await?;

        // // 提交指针跟进读指针
        // meta.commit_ptr = pd.meta.read_ptr.clone();

        // // 持久化元数据
        // let meta_path = pd.dir.join(META_NAME);
        // meta.save(&meta_path).await?;
        Ok(())
    }
}

struct PartitionReaderSession {
    dir: PathBuf,
    fd_cache: Arc<FdCacheAync>,
    reader_ptr: Arc<RwLock<PositionPtr>>,

    // 轮询
    ticker: Interval,
    // 文件变动监听
    watcher: FsEventWatcher,
    // 文件变动 接收器
    file_change_recver: mpsc::Receiver<StdResult<Event, notify::Error>>,
}

impl PartitionReaderSession {
    fn new(
        dir: PathBuf,
        reader: Arc<FdCacheAync>,
        reader_ptr: Arc<RwLock<PositionPtr>>,
    ) -> Result<Self> {
        let ticker = interval(Duration::from_millis(READER_SESSION_INTERVAL));

        let (tx, rx) = mpsc::channel(1);
        let watcher = FsEventWatcher::new(
            move |res: StdResult<Event, notify::Error>| {
                if let Ok(ref e) = res {
                    if !(e.paths.len() == 1 && e.paths[0].ends_with(META_NAME)) {
                        let _ = tx.blocking_send(res);
                    }
                }
            },
            Config::default().with_compare_contents(true),
        )?;

        Ok(Self {
            dir,
            fd_cache: reader,
            reader_ptr,
            ticker,
            file_change_recver: rx,
            watcher,
        })
    }

    async fn read(&self) -> Result<Bytes> {
        let (filename, read_offset) = {
            let reader_ptr_rl = self.reader_ptr.read().await;
            (reader_ptr_rl.filename.clone(), reader_ptr_rl.offset)
        };

        let file = self.fd_cache.get_or_create(&filename)?;

        let mut wl = file.reader().write().await;
        wl.seek(std::io::SeekFrom::Start(read_offset)).await?;

        // 读取消息长度前缀
        let mut len_buf = [0u8; 8];
        wl.read_exact(&mut len_buf).await?;
        let len = u64::from_le_bytes(len_buf);
        if len == 0 {
            return Ok(Bytes::new());
        }

        // 读取消息内容
        let mut buf = vec![0u8; len as usize];
        wl.read_exact(&mut buf).await?;

        // 更新读指针
        self.reader_ptr.write().await.offset += 8 + len;
        Ok(Bytes::from(buf))
    }

    async fn next(&mut self, stop: CancellationToken) -> Result<Bytes> {
        let mut has_monitor_files = false;
        loop {
            match self.read().await {
                Ok(data) => {
                    if !data.is_empty() {
                        return Ok(data);
                    }
                    // data为空，滚动文件查找
                    let filename = {
                        let reader_ptr_rl = self.reader_ptr.read().await;
                        reader_ptr_rl.filename.clone()
                    };
                    // 表示该文件读取完毕，寻找下一个
                    let next_filename = self.dir.join(filename_factor_next_record(&filename));
                    if check_exist(&next_filename) {
                        let mut read_ptr_wl = self.reader_ptr.write().await;
                        read_ptr_wl.filename = next_filename;
                        read_ptr_wl.offset = 0;
                        continue;
                    }
                    // 无下一个文件，需要开启监听
                    if !has_monitor_files {
                        if let Err(e) = self.watcher.watch(&self.dir, RecursiveMode::Recursive) {
                            error!(
                                "Start monitor dir[{:?}] change error: , self.dir{e:?}",
                                self.dir
                            );
                        }
                    }
                    has_monitor_files = true;
                    select! {
                        _ = stop.cancelled() => {
                            let _ = self.watcher.unwatch(&self.dir);
                            return Err(anyhow!("stopped"));
                        }

                        _ = self.file_change_recver.recv() => {
                            continue;
                        }

                        _ = self.ticker.tick() => {
                            continue;
                        }


                    }
                }
                Err(_) => todo!(),
            }
        }
    }
}

// struct PartitionDirWatcher {
//     tx: mpsc::Sender<()>,
// }
// impl PartitionDirWatcher {
//     fn new(tx: mpsc::Sender<()>) -> Self {
//         Self { tx }
//     }
// }

// impl EventHandler for PartitionDirWatcher {
//     fn handle_event(&mut self, _event: notify::Result<notify::Event>) {
//         let send = self.tx.clone();
//         println!("监听事件 = {_event:?}");
//         tokio::spawn(async move {
//             if let Err(e) = send.send(()).await {
//                 eprintln!("发送失败 : {e:?}");
//             }
//         });
//     }
// }

fn filename_factor_next_record(filename: &Path) -> PathBuf {
    let filename = PathBuf::from(filename.file_name().unwrap());
    println!("filename = {filename:?}");
    let filename_factor = filename
        .with_extension("")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    PathBuf::from(gen_record_filename(filename_factor + 1))
}

#[cfg(test)]
mod test {
    use super::{DiskConfig, DiskStorage};
    use crate::Storage;
    use anyhow::Result;
    use bytes::Bytes;
    use std::{path::PathBuf, time::Duration};
    use tokio::time;
    use tokio_util::sync::CancellationToken;

    fn new_disk_storage() -> DiskStorage {
        DiskStorage::new(DiskConfig {
            storage_dir: PathBuf::from("./data"),
            flusher_period: 50,
            flusher_factor: 0,
            max_msg_num_per_file: 4000,
            max_size_per_file: 500,
            compress_type: 0,
            fd_cache_size: 20,
            create_next_record_file_threshold: 80.0,
        })
    }

    #[tokio::test]
    async fn storage_store() -> Result<()> {
        let store = new_disk_storage();
        let datas = vec![
            "Apple",
            "Banana",
            "Cat",
            "Dog",
            "Elephant",
            "Fish",
            "Giraffe",
            "Horse",
            "Igloo",
            "Jaguar",
            "Kangaroo",
            "Lion",
            "Monkey",
            "Nest",
            "Ostrich",
            "Penguin",
            "Queen",
            "Rabbit",
            "Snake",
            "Tiger",
            "Umbrella",
            "Violin",
            "Whale",
            "Xylophone",
            "Yak",
            "Zebra",
        ];
        for d in datas {
            if let Err(e) = store
                .store("topic111", 11, Bytes::copy_from_slice(d.as_bytes()))
                .await
            {
                eprintln!("e = {e:?}");
            }
        }
        time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    #[tokio::test]
    async fn storage_next() -> Result<()> {
        let store = new_disk_storage();
        let stop = CancellationToken::new();
        let _stop = stop.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(5)).await;
            println!("stop cancelled");
            _stop.cancel();
        });

        loop {
            match store.next("topic111", 11, stop.child_token()).await {
                Ok(data) => println!("data = {data:?}"),
                Err(e) => {
                    eprintln!("e = {e:?}");
                    break;
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn storage_commit() -> Result<()> {
        let store = new_disk_storage();
        store.commit("topic111", 11).await?;
        Ok(())
    }

    // #[tokio::test]
    // async fn store_and_next() -> Result<()> {
    //     let store: DiskStorage = new_disk_storage();
    //     let _store = store.clone();
    //     let store_handle = tokio::spawn(async move {
    //         let datas = vec![
    //             "Apple",
    //             "Banana",
    //             "Cat",
    //             "Dog",
    //             "Elephant",
    //             "Fish",
    //             "Giraffe",
    //             "Horse",
    //             "Igloo",
    //             "Jaguar",
    //             "Kangaroo",
    //             "Lion",
    //             "Monkey",
    //             "Nest",
    //             "Ostrich",
    //             "Penguin",
    //             "Queen",
    //             "Rabbit",
    //             "Snake",
    //             "Tiger",
    //             "Umbrella",
    //             "Violin",
    //             "Whale",
    //             "Xylophone",
    //             "Yak",
    //             "Zebra",
    //         ];
    //         for d in datas {
    //             _store
    //                 .store("topic111", 11, Bytes::copy_from_slice(d.as_bytes()))
    //                 .await;
    //         }
    //     });

    //     let _store = store.clone();
    //     let next_handle = tokio::spawn(async move {
    //         let data = _store.next("topic111", 11).await.unwrap();
    //         println!("data = {:?}", data);
    //     });

    //     tokio::try_join!(store_handle, next_handle);
    //     Ok(())
    // }
}
