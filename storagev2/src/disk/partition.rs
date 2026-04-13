use crate::{
    MessagePayload,
    disk::{
        config::DiskReadWriteMode,
        gen_index_filename, gen_record_filename,
        index::{IndexReaderHandle, IndexWriterHandle},
        is_index_filename, is_record_filename, parse_factor,
        record::{RecordReaderHandle, RecordWriterHandle},
        switch_queue::SwitchQueue,
    },
    err::StorageResult,
};
use anyhow::Result;
use chrono::Duration;
use dashmap::DashMap;
use std::{
    io::IoSlice,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::{self, Instant},
};
use tokio::{fs, sync::Mutex};

pub struct PartitionWriterHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    cursors: PartitionCursors,
    buffer: SwitchQueue<MessagePayload>,
    store: StorageWriterGroup,

    // 最后写入时间
    pub(crate) latest_write_timestamp: Instant,
}

impl PartitionWriterHandle {
    pub async fn new(
        dir: PathBuf,
        topic: String,
        partition_id: u32,
        mode: DiskReadWriteMode,
    ) -> Result<Self> {
        let store = StorageWriterGroup::new(dir.clone(), topic.clone(), partition_id, mode).await?;

        Ok(Self {
            dir,
            topic,
            partition_id,
            cursors: PartitionCursors::new(),
            buffer: SwitchQueue::new(1000), // TODO: 配置化
            store,
            latest_write_timestamp: Duration::new(0, 0).unwrap(),
        })
    }

    pub async fn write_batch(&self, messages: Vec<MessagePayload>) -> Result<u64> {
        let messages_len = messages.len() as u64;
        // 1. write 'messages' into buffer
        for msg in messages {
            self.buffer.push(msg);
        }

        let cursor: u64 = self.cursors.max_append_seq.load(Ordering::Relaxed);
        self.cursors
            .max_append_seq
            .fetch_add(messages_len, Ordering::Relaxed);

        // 2. update the latest_write_timestamp
        self.latest_write_timestamp
            .store(time::Instant::now().elapsed().as_secs(), Ordering::Relaxed);

        Ok(cursor)
    }

    pub async fn flush(&self, all: bool, fsync: bool) -> Result<()> {
        // batch pop data from buffer
        let batch = if all {
            self.buffer.pop_all()
        } else {
            self.buffer.pop_batch(100) // TODO: batch size 配置化
        };

        if batch.is_empty() {
            return Ok(());
        }

        self.store.flush(&batch, fsync).await?;
        Ok(())
    }

    pub fn search(&self, logic_seq: u64) -> Result<MessagePayload> {
        todo!()
        // if logic_seq > self.cursors.max_append_seq.load(Ordering::Relaxed) {
        //     return Err(anyhow::anyhow!("Logic seq {} is out of range", logic_seq));
        // }
        // if logic_seq <= self.cursors.max_visible_seq.load(Ordering::Relaxed) {
        //     // read from disk
        // }

        // // read from buffer

        // let segment_id = self.cursors.binary_search(logic_seq);

        // let offset = (logic_seq - segment_id) * 8;

        // let msg = self.store.record.read_message(offset)?;

        // Ok(msg)
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

struct PartitionCursors {
    // 当前 partition 的逻辑序列号，写入内存数据时递增
    max_append_seq: AtomicU64,
    // 当前可查看的最大逻辑序列号，flush 后更新
    max_visible_seq: AtomicU64,
    // 已持久化的 seqs
    visible_seqs: Vec<u64>,
}

impl PartitionCursors {
    pub fn new() -> Self {
        Self {
            max_append_seq: AtomicU64::new(0),
            max_visible_seq: AtomicU64::new(0),
            visible_seqs: vec![],
        }
    }

    fn rotate_visibility(&self, n: u64) {
        self.max_visible_seq.fetch_add(n, Ordering::Release);
    }

    fn binary_search(&self, logic_seq: u64) -> u64 {
        // 二分查找 visible_seqs，找到第一个大于 logic_seq 的位置
        match self.visible_seqs.binary_search(&logic_seq) {
            Ok(pos) => self.visible_seqs[pos], // 找到 exact match，返回下一个位置
            Err(pos) => self.visible_seqs[pos - 1], // 没有找到，返回第一个大于 logic_seq 的位置
        }
    }
}

#[derive(Clone)]
struct StorageWriterGroup {
    dir: PathBuf,
    record: RecordWriterHandle,
    index: IndexWriterHandle,
}

impl StorageWriterGroup {
    pub async fn new(
        dir: PathBuf,
        topic: String,
        partition_id: u32,
        mode: DiskReadWriteMode,
    ) -> Result<Self> {
        println!("dir = {:?}", dir);
        let record =
            RecordWriterHandle::new(dir.clone(), topic.clone(), partition_id, mode).await?;
        let index = IndexWriterHandle::new(dir.clone(), topic.clone(), partition_id, mode).await?;

        Ok(Self { dir, record, index })
    }

    pub async fn flush(&self, data: &[MessagePayload], fsync: bool) -> Result<()> {
        let data = data
            .iter()
            .map(|msg| IoSlice::new(&msg.payload))
            .collect::<Vec<_>>();
        let offsets = self.record.flush(&data, fsync).await?;
        self.index.flush(&offsets, fsync).await?;
        Ok(())
    }

    fn search(&self, logic_seq: u64) -> Result<MessagePayload> {
        todo!()
    }
}

#[derive(Clone)]
struct StorageReaderGroup {
    dir: PathBuf,
    record: RecordReaderHandle,
    index: IndexReaderHandle,
}

impl StorageReaderGroup {
    fn search(&self, offset: u64) -> Result<MessagePayload> {
        todo!()
    }
}

#[derive(Clone)]
pub struct PartitionReaderHandle {
    // 数据文件所在目录（包含 topic 和 partition 信息）
    dir: PathBuf,

    topic: String,
    partition_id: u32,

    // 已持久化的 seqs
    visible_seqs: Arc<Mutex<Vec<u64>>>,

    // segment_id -> StorageReaderGroup
    stores: Arc<DashMap<u64, StorageReaderGroup>>,
}

struct ConsumerCursor {
    consumer_id: u64,
    // 当前 consumer 的逻辑序列号，消费时递增
    logic_seq: AtomicU64,
}

impl PartitionReaderHandle {
    pub async fn new(
        dir: PathBuf,
        topic: String,
        partition_id: u32,
        mode: DiskReadWriteMode,
    ) -> Result<Self> {
        Ok(Self {
            dir,
            topic,
            partition_id,
            visible_seqs: Arc::default(),
            stores: Arc::new(DashMap::new()),
        })
    }

    pub async fn search(&self, logic_seq: u64) -> Result<MessagePayload> {
        let segment_id = {
            let mu = self.visible_seqs.lock().await;
            match mu.binary_search(&logic_seq) {
                Ok(pos) => mu[pos],
                Err(pos) => mu[pos - 1],
            }
        };

        let offset = (logic_seq - segment_id) * 8;

        // 1. 二分查找 visible_seqs，找到对应的 StorageGroup
        let store = self
            .stores
            .get(&segment_id)
            .ok_or_else(|| anyhow::anyhow!("Logic seq {} not found", logic_seq))?;

        // 2. 在 StorageGroup 中查找消息
        store.search(logic_seq - segment_id)
    }
}
