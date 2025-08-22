use crate::{
    MessagePayload, ReadPosition, SegmentOffset, StorageError, StorageReader, StorageReaderSession,
    StorageResult, StorageWriter,
};
use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{RwLock, oneshot};

struct PartitionQueue {
    // sema: Arc<Semaphore>,
    messages: Arc<RwLock<VecDeque<MessagePayload>>>,
    read_pos: Arc<AtomicUsize>, // 当前读取位置
}

impl PartitionQueue {
    fn gen_segment_offset(&self) -> SegmentOffset {
        SegmentOffset {
            segment_id: 0,
            offset: self.read_pos.load(Ordering::Relaxed) as _,
        }
    }
}

struct PartitionStorage {
    partitions: Arc<DashMap<u32, PartitionQueue>>,
}

#[derive(Clone, Default)]
pub struct MemStorage {
    topics: Arc<DashMap<String, PartitionStorage>>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }

    /// 获取下一个消息并移动读取指针
    async fn next(
        &self,
        topic: &str,
        partition_id: u32,
        _n: NonZero<u64>,
    ) -> StorageResult<(MessagePayload, u64, SegmentOffset)> {
        let topic_storage = self
            .topics
            .get(topic)
            .ok_or_else(|| StorageError::TopicNotFound(topic.to_string()))?;

        let partition_queue = topic_storage
            .partitions
            .get_mut(&partition_id)
            .ok_or_else(|| StorageError::PartitionNotFound(partition_id.to_string()))?;

        let msg = {
            let messages_rl = partition_queue.messages.read().await;
            if partition_queue.read_pos.load(Ordering::Relaxed) >= messages_rl.len() {
                return Err(StorageError::NoMoreMessages(format!(
                    "No more messages in {}/{}",
                    topic, partition_id
                )));
            }
            messages_rl[partition_queue.read_pos.load(Ordering::Relaxed)].clone()
        };
        partition_queue.read_pos.fetch_add(1, Ordering::Relaxed); // 移动读取指针
        let msg_len = msg
            .len()
            .map_err(|e| StorageError::SerializeError(e.to_string()))? as u64;
        Ok((msg, msg_len, partition_queue.gen_segment_offset()))
    }

    /// 提交消费并移除已处理的消息
    async fn commit(&self, topic: &str, partition: u32) -> StorageResult<()> {
        let topic_storage = self
            .topics
            .get(topic)
            .ok_or_else(|| StorageError::TopicNotFound(topic.to_string()))?;

        let partition_queue = topic_storage
            .partitions
            .get(&partition)
            .ok_or_else(|| StorageError::PartitionNotFound(partition.to_string()))?;

        {
            let mut messages_wl = partition_queue.messages.write().await;
            // 移除已提交的消息
            messages_wl.pop_front();
        }

        // 调整指针位置
        if partition_queue.read_pos.load(Ordering::Relaxed) > 0 {
            partition_queue.read_pos.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[async_trait]
impl StorageWriter for MemStorage {
    /// 存储消息到指定 topic 和 partition
    async fn store(
        &self,
        topic: &str,
        partition: u32,
        payloads: Vec<MessagePayload>,
        _notify: Option<oneshot::Sender<StorageResult<()>>>,
    ) -> StorageResult<()> {
        let topic_storage =
            self.topics
                .entry(topic.to_string())
                .or_insert_with(|| PartitionStorage {
                    partitions: Arc::new(DashMap::new()),
                });

        let partition_queue = topic_storage
            .partitions
            .entry(partition)
            .or_insert_with(|| PartitionQueue {
                // sema: Arc::new(Semaphore::new(0)),
                messages: Arc::new(RwLock::new(VecDeque::new())),
                read_pos: Arc::new(AtomicUsize::new(0)),
            });

        let mut wl = partition_queue.messages.write().await;
        for data in payloads {
            wl.push_back(data);
        }
        // partition_queue.sema.add_permits(1);
        Ok(())
    }
}

#[derive(Clone)]
pub struct MemStorageReader {
    storage: MemStorage,
    has_session: Arc<AtomicBool>,
}

impl MemStorageReader {
    fn new(storage: MemStorage) -> Self {
        Self {
            storage,
            has_session: Arc::default(),
        }
    }
}

/// 内存消息队列限制
/// 1. 只能有 1 个消费者
/// 2. 只能从头开始 read, commit
#[async_trait]
impl StorageReader for MemStorageReader {
    /// New a session with group_id, it will be return Err() when session has been created.
    async fn new_session(
        &self,
        _group_id: u32,
        _read_position: Vec<(String, ReadPosition)>,
    ) -> StorageResult<Box<dyn StorageReaderSession>> {
        if self
            .has_session
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return Ok(Box::new(MemStorageReaderSession::new(self.storage.clone())));
        }
        Err(StorageError::Unknown(
            "mem storage only open once session".to_string(),
        ))
    }

    /// Close a session by group_id.
    async fn close_session(&self, _group_id: u32) {
        let _ =
            self.has_session
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed);
    }
}

pub struct MemStorageReaderSession {
    storage: MemStorage,
}

impl MemStorageReaderSession {
    fn new(storage: MemStorage) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl StorageReaderSession for MemStorageReaderSession {
    async fn next(
        &self,
        topic: &str,
        partition_id: u32,
        n: NonZero<u64>,
    ) -> StorageResult<Vec<(MessagePayload, u64, SegmentOffset)>> {
        let mut list = vec![];
        for _ in 0..n.into() {
            list.push(self.storage.next(topic, partition_id, n).await?);
        }
        Ok(list)
    }

    /// Commit the message has been consumed, and the consume ptr should rorate the next ptr.
    async fn commit(
        &self,
        topic: &str,
        partition: u32,
        _offset: SegmentOffset,
    ) -> StorageResult<()> {
        self.storage.commit(topic, partition).await
    }
}
