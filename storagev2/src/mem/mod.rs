use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::Storage;

#[derive(Clone, Default)]
pub struct MemStorage {
    topics: Arc<DashMap<String, PartitionStorage>>,
}

struct PartitionStorage {
    partitions: Arc<DashMap<u32, PartitionQueue>>,
}

struct PartitionQueue {
    messages: Arc<Mutex<VecDeque<Bytes>>>,
    read_pos: Arc<AtomicUsize>, // 当前读取位置
}

#[async_trait]
impl Storage for MemStorage {
    /// 存储消息到指定 topic 和 partition
    async fn store(&self, topic: &str, partition: u32, data: Bytes) -> Result<()> {
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
                messages: Arc::new(Mutex::new(VecDeque::new())),
                read_pos: Arc::new(AtomicUsize::new(0)),
            });

        partition_queue.messages.lock().unwrap().push_back(data);
        Ok(())
    }

    /// 获取下一个消息并移动读取指针
    async fn next(&self, topic: &str, partition: u32) -> Result<Bytes> {
        let topic_storage = self
            .topics
            .get(topic)
            .ok_or_else(|| anyhow!("Topic {} not found", topic))?;

        let partition_queue = topic_storage
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| anyhow!("Partition {} not found", partition))?;

        let msg = {
            let messages = partition_queue.messages.lock().unwrap();
            if partition_queue.read_pos.load(Ordering::Relaxed) >= messages.len() {
                return Err(anyhow!("No more messages in {}/{}", topic, partition));
            }
            messages[partition_queue.read_pos.load(Ordering::Relaxed)].clone()
        };
        partition_queue.read_pos.fetch_add(1, Ordering::Relaxed); // 移动读取指针

        Ok(msg)
    }

    /// 提交消费并移除已处理的消息
    async fn commit(&self, topic: &str, partition: u32) -> Result<()> {
        let topic_storage = self
            .topics
            .get(topic)
            .ok_or_else(|| anyhow!("Topic {} not found", topic))?;

        let partition_queue = topic_storage
            .partitions
            .get(&partition)
            .ok_or_else(|| anyhow!("Partition {} not found", partition))?;

        {
            let mut messages = partition_queue.messages.lock().unwrap();
            // 移除已提交的消息
            messages.pop_front();
        }

        // 调整指针位置
        if partition_queue.read_pos.load(Ordering::Relaxed) > 0 {
            partition_queue.read_pos.fetch_sub(1, Ordering::Relaxed);
        }

        Ok(())
    }
}
