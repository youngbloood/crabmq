use crossbeam::queue::{ArrayQueue, SegQueue};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use crate::{
    MessageSize,
    err::{ErrorCode, StorageError, StorageResult},
};

/**
 * 双队列
 */
#[derive(Clone)]
pub struct SwitchQueue<T: MessageSize> {
    switcher: Arc<AtomicBool>,

    queue_a: Arc<SegQueue<T>>,
    queue_b: Arc<SegQueue<T>>,

    size_limit: Arc<AtomicU64>,
}

impl<T: MessageSize> SwitchQueue<T> {
    const ACQUIRE_ORDER: Ordering = Ordering::Acquire;
    const RELEASE_ORDER: Ordering = Ordering::Release;
    const RELAXED_ORDER: Ordering = Ordering::Relaxed;

    pub fn new(s: u64) -> Self {
        Self {
            switcher: Arc::new(AtomicBool::new(false)),
            queue_a: Arc::new(SegQueue::new()),
            queue_b: Arc::new(SegQueue::new()),
            size_limit: Arc::new(AtomicU64::new(s)),
        }
    }

    #[inline(always)]
    pub fn push(&self, data: T) -> StorageResult<()> {
        let current = self.switcher.load(Self::RELAXED_ORDER);
        let queue = if current {
            &self.queue_b
        } else {
            &self.queue_a
        };

        let data_size = data.get_size() as u64;

        self.size_limit
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current < data_size {
                    None
                } else {
                    Some(current - data_size)
                }
            })
            .map_err(|e| StorageError::new(ErrorCode::PartitionSizeLimitExceeded))?;

        // 使用CAS循环原子地检查并扣除容量
        loop {
            let current_limit = self.size_limit.load(Ordering::Relaxed);
            if current_limit < data_size {
                return Err(StorageError::new(ErrorCode::PartitionSizeLimitExceeded));
            }

            // 原子地从 current_limit 扣除 data_size
            match self.size_limit.compare_exchange(
                current_limit,
                current_limit - data_size,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,     // 成功扣除
                Err(_) => continue, // 有竞争，重试
            }
        }

        queue.push(data);
        Ok(())
    }

    #[inline(always)]
    pub fn is_dirty(&self) -> bool {
        !self.queue_a.is_empty() || !self.queue_b.is_empty()
    }

    /**
     * 弹出一批元素，优先从当前活跃队列弹出，如果活跃队列空了且非活跃队列有数据，则切换并继续弹出
     */
    pub fn pop_batch(&self, batch_size: usize) -> Vec<T> {
        let mut results = Vec::with_capacity(batch_size);
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        let mut size = 0;
        // 优先处理活跃队列，为 None 自动结束循环
        while let Some(item) = active_queue.pop() {
            size += item.get_size() as u64;
            results.push(item);
            if results.len() >= batch_size {
                self.size_limit.fetch_add(size, Ordering::Relaxed);
                return results;
            }
        }

        // 活跃队列空时检查非活跃队列
        if !inactive_queue.is_empty() {
            // 原子切换队列
            self.switcher
                .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER)
                .ok(); // 不关心是否切换成功

            // 处理新活跃队列
            let new_active = if current {
                &self.queue_a
            } else {
                &self.queue_b
            };

            // 为 None 自动结束循环
            while let Some(item) = new_active.pop() {
                size += item.get_size() as u64;
                results.push(item);
                if results.len() >= batch_size {
                    break;
                }
            }
        }
        self.size_limit.fetch_add(size, Ordering::Relaxed);

        results
    }

    /**
     * 弹出所有元素，先清空当前活跃队列，再尝试切换并清空新活跃队列
     */
    pub fn pop_all(&self) -> Vec<T> {
        let mut results = Vec::new();
        let mut size = 0;
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, _inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 清空活跃队列
        while let Some(item) = active_queue.pop() {
            size += item.get_size() as u64;
            results.push(item);
        }

        // 尝试切换并处理新队列
        if self
            .switcher
            .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER)
            .is_ok()
        {
            let new_active = if current {
                &self.queue_a
            } else {
                &self.queue_b
            };
            while let Some(item) = new_active.pop() {
                size += item.get_size() as u64;
                results.push(item);
            }
        }
        self.size_limit.fetch_add(size, Ordering::Relaxed);

        results
    }

    /**
     * 弹出当前活跃队列中的所有数据，并切换活跃队列
     */
    pub fn pop_active(&self) -> Vec<T> {
        let mut results = Vec::new();
        let mut size = 0;
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, _inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 清空活跃队列
        while let Some(item) = active_queue.pop() {
            size += item.get_size() as u64;
            results.push(item);
        }

        // 切换活跃队列
        self.switcher
            .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER);
        self.size_limit.fetch_add(size, Ordering::Relaxed);

        results
    }
}

/**
 * 单队列
 */
pub struct SimpleQueue<T> {
    queue: Arc<SegQueue<T>>,
}

impl<T> SimpleQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(SegQueue::new()),
        }
    }

    pub fn push(&self, data: T) {
        self.queue.push(data);
    }

    pub fn pop_batch(&self, batch_size: usize) -> Vec<T> {
        let mut results = Vec::with_capacity(batch_size);
        while let Some(item) = self.queue.pop() {
            results.push(item);
            if results.len() >= batch_size {
                break;
            }
        }
        results
    }

    pub fn is_dirty(&self) -> bool {
        !self.queue.is_empty()
    }
}
