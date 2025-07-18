use crossbeam::queue::SegQueue;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/**
 * 双队列
 */
pub struct SwitchQueue<T> {
    switcher: Arc<AtomicBool>,

    queue_a: Arc<SegQueue<T>>,
    queue_b: Arc<SegQueue<T>>,
}

impl<T> SwitchQueue<T> {
    const ACQUIRE_ORDER: Ordering = Ordering::Acquire;
    const RELEASE_ORDER: Ordering = Ordering::Release;
    const RELAXED_ORDER: Ordering = Ordering::Relaxed;

    pub fn new() -> Self {
        Self {
            switcher: Arc::new(AtomicBool::new(false)),
            queue_a: Arc::new(SegQueue::new()),
            queue_b: Arc::new(SegQueue::new()),
        }
    }

    #[inline(always)]
    pub fn push(&self, data: T) {
        let current = self.switcher.load(Self::RELAXED_ORDER);
        let queue = if current {
            &self.queue_b
        } else {
            &self.queue_a
        };
        queue.push(data);
    }

    #[inline(always)]
    pub fn is_dirty(&self) -> bool {
        !self.queue_a.is_empty() || !self.queue_b.is_empty()
    }

    // 高性能批量弹出
    pub fn pop_batch(&self, batch_size: usize) -> Vec<T> {
        let mut results = Vec::with_capacity(batch_size);
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 优先处理活跃队列
        while let Some(item) = active_queue.pop() {
            results.push(item);
            if results.len() >= batch_size {
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
            while let Some(item) = new_active.pop() {
                results.push(item);
                if results.len() >= batch_size {
                    break;
                }
            }
        }

        results
    }

    // 弹出全部元素
    pub fn pop_all(&self) -> Vec<T> {
        let mut results = Vec::new();
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 清空活跃队列
        while let Some(item) = active_queue.pop() {
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
                results.push(item);
            }
        }

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
