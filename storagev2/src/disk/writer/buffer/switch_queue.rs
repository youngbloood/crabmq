use crossbeam::queue::{ArrayQueue, SegQueue};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/**
 * 双队列
 */
pub struct SwitchQueue<T> {
    switcher: Arc<AtomicBool>,

    queue_a: Arc<ArrayQueue<T>>,
    queue_b: Arc<ArrayQueue<T>>,
}

impl<T> SwitchQueue<T> {
    const ACQUIRE_ORDER: Ordering = Ordering::Acquire;
    const RELEASE_ORDER: Ordering = Ordering::Release;
    const RELAXED_ORDER: Ordering = Ordering::Relaxed;

    pub fn new(buf_length: usize) -> Self {
        let half = buf_length / 2;
        Self {
            switcher: Arc::new(AtomicBool::new(false)),
            queue_a: Arc::new(ArrayQueue::new(half)),
            queue_b: Arc::new(ArrayQueue::new(buf_length - half)),
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

        // 优先处理活跃队列，为 None 自动结束循环
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

            // 为 None 自动结束循环
            while let Some(item) = new_active.pop() {
                results.push(item);
                if results.len() >= batch_size {
                    break;
                }
            }
        }

        results
    }

    /**
     * 弹出所有元素，先清空当前活跃队列，再尝试切换并清空新活跃队列
     */
    pub fn pop_all(&self) -> Vec<T> {
        let mut results = Vec::new();
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, _inactive_queue) = if current {
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

    /**
     * 弹出当前活跃队列中的所有数据，并切换活跃队列
     */
    pub fn pop_active(&self) -> Vec<T> {
        let mut results = Vec::new();
        let current = self.switcher.load(Self::ACQUIRE_ORDER);

        let (active_queue, _inactive_queue) = if current {
            (&self.queue_b, &self.queue_a)
        } else {
            (&self.queue_a, &self.queue_b)
        };

        // 清空活跃队列
        while let Some(item) = active_queue.pop() {
            results.push(item);
        }

        // 切换活跃队列
        self.switcher
            .compare_exchange(current, !current, Self::RELEASE_ORDER, Self::RELAXED_ORDER);

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
