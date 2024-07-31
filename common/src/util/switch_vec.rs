use crossbeam::queue::{ArrayQueue, SegQueue};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use tracing::debug;

#[derive(Debug)]
struct UnboundSyncVec<T>
where
    T: Clone + Sync + Send,
{
    array: ArrayQueue<T>,
    seg: SegQueue<T>,
}

impl<T> UnboundSyncVec<T>
where
    T: Clone + Sync + Send,
{
    fn with_capacity(cap: usize) -> Self {
        UnboundSyncVec {
            array: ArrayQueue::new(cap),
            seg: SegQueue::new(),
        }
    }

    fn push(&self, t: T) {
        if self.array.push(t.clone()).is_err() {
            self.seg.push(t);
        }
    }

    fn pop(&self) -> Option<T> {
        if !self.array.is_empty() {
            return self.array.pop();
        }
        self.seg.pop()
    }

    fn pop_all(&self) -> Vec<T> {
        let mut res = vec![];
        while let Some(v) = self.array.pop() {
            res.push(v);
        }
        while let Some(v) = self.seg.pop() {
            res.push(v);
        }
        res
    }
}

#[derive(Debug)]
pub struct SwitcherVec<T>
where
    T: Clone + Sync + Send,
{
    switcher: AtomicBool,

    t1: UnboundSyncVec<T>,
    t2: UnboundSyncVec<T>,
}

impl<T> Default for SwitcherVec<T>
where
    T: Clone + Sync + Send,
{
    fn default() -> Self {
        Self {
            switcher: Default::default(),
            t1: UnboundSyncVec::with_capacity(10000),
            t2: UnboundSyncVec::with_capacity(10000),
        }
    }
}

impl<T> SwitcherVec<T>
where
    T: Clone + Sync + Send,
{
    pub fn with_capacity(cap: usize) -> Self {
        SwitcherVec {
            switcher: AtomicBool::new(false),
            t1: UnboundSyncVec::with_capacity(cap),
            t2: UnboundSyncVec::with_capacity(cap),
        }
    }

    pub fn push(&self, v: T) {
        if !self.switcher.load(Relaxed) {
            debug!("push into t1");
            self.t1.push(v);
        } else {
            debug!("push into t2");
            self.t2.push(v);
        }
    }

    pub fn pop(&self) -> Vec<T> {
        if self
            .switcher
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok()
        {
            return self.t1.pop_all();
        }

        if self
            .switcher
            .compare_exchange(true, false, Relaxed, Relaxed)
            .is_ok()
        {
            return self.t2.pop_all();
        }

        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::SwitcherVec;
    use crate::util::interval;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn test_switcher_push_and_pop() {
        let switcher = SwitcherVec::with_capacity(5);
        switcher.push(1);
        switcher.push(2);
        switcher.push(3);
        println!("pop1 = {:?}", switcher.pop());
        println!("pop2 = {:?}", switcher.pop());
        println!("pop3 = {:?}", switcher.pop());
        // println!("pop4 = {:?}", switcher.pop());

        switcher.push(4);
        switcher.push(5);
        switcher.push(6);
        println!("pop5 = {:?}", switcher.pop());

        switcher.push(7);
        switcher.push(8);
        switcher.push(9);
        println!("pop6 = {:?}", switcher.pop());
    }

    #[tokio::test]
    async fn test_switcher_push_and_pop2() {
        let switcher = Arc::new(SwitcherVec::with_capacity(5));
        let arc_switcher = switcher.clone();
        tokio::spawn(async move {
            for i in 1..10000 {
                arc_switcher.push(i);
            }
        });

        let mut ticker = interval(Duration::from_millis(1)).await;
        let mut null_num = 0;
        let mut times = 1;
        while null_num <= 2 {
            ticker.tick().await;
            let list = switcher.pop();
            if list.is_empty() {
                null_num += 1;
            }
            for v in list {
                assert_eq!(times, v);
                times += 1;
            }
        }
    }
}
