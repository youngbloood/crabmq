use anyhow::{anyhow, Result};
use common::global::CANCEL_TOKEN;
use common::{global::Guard, util::interval};
use dynamic_queue::{FlowControl, Queue};
use parking_lot::RwLock;
use protocol::message::{Message, MessageOperation};
use std::sync::atomic::Ordering::Relaxed;
use std::{sync::atomic::AtomicUsize, time::Duration};
use tokio::select;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::CacheOperation;

pub struct MessageCache {
    sender: UnboundedSender<Message>,
    recver: Guard<UnboundedReceiver<Message>>,
}

impl MessageCache {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        MessageCache {
            sender: tx,
            recver: Guard::new(rx),
        }
    }
}

#[async_trait::async_trait]
impl Queue for MessageCache {
    type Item = Message;

    async fn push(&self, msg: Message) -> Result<()> {
        self.sender.send(msg)?;
        Ok(())
    }

    async fn pop(&self) -> Option<Message> {
        self.recver.get_mut().recv().await
    }

    fn resize(&self, _: usize) {}
}

pub struct MessageCacheSlidingWindows {
    /// 流量控制
    ctrl: FlowControl,

    /// 滑动窗口大小
    slide_win: AtomicUsize,

    /// 读取msgs的pos指针
    read_ptr: AtomicUsize,

    /// msgs消息列表
    msgs: RwLock<Vec<MessageWrapper>>,
}

type MessageWrapper = Guard<Message>;

impl MessageCacheSlidingWindows {
    pub fn new(cap: usize, slide_win: usize) -> Self {
        let mut _slide_win = slide_win;
        if _slide_win > cap {
            _slide_win = cap;
        }
        MessageCacheSlidingWindows {
            ctrl: FlowControl::new(cap),
            slide_win: AtomicUsize::new(_slide_win),
            read_ptr: AtomicUsize::default(),
            msgs: RwLock::default(),
        }
    }

    fn pop_really(&self) -> Option<MessageWrapper> {
        let mut wd = self.msgs.write();
        wd.pop()
    }

    fn get(&self, id: &str) -> Option<(usize, MessageWrapper)> {
        for (index, v) in self.msgs.read().iter().enumerate() {
            if v.get().get_id() == id {
                return Some((index, v.clone()));
            }
        }

        None
    }
}

impl CacheOperation for MessageCacheSlidingWindows {
    #[doc = " push a message into cache. it will be block if the cache is filled."]
    async fn try_push(&self, msg: Message) -> Result<()> {
        if !self.ctrl.has_permit() {
            return Err(anyhow!("not has permit to push msg"));
        }
        self.push(msg).await
    }

    #[doc = " push a message into cache. it will be block if the cache is filled."]
    async fn push(&self, msg: Message) -> Result<()> {
        self.ctrl.grant(1).await;
        let mut wd = self.msgs.write();
        wd.push(Guard::new(msg));
        Ok(())
    }

    async fn seek(&self, block: bool) -> Option<Message> {
        let mut ticker = interval(Duration::from_millis(300)).await;

        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return None
                }

                msg = async {
                    let read_ptr = self.read_ptr.load(Relaxed);
                    let rd = self.msgs.read();
                    if read_ptr >= rd.len() {
                        return None;
                    }
                    Some(rd.get(read_ptr).unwrap().get().clone())
                } => {
                    match msg {
                        Some(msg) => {
                            return Some(msg);
                        }
                        None => {
                            if block {
                                ticker.tick().await;
                            }else{
                                return None;
                            }
                        }
                    }
                }
            }
        }
    }

    #[doc = " pop a message from cache. if it is defer message, cache should control it pop when it\'s expired. or pop the None"]
    async fn pop(&self, block: bool) -> Option<Message> {
        let mut ticker = interval(Duration::from_millis(300)).await;
        loop {
            select! {
                _ = CANCEL_TOKEN.cancelled() => {
                    return None
                }

                msg = async {
                    if self.read_ptr.load(Relaxed) >= self.slide_win.load(Relaxed) {
                        return None;
                    }
                    if let Some(msg_wrapper) = self.msgs.read().get(self.read_ptr.load(Relaxed)) {
                        // rorate the read_ptr
                        self.read_ptr.fetch_add(1, Relaxed);
                        return Some(msg_wrapper.clone());
                    }

                    None
                } => {
                    match msg {
                        Some(msg) => {
                            return Some(msg.get().clone());
                        }
                        None => {
                            if block {
                                select!{
                                    _ = CANCEL_TOKEN.cancelled() => {
                                        return None;
                                    }

                                    _ = ticker.tick() => {
                                        continue;
                                    }
                                }
                            }
                            return None;
                        }
                    }
                }
            }
        }
    }

    #[doc = " resize the buffer length in cache."]
    async fn resize(&self, cap: usize, slide_win: usize) {
        self.ctrl.resize(cap);
        let mut _slide_win = slide_win;
        if _slide_win > cap {
            _slide_win = cap;
        }
        self.slide_win
            .store(_slide_win, std::sync::atomic::Ordering::Relaxed);
        self.slide_win.store(_slide_win, Relaxed);
    }

    async fn update_consume(&self, id: &str, consume: bool) -> Result<()> {
        if let Some((index, msg)) = self.get(id) {
            msg.get_mut().update_consume(consume)?;
            if index == 0 && consume && self.pop_really().is_some() {
                self.ctrl.revert(1).await;
                // rorate the read_ptr
                // TODO: 测试fetch_sub可以减至负数吗
                self.read_ptr.fetch_sub(1, Relaxed);
                // return Some(msg_wrapper.msg);
            }
        }
        Ok(())
    }

    /// consume a message by id.
    async fn update_notready(&self, id: &str, notready: bool) -> Result<()> {
        if let Some((_, msg)) = self.get(id) {
            msg.get_mut().update_notready(notready)?;
        }
        Ok(())
    }

    /// consume a message by id.
    async fn update_delete(&self, id: &str, delete: bool) -> Result<()> {
        if let Some((_, msg)) = self.get(id) {
            msg.get_mut().update_delete(delete)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use protocol::message::Message;

    use crate::cache::CacheOperation;

    use super::MessageCacheSlidingWindows;

    fn slide_window_new() -> MessageCacheSlidingWindows {
        MessageCacheSlidingWindows::new(100, 100)
    }

    #[tokio::test]
    async fn slide_window_push() {
        let sw = slide_window_new();
        for _ in 0..100 {
            let msg = Message::default();
            assert!(sw.push(msg).await.is_ok());
        }
    }

    #[tokio::test]
    async fn slide_window_try_push() {
        let sw = slide_window_new();

        let msg = Message::default();
        for _ in 0..100 {
            assert!(sw.try_push(msg.clone()).await.is_ok());
        }
        for _ in 0..100 {
            assert!(sw.try_push(msg.clone()).await.is_err());
        }
    }

    #[tokio::test]
    async fn slide_window_pop() {
        let sw = slide_window_new();

        let msg = Message::default();
        for _ in 0..100 {
            assert!(sw.try_push(msg.clone()).await.is_ok());
        }
        for _ in 0..100 {
            assert!(sw.pop(false).await.is_some());
        }
    }

    #[tokio::test]
    async fn slide_window_try_push_and_pop() {
        let sw = slide_window_new();

        let msg = Message::default();
        for _ in 0..100 {
            assert!(sw.try_push(msg.clone()).await.is_ok());
        }
        for _ in 0..100 {
            assert!(sw.pop(false).await.is_some());
        }
        for _ in 0..100 {
            assert!(sw.try_push(msg.clone()).await.is_err());
        }
    }

    #[tokio::test]
    async fn slide_window_try_push_and_consume_and_pop() {
        let sw = slide_window_new();

        for i in 0..100 {
            let msg = Message::default();
            assert!(sw.try_push(msg.clone()).await.is_ok());
        }
        for _ in 0..100 {
            assert!(sw.pop(false).await.is_some());
        }
        // consume非队头消息
        assert!(sw.update_consume("2", true).await.is_ok());
        // consume非队头消息，push结果err
        for i in 0..100 {
            let msg = Message::default();
            assert!(sw.try_push(msg.clone()).await.is_err());
        }

        // consume
        assert!(sw.update_consume("0", true).await.is_ok());
        // 消费掉一个，再次push结果ok
        let msg = Message::default();
        assert!(sw.try_push(msg.clone()).await.is_ok());

        // 已经push一个，再次push结果err
        assert!(sw.try_push(msg.clone()).await.is_err());
    }
}
