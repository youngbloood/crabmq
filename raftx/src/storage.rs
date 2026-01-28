use parking_lot::RwLock;
use protobuf::{Message as _, SingularPtrField};
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::{GetEntriesContext, Result, StorageError, prelude::*, storage::Storage};
use sled::{Db, IVec, transaction::TransactionError};
use std::sync::Arc;
use std::time::Duration;
use std::{
    convert::TryInto,
    sync::atomic::{AtomicU64, Ordering},
};

const HARD_STATE_KEY: &[u8] = b"hard_state";
const CONF_STATE_KEY: &[u8] = b"conf_state";
const SNAPSHOT_KEY: &[u8] = b"snapshot";
const ENTRY_PREFIX: &[u8] = b"entry:";
const META_FIRST_INDEX: &[u8] = b"meta_first";
const META_LAST_INDEX: &[u8] = b"meta_last";

#[derive(Clone)]
pub struct DbConfig {
    pub id: u64,
    pub db_path: String,
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    db: Db,
    id: u64,
    snap_metadata: Arc<RwLock<SnapshotMetadata>>,
    meta_first: Arc<AtomicU64>,
    meta_last: Arc<AtomicU64>,
}

impl SledStorage {
    pub fn new(id: u64, db: Db) -> Self {
        // 1. 初始化时加载快照元数据
        let snap_meta = db
            .get(SNAPSHOT_KEY)
            .map(|data| data.map(|d| Snapshot::parse_from_bytes(&d)))
            .map(|s| {
                let ss = s.unwrap_or(Ok(Snapshot::default()));
                ss.unwrap_or_default().take_metadata()
            })
            .unwrap_or_else(|_| SnapshotMetadata::default());

        // 2. 加载索引元数据
        let (first, last) = match (
            db.get(META_FIRST_INDEX).transpose(),
            db.get(META_LAST_INDEX).transpose(),
        ) {
            (Some(Ok(f)), Some(Ok(l))) => (
                u64::from_be_bytes(f.as_ref().try_into().unwrap_or([0; 8])),
                u64::from_be_bytes(l.as_ref().try_into().unwrap_or([0; 8])),
            ),
            _ => (snap_meta.index + 1, snap_meta.index),
        };

        // 3. 验证HardState的commit是否合法
        if let Ok(Some(data)) = db.get(HARD_STATE_KEY) {
            if let Ok(mut hs) = HardState::parse_from_bytes(&data) {
                if hs.commit > last {
                    hs.commit = 0;
                    db.insert(HARD_STATE_KEY, hs.write_to_bytes().unwrap()).ok();
                }
            }
        }

        let ss = Self {
            db,
            id,
            snap_metadata: Arc::new(RwLock::new(snap_meta)),
            meta_first: Arc::new(AtomicU64::new(first)), // 初始化内存缓存
            meta_last: Arc::new(AtomicU64::new(last)),   // 初始化内存缓存
        };
        ss.clone().start_sync_task(Duration::from_millis(200));

        ss
    }

    // 内部方法：转换 sled 错误
    fn map_err(e: sled::Error) -> StorageError {
        StorageError::Other(Box::new(e))
    }

    // 内部方法：转换 sled 错误
    fn map_tx_err(e: TransactionError) -> StorageError {
        StorageError::Other(Box::new(e))
    }

    // 更新快照元数据缓存
    fn update_snap_metadata(&self, meta: SnapshotMetadata) {
        let mut writer = self.snap_metadata.write();
        *writer = meta;
    }

    // 获取快照元数据（带缓存）
    fn snap_metadata(&self) -> SnapshotMetadata {
        self.snap_metadata.read().clone()
    }

    fn entry_key(index: u64) -> IVec {
        let mut key = ENTRY_PREFIX.to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        IVec::from(key)
    }

    pub fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let meta = snapshot.get_metadata().clone();

        // 原子更新
        let mut batch = sled::Batch::default();
        batch.insert(SNAPSHOT_KEY, snapshot.get_data());

        let hs = self.hard_state()?;
        batch.insert(HARD_STATE_KEY, self.hard_state()?.write_to_bytes()?);
        batch.insert(
            CONF_STATE_KEY,
            meta.conf_state.as_ref().unwrap().write_to_bytes()?,
        );

        self.db
            .transaction(|tx| {
                tx.apply_batch(&batch)?;

                // 更新缓存
                let first = meta.index + 1;
                let last = meta.index;
                tx.insert(META_FIRST_INDEX, &first.to_be_bytes())?;
                tx.insert(META_LAST_INDEX, &last.to_be_bytes())?;

                Ok(())
            })
            .map_err(Self::map_tx_err)?;

        self.meta_first.store(meta.index + 1, Ordering::Release);
        self.meta_last.store(meta.index, Ordering::Release);
        self.update_snap_metadata(meta);
        Ok(())
    }

    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut batch = sled::Batch::default();
        let last_index = self.meta_last.load(Ordering::Acquire);

        // 1. 检查索引连续性
        if entries[0].index != last_index + 1 {
            return Err(StorageError::LogTemporarilyUnavailable.into());
        }

        // 2. 批量写入日志条目
        for entry in entries {
            batch.insert(Self::entry_key(entry.index), entry.write_to_bytes()?);
        }

        // 3. 原子更新last_index（内存+磁盘）
        let new_last = entries.last().unwrap().index;
        batch.insert(META_LAST_INDEX, &new_last.to_be_bytes());

        // 4. 提交批量操作
        self.db.apply_batch(batch).map_err(Self::map_err)?;

        // 5. 更新内存缓存
        self.meta_last.store(new_last, Ordering::Release);
        Ok(())
    }

    pub fn compact(&self, compact_index: u64) -> Result<()> {
        let first = self.meta_first.load(Ordering::Acquire);
        if compact_index <= first {
            return Ok(());
        }

        let mut batch = sled::Batch::default();

        // 1. 删除旧日志
        for idx in first..compact_index {
            batch.remove(Self::entry_key(idx));
        }

        // 2. 原子更新first_index（内存+磁盘）
        batch.insert(META_FIRST_INDEX, &compact_index.to_be_bytes());

        // 3. 提交批量操作
        self.db.apply_batch(batch).map_err(Self::map_err)?;

        // 4. 更新内存缓存
        self.meta_first.store(compact_index, Ordering::Release);
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState> {
        Ok(self
            .db
            .get(HARD_STATE_KEY)
            .map_err(Self::map_err)?
            .map(|data| HardState::parse_from_bytes(&data))
            .transpose()?
            .unwrap_or_default())
    }

    pub fn set_hard_state(&self, hs: &HardState) -> Result<()> {
        self.db
            .insert(HARD_STATE_KEY, hs.write_to_bytes().unwrap())
            .map_err(Self::map_err)?;
        Ok(())
    }

    pub fn set_hard_state_commit(&self, commit: u64) -> Result<()> {
        let mut hs = self.hard_state()?;
        hs.commit = commit;
        self.db
            .insert(HARD_STATE_KEY, hs.write_to_bytes().unwrap())
            .map_err(Self::map_err)?;
        Ok(())
    }

    fn conf_state(&self) -> Result<ConfState> {
        Ok(self
            .db
            .get(CONF_STATE_KEY)
            .map_err(Self::map_err)?
            .map(|data| ConfState::parse_from_bytes(&data))
            .transpose()?
            .unwrap_or(ConfState {
                voters: vec![self.id],
                ..Default::default()
            }))
    }

    pub fn set_conf_state(&self, cs: &ConfState) -> Result<()> {
        self.db
            .insert(CONF_STATE_KEY, cs.write_to_bytes().unwrap())
            .map_err(Self::map_err)?;
        Ok(())
    }

    // 将内存中的 first_index 持久化到磁盘
    fn sync_first_index(&self) -> Result<()> {
        let first = self.meta_first.load(Ordering::Relaxed);
        self.db
            .insert(META_FIRST_INDEX, &first.to_be_bytes())
            .map_err(Self::map_err)?;
        Ok(())
    }

    // 将内存中的 last_index 持久化到磁盘
    fn sync_last_index(&self) -> Result<()> {
        let last = self.meta_last.load(Ordering::Relaxed);
        self.db
            .insert(META_LAST_INDEX, &last.to_be_bytes())
            .map_err(Self::map_err)?;
        Ok(())
    }

    // 双写：更新内存并立即持久化
    fn set_first_index(&self, index: u64) -> Result<()> {
        self.meta_first.store(index, Ordering::Release);
        self.sync_first_index()
    }

    fn set_last_index(&self, index: u64) -> Result<()> {
        self.meta_last.store(index, Ordering::Release);
        self.sync_last_index()
    }

    pub fn start_sync_task(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let storage = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                if let Err(e) = storage.sync_all_indexes() {
                    log::error!("Failed to sync indexes: {}", e);
                }
            }
        })
    }

    fn sync_all_indexes(&self) -> Result<()> {
        self.sync_first_index()?;
        self.sync_last_index()?;
        Ok(())
    }
}

impl Storage for SledStorage {
    fn initial_state(&self) -> Result<RaftState> {
        // HardState：空值返回默认
        let hard_state = self.hard_state()?;
        // ConfState：空值返回包含自身的默认配置
        let conf_state = self.conf_state()?;

        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> Result<Vec<Entry>> {
        let first = self.meta_first.load(Ordering::Relaxed);
        let last = self.meta_last.load(Ordering::Relaxed);

        // 边界检查（遵循 MemStorage 的 panic 策略）
        if low < first {
            return Err(StorageError::Compacted.into());
        }
        if high > last + 1 {
            panic!(
                "entries' high({}) is out of bound lastindex({})",
                high, last
            );
        }

        let mut entries = Vec::new();
        let mut size_left = max_size.into().unwrap_or(u64::MAX);

        for idx in low..high {
            match self.db.get(Self::entry_key(idx)) {
                Ok(Some(data)) => {
                    let entry = Entry::parse_from_bytes(&data)?;
                    let entry_size = entry.compute_size() as u64;

                    if size_left >= entry_size || entries.is_empty() {
                        entries.push(entry);
                        size_left = size_left.saturating_sub(entry_size);
                    } else {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(Self::map_err(e).into()),
            }
        }

        Ok(entries)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        let snap_meta = self.snap_metadata();
        if idx == snap_meta.index {
            return Ok(snap_meta.term);
        }

        // 兼容 MemStorage 行为：索引不存在返回0
        Ok(self
            .db
            .get(Self::entry_key(idx))
            .map_err(Self::map_err)?
            .map(|data| Entry::parse_from_bytes(&data).map(|e| e.term))
            .transpose()?
            .unwrap_or(0))
    }

    fn first_index(&self) -> Result<u64> {
        Ok(self.meta_first.load(Ordering::Relaxed))
    }

    fn last_index(&self) -> Result<u64> {
        Ok(self.meta_last.load(Ordering::Relaxed))
    }

    fn snapshot(&self, request_index: u64, _: u64) -> Result<Snapshot> {
        let mut snapshot = self
            .db
            .get(SNAPSHOT_KEY)
            .map_err(Self::map_err)?
            .map(|data| Snapshot::parse_from_bytes(&data))
            .transpose()?
            .unwrap_or_else(|| {
                let mut s = Snapshot::default();
                s.mut_metadata().index = request_index;
                s
            });

        let value = snapshot.write_to_bytes()?;
        let meta = snapshot.mut_metadata();
        if meta.index < request_index {
            meta.index = request_index;
            meta.term = self.term(request_index).unwrap_or(0);
            meta.conf_state = SingularPtrField::some(self.initial_state()?.conf_state);

            self.db.insert(SNAPSHOT_KEY, value).map_err(Self::map_err)?;

            self.update_snap_metadata(meta.clone());
        }

        Ok(snapshot)
    }
}
