# CrabMQ Raft 重构方案与 Bug 修复

## 📋 目录

1. [问题分析](#问题分析)
2. [当前实现的严重 Bug](#当前实现的严重-bug)
3. [Raft-rs Ready 处理的正确流程](#raft-rs-ready-处理的正确流程)
4. [重构方案](#重构方案)
5. [代码对比](#代码对比)
6. [测试验证](#测试验证)

---

## 问题分析

### ✅ 你的两个问题都是正确的！

#### 问题 1: persisted_messages 处理顺序错误

**你的理解：**
> 对于 `ready.take_persisted_messages()` 的处理，是不是应该先调用本地存储持久化到本地之后（如 SledStorage），才根据 msg.to 分发至其他各个节点？

**答案：✅ 完全正确！**

**官方文档说明：**
> Persisted messages are outbound messages that can't be sent until **HardState, Entries, and Snapshot are persisted to stable storage**.

**当前 crabmq 的 Bug：**

```rust
// node.rs:424-442 (当前的错误实现)
async fn handle_all_ready(&self) {
    // 1. handle messages
    self.handle_messages(ready.take_messages()).await;
    // 2. handle snapshot
    self.handle_snapshot(ready.snapshot(), &store).await;
    // 3. handle committed entries
    self.handle_entries(ready.take_committed_entries(), &store).await;
    // 4. handle entries
    if let Err(e) = store.append(&ready.take_entries()) {  // ← 持久化在这里
        error!("persist raft log fail: {:?}, need to retry or panic", e);
        return;
    }
    // 5. handle HardState
    if let Some(hs) = ready.hs() {
        let _ = store.set_hard_state(hs);  // ← 持久化在这里
    }
    // 6. handle persisted messages
    self.handle_messages(ready.take_persisted_messages()).await;  // ❌ Bug!
}
```

**Bug 详情：**
- `handle_messages()` 是**异步函数** (`async fn`)
- 第 6 步调用时，第 4、5 步的持久化可能**还没完成**
- `store.append()` 和 `store.set_hard_state()` 虽然是同步调用，但如果底层存储（Sled）使用异步写入，数据可能还在内存缓冲区
- **违反了 Raft 的安全性保证！**

**可能导致的问题：**
1. 节点崩溃时，已发送的 `persisted_messages` 依赖的数据丢失
2. 其他节点收到消息后期待的日志条目不存在
3. Raft 状态机不一致

---

#### 问题 2: committed_entries 需要持久化吗？

**你的理解：**
> 对于 `ready.take_committed_entries()` 的处理需要像 1 一样吗？也是将 entry 持久化到本地存储？

**答案：⚠️ 需要分情况！**

**官方文档说明：**
> Although Raft guarantees only **persisted committed entries** will be applied, it doesn't guarantee **commit index is persisted before being applied**.

**关键点：**

1. **Entry 本身已经在之前持久化了**
   - `ready.entries()` 包含**所有新日志**（包括未提交的）
   - `ready.committed_entries()` 是 `entries` 的**子集**（已经被持久化过了）
   - ✅ **不需要再次持久化 Entry 数据**

2. **Commit Index 必须持久化**
   - Commit Index 决定了哪些日志可以应用
   - **必须在应用前或同时持久化 Commit Index**
   - ❌ **crabmq 当前实现有问题**

**当前 crabmq 的实现：**

```rust
// node.rs:429-430
self.handle_entries(ready.take_committed_entries(), &store).await;
```

在 `handle_entries()` 中：
- ✅ 处理 EntryConfChange（调用 `apply_conf_change`）
- ❌ **没有持久化 applied index**
- ❌ EntryNormal 未实现

**缺失的关键逻辑：**
```rust
// 应该在应用后保存 applied index
for entry in committed_entries {
    // 应用到状态机
    apply_to_state_machine(entry);

    // ❌ 缺失：持久化 applied index
    store.set_applied_index(entry.index)?;
}
```

**为什么需要持久化 applied index？**
- 重启后需要知道哪些日志已经应用，避免重复应用
- 如果不持久化，重启后会从 index 0 开始重新应用所有日志
- 对于有副作用的操作（如数据库写入），重复应用会导致错误

---

## 当前实现的严重 Bug

### Bug 1: persisted_messages 发送时机错误 🔴 严重

**位置：** `node.rs:442`

**问题：**
```rust
// 5. handle HardState
if let Some(hs) = ready.hs() {
    let _ = store.set_hard_state(hs);  // ← 同步调用，但可能异步写入
}
// 6. handle persisted messages
self.handle_messages(ready.take_persisted_messages()).await;  // ← 异步发送
```

**正确做法：**
```rust
// 5. handle HardState (同步持久化)
if let Some(hs) = ready.hs() {
    store.set_hard_state(hs)?;
}

// 5.5. 确保所有数据已刷盘
store.flush()?;  // ← 关键：确保数据真正写入磁盘

// 6. handle persisted messages (现在安全了)
self.handle_messages(ready.take_persisted_messages()).await;
```

**影响：** 可能导致数据丢失、Raft 状态不一致

---

### Bug 2: applied index 未持久化 🔴 严重

**位置：** `node.rs:508-581`

**问题：**
```rust
async fn handle_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    for entry in entries {
        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                // ❌ 注释掉了，未实现
            }
            EntryType::EntryConfChange => {
                let cs = raw_node.apply_conf_change(&cc).unwrap();
                store.set_conf_state(&cs);  // ← 只保存了 conf_state
                // ❌ 没有保存 applied index
            }
        }
    }
}
```

**正确做法：**
```rust
async fn handle_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    for entry in entries {
        if entry.data.is_empty() {
            continue;
        }

        // 应用到状态机
        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                self.apply_normal_entry(&entry).await?;
            }
            EntryType::EntryConfChange => {
                self.apply_conf_change(&entry, store).await?;
            }
        }

        // ✅ 关键：持久化 applied index
        store.set_applied_index(entry.index)?;
    }
}
```

**影响：** 重启后重复应用日志，可能导致数据错误

---

### Bug 3: committed_entries 处理顺序错误 🟡 中等

**位置：** `node.rs:429`

**问题：**
```rust
// 3. handle committed entries
self.handle_entries(ready.take_committed_entries(), &store).await;  // ← 先应用
// 4. handle entries
if let Err(e) = store.append(&ready.take_entries()) {  // ← 后持久化
    error!("persist raft log fail: {:?}", e);
    return;
}
```

**官方推荐顺序：**
```
1. 发送 messages
2. 应用 snapshot
3. 持久化 entries         ← 先持久化
4. 持久化 hardstate
5. 应用 committed_entries  ← 后应用
6. 发送 persisted_messages
```

**虽然 `committed_entries` 是 `entries` 的子集（理论上已持久化），但这样做更安全。**

**影响：** 如果 `entries` 持久化失败，已应用的 `committed_entries` 可能丢失

---

### Bug 4: 错误处理不当 🟡 中等

**位置：** 多处

**问题：**
```rust
// 示例 1：忽略错误
let _ = store.set_hard_state(hs);  // ❌ 持久化失败被忽略

// 示例 2：panic 风险
cc.merge_from_bytes(&entry.data).unwrap();  // ❌ 数据损坏会 panic

// 示例 3：返回但不处理
if let Err(e) = store.append(&ready.take_entries()) {
    error!("persist raft log fail: {:?}", e);
    return;  // ❌ 没有重试或恢复机制
}
```

**正确做法：**
```rust
// 持久化必须成功
store.set_hard_state(hs)
    .expect("FATAL: failed to persist hard state");

// 数据解析失败应记录并跳过
match cc.merge_from_bytes(&entry.data) {
    Ok(_) => { /* 处理 */ }
    Err(e) => {
        error!("Failed to parse ConfChange at index {}: {:?}", entry.index, e);
        continue;  // 跳过损坏的条目
    }
}
```

**影响：** 静默失败，难以排查问题

---

## Raft-rs Ready 处理的正确流程

### 官方标准流程（7 步）

根据 raft-rs 文档和 TiKV 实践：

```
┌─────────────────────────────────────────────────────────────────┐
│                         处理 Ready 的正确顺序                       │
└─────────────────────────────────────────────────────────────────┘

1. 发送普通消息 (messages)
   └─► 可并行发送，不需要等待持久化
   └─► 如果包含 MsgSnap，需要通过 report_snapshot 回报状态

2. 应用快照 (snapshot)
   └─► 如果快照非空，应用到存储
   └─► 这会替换当前的所有日志

3. 持久化日志条目 (entries)                    ← 关键！先持久化
   └─► 将未提交的日志写入稳定存储
   └─► 必须保证写入成功

4. 持久化 HardState (hs)                       ← 关键！先持久化
   └─► 保存 term、vote、commit
   └─► 必须保证写入成功

5. 确保刷盘 (flush)                            ← 关键！确保写入磁盘
   └─► 调用 fsync 或 flush
   └─► 确保数据真正落盘

6. 发送持久化消息 (persisted_messages)         ← 关键！必须在持久化后
   └─► 这些消息依赖已持久化的数据
   └─► 必须等待第 3-5 步完成

7. Advance 和 Light Ready
   ├─► advance(ready)
   ├─► 更新 commit index
   ├─► 应用已提交条目 (committed_entries)      ← 应用到状态机
   │   └─► 持久化 applied index               ← 关键！
   ├─► 发送 light_ready.messages()
   ├─► 应用 light_ready.committed_entries()
   └─► advance_apply()
```

### 关键不变式

| 不变式 | 说明 | 违反后果 |
|-------|------|---------|
| **entries 持久化在先** | entries 必须在 persisted_messages 发送前持久化 | 节点崩溃后日志丢失，无法响应其他节点请求 |
| **hardstate 持久化在先** | hardstate 必须在 persisted_messages 发送前持久化 | term/vote 丢失，可能导致脑裂 |
| **applied ≤ committed** | applied index 不能超过 committed index | 应用未提交日志，违反 Raft 保证 |
| **committed ≤ last_index** | commit index 不能超过最后日志索引 | 指向不存在的日志 |
| **applied index 持久化** | 应用条目后必须持久化 applied index | 重启后重复应用 |

---

## 重构方案

### 方案 1：最小改动方案 (推荐用于快速修复)

**目标：** 修复严重 Bug，保持当前架构

#### 1.1 修复 persisted_messages 发送时机

```rust
// node.rs:410-465
async fn handle_all_ready(&self) {
    let result = 'ready_block: {
        let mut raw_node = self.raw_node.lock().await;
        if !raw_node.has_ready() {
            break 'ready_block (None, raw_node.raft.raft_log.store.clone());
        }
        (Some(raw_node.ready()), raw_node.raft.raft_log.store.clone())
    };
    if result.0.is_none() {
        return;
    }
    let store = result.1;
    let mut ready = result.0.unwrap();

    // ===== 第一阶段：发送普通消息 =====
    // 1. handle messages (可并行，不需要等待持久化)
    self.send_messages(ready.take_messages()).await;

    // ===== 第二阶段：处理快照 =====
    // 2. handle snapshot
    if !ready.snapshot().is_empty() {
        self.apply_snapshot(ready.snapshot(), &store).await;
    }

    // ===== 第三阶段：持久化数据 (同步操作) =====
    // 3. 持久化日志条目
    if !ready.entries().is_empty() {
        if let Err(e) = store.append(&ready.take_entries()) {
            error!("FATAL: Failed to persist entries: {:?}", e);
            panic!("Cannot continue without persisting entries");
        }
    }

    // 4. 持久化 HardState
    if let Some(hs) = ready.hs() {
        if let Err(e) = store.set_hard_state(hs) {
            error!("FATAL: Failed to persist hard state: {:?}", e);
            panic!("Cannot continue without persisting hard state");
        }
    }

    // ✅ 新增：确保数据刷盘
    if let Err(e) = store.flush() {
        error!("FATAL: Failed to flush storage: {:?}", e);
        panic!("Cannot continue without flushing storage");
    }

    // ===== 第四阶段：发送持久化消息 =====
    // 5. ✅ 修复：现在可以安全发送 persisted_messages
    self.send_messages(ready.take_persisted_messages()).await;

    // ===== 第五阶段：Advance =====
    let mut light_rd = {
        let mut raw_node = self.raw_node.lock().await;
        raw_node.advance(ready)
    };

    // 6. 更新 commit index
    if let Some(commit) = light_rd.commit_index() {
        if let Err(e) = store.set_hard_state_commit(commit) {
            error!("Failed to update commit index: {:?}", e);
        }
    }

    // ===== 第六阶段：应用已提交条目 =====
    // 7. ✅ 修复：先处理 light_ready 的 committed_entries，持久化 applied index
    if !light_rd.committed_entries().is_empty() {
        self.apply_committed_entries(light_rd.take_committed_entries(), &store)
            .await;
    }

    // 8. 发送 light_ready 的消息
    self.send_messages(light_rd.take_messages()).await;

    // 9. Advance apply
    let mut raw_node = self.raw_node.lock().await;
    raw_node.advance_apply();
}
```

#### 1.2 重命名函数，消除混淆

```rust
// 旧名称 → 新名称

handle_messages()  → send_messages()          // 发送到其他节点
handle_entries()   → apply_committed_entries() // 应用到状态机
handle_raft_message() → receive_raft_message() // 接收 Raft 协议消息
```

#### 1.3 实现 applied index 持久化

```rust
async fn apply_committed_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    for entry in entries {
        if entry.data.is_empty() {
            // Leader 切换时的空条目，仍需更新 applied index
            if let Err(e) = store.set_applied_index(entry.index) {
                error!("Failed to update applied index to {}: {:?}", entry.index, e);
            }
            continue;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                // ✅ 实现业务逻辑
                if let Err(e) = self.apply_normal_entry(&entry).await {
                    error!("Failed to apply entry at index {}: {:?}", entry.index, e);
                    continue;
                }
            }

            EntryType::EntryConfChange => {
                if let Err(e) = self.apply_conf_change_entry(&entry, store).await {
                    error!("Failed to apply conf change at index {}: {:?}", entry.index, e);
                    continue;
                }
            }

            EntryType::EntryConfChangeV2 => {
                if let Err(e) = self.apply_conf_change_v2_entry(&entry, store).await {
                    error!("Failed to apply conf change v2 at index {}: {:?}", entry.index, e);
                    continue;
                }
            }
        }

        // ✅ 关键：持久化 applied index
        if let Err(e) = store.set_applied_index(entry.index) {
            error!("FATAL: Failed to persist applied index {}: {:?}", entry.index, e);
            panic!("Cannot continue without persisting applied index");
        }
    }
}
```

#### 1.4 添加 flush() 方法到 SledStorage

```rust
// storage.rs
impl SledStorage {
    pub fn flush(&self) -> Result<()> {
        self.db.flush()
            .map_err(|e| Error::Store(StorageError::Other(Box::new(e))))?;
        Ok(())
    }

    pub fn set_applied_index(&self, index: u64) -> Result<()> {
        self.db.insert(APPLIED_INDEX_KEY, &index.to_be_bytes())
            .map_err(|e| Error::Store(StorageError::Other(Box::new(e))))?;
        Ok(())
    }

    pub fn get_applied_index(&self) -> Result<u64> {
        match self.db.get(APPLIED_INDEX_KEY)? {
            Some(bytes) => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                Ok(u64::from_be_bytes(buf))
            }
            None => Ok(0),
        }
    }
}

const APPLIED_INDEX_KEY: &[u8] = b"applied_index";
```

---

### 方案 2：完整重构方案 (推荐用于长期维护)

**目标：** 清晰的消息流转、更好的可测试性

#### 2.1 消息分层

```rust
// 定义清晰的消息层次
pub mod message {
    /// 应用层消息（外部输入）
    pub enum AppMessage {
        /// 元数据请求（节点发现）
        MetaRequest {
            remote_addr: String,
            request: CooRaftGetMetaRequest,
        },
        /// 配置变更请求（添加/删除节点）
        ConfChangeRequest {
            request: CooRaftConfChangeRequest,
        },
        /// 数据提议请求（业务数据）
        ProposeRequest {
            request: CooRaftProposeMessage,
        },
        /// Raft 协议消息（节点间通信）
        RaftProtocol {
            message: raft::prelude::Message,
        },
    }

    /// Raft 输出消息（内部输出）
    pub enum RaftOutput {
        /// 需要发送到其他节点的消息
        OutboundMessages(Vec<raft::prelude::Message>),
        /// 需要应用到状态机的条目
        CommittedEntries(Vec<raft::prelude::Entry>),
        /// 需要持久化的快照
        Snapshot(raft::prelude::Snapshot),
    }
}
```

#### 2.2 状态机分离

```rust
/// 业务状态机接口
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// 应用普通日志条目
    async fn apply(&mut self, index: u64, data: &[u8]) -> Result<()>;

    /// 获取当前的 applied index
    fn applied_index(&self) -> u64;

    /// 应用快照
    async fn apply_snapshot(&mut self, snapshot: &[u8]) -> Result<()>;

    /// 生成快照
    async fn snapshot(&self) -> Result<Vec<u8>>;
}

/// Raft 节点（只负责共识）
pub struct RaftNode<S: Storage> {
    raw_node: RawNode<S>,
    // 不再包含业务逻辑
}

/// 应用节点（业务逻辑 + Raft）
pub struct AppNode<S: Storage, M: StateMachine> {
    raft: RaftNode<S>,
    state_machine: M,
    // 其他组件
}
```

#### 2.3 重构 handle_all_ready

```rust
impl<S: Storage> RaftNode<S> {
    /// 处理 Ready，返回需要执行的操作
    pub fn handle_ready(&mut self) -> Option<ReadyActions> {
        if !self.raw_node.has_ready() {
            return None;
        }

        let mut ready = self.raw_node.ready();

        Some(ReadyActions {
            messages: ready.take_messages(),
            snapshot: ready.snapshot().clone(),
            entries: ready.take_entries(),
            hard_state: ready.hs().cloned(),
            committed_entries: ready.take_committed_entries(),
            persisted_messages: ready.take_persisted_messages(),
            ready_for_advance: ready,
        })
    }
}

/// Ready 需要执行的操作（纯数据）
pub struct ReadyActions {
    pub messages: Vec<Message>,
    pub snapshot: Snapshot,
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub committed_entries: Vec<Entry>,
    pub persisted_messages: Vec<Message>,
    ready_for_advance: Ready,
}

impl<S: Storage, M: StateMachine> AppNode<S, M> {
    /// 执行 Ready 操作（明确顺序）
    pub async fn execute_ready_actions(&mut self, mut actions: ReadyActions) -> Result<()> {
        // Phase 1: 发送普通消息（可并行）
        self.send_messages(actions.messages).await;

        // Phase 2: 应用快照
        if !actions.snapshot.is_empty() {
            self.apply_snapshot(&actions.snapshot).await?;
        }

        // Phase 3: 持久化（同步操作）
        if !actions.entries.is_empty() {
            self.storage.append(&actions.entries)?;
        }
        if let Some(hs) = actions.hard_state {
            self.storage.set_hard_state(&hs)?;
        }
        self.storage.flush()?;  // ✅ 确保刷盘

        // Phase 4: 发送持久化消息（必须在持久化后）
        self.send_messages(actions.persisted_messages).await;

        // Phase 5: Advance
        let mut light_rd = self.raft.raw_node.advance(actions.ready_for_advance);

        // Phase 6: 更新 commit index
        if let Some(commit) = light_rd.commit_index() {
            self.storage.set_commit(commit)?;
        }

        // Phase 7: 应用已提交条目
        self.apply_committed_entries(actions.committed_entries).await?;
        self.apply_committed_entries(light_rd.take_committed_entries()).await?;

        // Phase 8: 发送 light_ready 消息
        self.send_messages(light_rd.take_messages()).await;

        // Phase 9: Advance apply
        self.raft.raw_node.advance_apply();

        Ok(())
    }

    async fn apply_committed_entries(&mut self, entries: Vec<Entry>) -> Result<()> {
        for entry in entries {
            if entry.data.is_empty() {
                self.storage.set_applied_index(entry.index)?;
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    // 委托给状态机
                    self.state_machine.apply(entry.index, &entry.data).await?;
                }
                EntryType::EntryConfChange => {
                    self.apply_conf_change(&entry).await?;
                }
                EntryType::EntryConfChangeV2 => {
                    self.apply_conf_change_v2(&entry).await?;
                }
            }

            // ✅ 持久化 applied index
            self.storage.set_applied_index(entry.index)?;
        }
        Ok(())
    }
}
```

---

## 代码对比

### Before (当前实现)

```rust
// ❌ 问题 1: 命名混淆
async fn handle_messages(&self, messages: Vec<Message>) {
    // 实际是"发送"消息，不是"处理"
}

async fn handle_raft_message(&self, req: &CooRaftOriginMessage) {
    // 这才是"处理"消息
}

// ❌ 问题 2: 持久化顺序错误
async fn handle_all_ready(&self) {
    // 1. 发送消息
    self.handle_messages(ready.take_messages()).await;

    // 2. 应用 committed entries
    self.handle_entries(ready.take_committed_entries(), &store).await;

    // 3. 持久化 entries
    store.append(&ready.take_entries());

    // 4. 持久化 hardstate
    let _ = store.set_hard_state(hs);  // ❌ 忽略错误

    // 5. ❌ Bug: 在持久化后立即发送，但持久化可能还没完成
    self.handle_messages(ready.take_persisted_messages()).await;
}

// ❌ 问题 3: 缺少 applied index 持久化
async fn handle_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    for entry in entries {
        match entry.get_entry_type() {
            EntryType::EntryConfChange => {
                raw_node.apply_conf_change(&cc);
                store.set_conf_state(&cs);
                // ❌ 没有保存 applied index
            }
        }
    }
}
```

### After (修复后)

```rust
// ✅ 改进 1: 清晰的命名
async fn send_messages(&self, messages: Vec<Message>) {
    // 发送到其他节点
}

async fn receive_raft_message(&self, req: &CooRaftOriginMessage) {
    // 接收并处理
}

// ✅ 改进 2: 正确的持久化顺序
async fn handle_all_ready(&self) {
    // Phase 1: 发送普通消息（并行）
    self.send_messages(ready.take_messages()).await;

    // Phase 2: 应用快照
    self.apply_snapshot(ready.snapshot(), &store).await;

    // Phase 3: 持久化（同步）
    store.append(&ready.take_entries())?;  // ✅ 检查错误
    if let Some(hs) = ready.hs() {
        store.set_hard_state(hs)?;  // ✅ 检查错误
    }
    store.flush()?;  // ✅ 确保刷盘

    // Phase 4: 发送持久化消息（现在安全了）
    self.send_messages(ready.take_persisted_messages()).await;

    // Phase 5-9: Advance 和应用
    let mut light_rd = raw_node.advance(ready);
    if let Some(commit) = light_rd.commit_index() {
        store.set_hard_state_commit(commit)?;
    }
    self.apply_committed_entries(light_rd.take_committed_entries(), &store).await;
    self.send_messages(light_rd.take_messages()).await;
    raw_node.advance_apply();
}

// ✅ 改进 3: 持久化 applied index
async fn apply_committed_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    for entry in entries {
        if entry.data.is_empty() {
            store.set_applied_index(entry.index)?;  // ✅ 空条目也要更新
            continue;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                self.apply_normal_entry(&entry).await?;
            }
            EntryType::EntryConfChange => {
                self.apply_conf_change(&entry, store).await?;
            }
            EntryType::EntryConfChangeV2 => {
                self.apply_conf_change_v2(&entry, store).await?;
            }
        }

        // ✅ 持久化 applied index
        store.set_applied_index(entry.index)?;
    }
}
```

---

## 测试验证

### 测试 1: 验证 persisted_messages 在持久化后发送

```rust
#[tokio::test]
async fn test_persisted_messages_after_flush() {
    let node = create_test_node();
    let storage = node.storage.clone();

    // 1. 提议一条日志
    node.propose(b"test data").await.unwrap();

    // 2. 模拟持久化失败
    storage.set_fail_next_flush(true);

    // 3. 处理 ready（应该 panic 或返回错误）
    let result = node.handle_all_ready().await;
    assert!(result.is_err(), "Should fail if flush fails");

    // 4. 验证没有发送 persisted_messages
    let sent_messages = node.get_sent_messages();
    assert!(
        !sent_messages.iter().any(|m| m.msg_type == MessageType::MsgAppend),
        "Should not send persisted_messages if flush failed"
    );
}
```

### 测试 2: 验证 applied index 持久化

```rust
#[tokio::test]
async fn test_applied_index_persistence() {
    let node = create_test_node();

    // 1. 应用一些条目
    for i in 1..=10 {
        node.propose(format!("data{}", i).as_bytes()).await.unwrap();
    }

    // 等待提交
    wait_for_commit(&node, 10).await;

    // 2. 验证 applied index
    let applied = node.storage.get_applied_index().unwrap();
    assert_eq!(applied, 10);

    // 3. 重启节点
    drop(node);
    let node = create_test_node_with_same_storage();

    // 4. 验证重启后 applied index 正确
    let applied_after_restart = node.storage.get_applied_index().unwrap();
    assert_eq!(applied_after_restart, 10, "Applied index should persist across restarts");
}
```

### 测试 3: 验证重启后不重复应用

```rust
#[tokio::test]
async fn test_no_duplicate_apply_after_restart() {
    let state_machine = Arc::new(Mutex::new(TestStateMachine::new()));
    let node = create_test_node_with_state_machine(state_machine.clone());

    // 1. 应用一些条目
    for i in 1..=5 {
        node.propose(format!("data{}", i).as_bytes()).await.unwrap();
    }
    wait_for_commit(&node, 5).await;

    // 2. 记录应用次数
    let apply_count_before = state_machine.lock().await.apply_count();
    assert_eq!(apply_count_before, 5);

    // 3. 重启节点
    drop(node);
    let node = create_test_node_with_state_machine(state_machine.clone());

    // 4. 验证没有重复应用
    tokio::time::sleep(Duration::from_secs(1)).await;
    let apply_count_after = state_machine.lock().await.apply_count();
    assert_eq!(
        apply_count_after, 5,
        "Should not re-apply entries after restart"
    );
}
```

---

## 实施步骤

### 阶段 1: 紧急修复（1-2 天）

**目标：** 修复严重 Bug，确保正确性

1. ✅ 添加 `storage.flush()` 方法
2. ✅ 在 `persisted_messages` 前调用 `flush()`
3. ✅ 添加 `set_applied_index()` 和 `get_applied_index()`
4. ✅ 在 `apply_committed_entries()` 中持久化 applied index
5. ✅ 改进错误处理（持久化失败应 panic）

**风险：** 低（只修改关键路径）

---

### 阶段 2: 重构优化（1 周）

**目标：** 提高代码清晰度

1. ✅ 重命名函数
   - `handle_messages` → `send_messages`
   - `handle_entries` → `apply_committed_entries`
   - `handle_raft_message` → `receive_raft_message`

2. ✅ 调整 Ready 处理顺序
   - 先持久化 entries
   - 后应用 committed_entries

3. ✅ 实现 EntryNormal 处理
   - 定义 StateMachine trait
   - 实现业务逻辑应用

**风险：** 中（需要充分测试）

---

### 阶段 3: 架构改进（2-3 周）

**目标：** 清晰的分层架构

1. ✅ 消息分层（AppMessage / RaftOutput）
2. ✅ 状态机分离（RaftNode / AppNode）
3. ✅ 添加完整测试覆盖

**风险：** 高（大规模重构）

---

## 总结

### 你的两个问题都是正确的！

1. ✅ **persisted_messages 必须在持久化后发送**
   - 当前实现是异步的，持久化可能还没完成
   - 需要添加 `flush()` 确保刷盘

2. ✅ **committed_entries 需要持久化 applied index**
   - Entry 数据本身不需要重复持久化（已在 entries 中持久化过）
   - 但 **applied index 必须持久化**，否则重启后会重复应用

### 当前实现的严重问题

| Bug | 严重程度 | 影响 | 修复优先级 |
|-----|---------|------|-----------|
| persisted_messages 发送时机错误 | 🔴 严重 | 可能导致数据丢失、状态不一致 | P0 |
| applied index 未持久化 | 🔴 严重 | 重启后重复应用日志 | P0 |
| committed_entries 处理顺序错误 | 🟡 中等 | 持久化失败可能丢失已应用数据 | P1 |
| 错误处理不当 | 🟡 中等 | 静默失败，难以排查 | P1 |
| 命名混淆 | 🟢 轻微 | 代码难以理解 | P2 |

### 推荐方案

- **短期：** 方案 1（最小改动）—— 1-2 天完成，立即修复严重 Bug
- **长期：** 方案 2（完整重构）—— 2-3 周完成，建立清晰架构

---

## 附录：相关资源

- [Raft-rs 官方文档](https://docs.rs/raft/)
- [TiKV Raft 实现](https://github.com/tikv/raft-rs)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [Processing Ready State](https://docs.rs/raft/latest/raft/#processing-the-ready-state)

---

**生成日期：** 2026-01-23
**版本：** 1.0
**作者：** Claude Code
