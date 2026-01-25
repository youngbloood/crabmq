# CrabMQ Raft 重构方案 - 清晰架构设计

**版本：** 2.0
**日期：** 2026-01-23
**目标：** 逻辑清晰、架构合理、高效、健壮

---

## 目录

1. [当前架构的"怪怪的"问题分析](#1-当前架构的怪怪的问题分析)
2. [核心混淆点深度剖析](#2-核心混淆点深度剖析)
3. [重构架构设计](#3-重构架构设计)
4. [消息流转完整图解](#4-消息流转完整图解)
5. [详细设计方案](#5-详细设计方案)
6. [代码实现](#6-代码实现)
7. [性能与健壮性保证](#7-性能与健壮性保证)

---

## 1. 当前架构的"怪怪的"问题分析

### 1.1 你感觉"怪怪的"地方到底是什么？

让我们先回顾你提出的疑惑：

> 1. Run Loop 中的消息来自哪里？
> 2. handle_message 的消息来自哪里？
> 3. handle_entries 的消息来自哪里？
> 4. 这些是不是重复处理了？

**核心困惑：** 有太多"消息"（Message）这个词，但它们指的是**完全不同的东西**！

---

### 1.2 混淆的根源：三种"消息"混用同一个概念

当前代码中有 **三种完全不同的"消息"**，但都用了类似的命名：

| 消息类型 | 实际含义 | 代码中的名称 | 混淆点 |
|---------|---------|-------------|--------|
| **应用层消息** | 外部请求（提议、配置变更等） | `TransportMessage` | 叫 "Message" |
| **Raft 协议消息** | 节点间通信（心跳、日志复制等） | `raft::Message` | 也叫 "Message" |
| **日志条目** | 已提交的数据 | `Entry` | 被 `handle_entries` 处理，像"消息" |

**问题：** 代码中到处都是 `handle_message`，但处理的是**不同的东西**！

---

### 1.3 混淆的具体表现

#### 混淆 1: `handle_messages()` 到底处理什么？

```rust
// node.rs:467
async fn handle_messages(&self, messages: Vec<Message>) {
    // 这里的 Message 是 raft::prelude::Message
    // 实际上是"发送 Raft 协议消息到其他节点"
    // 不是"处理消息"，而是"发送消息"！
}
```

**问题：** 函数名叫 `handle`（处理），但实际是 `send`（发送）。

---

#### 混淆 2: `handle_raft_message()` vs `handle_messages()`

```rust
// node.rs:174
async fn handle_raft_message(&self, req: &CooRaftOriginMessage) {
    // 这才是真正"处理"Raft消息（接收）
    let mut msg = Message::new();
    msg.merge_from_bytes(&req.message);
    self.raw_node.lock().await.step(msg);
}

// node.rs:467
async fn handle_messages(&self, messages: Vec<Message>) {
    // 这是"发送"Raft消息
    for msg in messages {
        sender.send(t).await;
    }
}
```

**问题：** 两个函数都有 `message`，但一个是接收，一个是发送，方向相反！

---

#### 混淆 3: Run Loop 中的消息分发

```rust
// node.rs:233-260
match msg.index {
    COO_RAFT_ORIGIN_MESSAGE_INDEX => {
        // 处理"外部的 Raft 协议消息"
        node.handle_raft_message(req).await;
    }
    COO_RAFT_PROPOSE_MESSAGE_INDEX => {
        // 处理"外部的提议请求"
        node.handle_propose_message(req).await;
    }
}

// 然后在 handle_all_ready 中
self.handle_messages(ready.take_messages()).await;  // 发送"内部的 Raft 协议消息"
self.handle_entries(ready.take_committed_entries(), &store).await;  // 处理"日志条目"
```

**问题：**
- `handle_raft_message` = 接收外部消息
- `handle_messages` = 发送内部消息
- `handle_entries` = 处理日志条目

三个函数名都是 `handle_xxx`，但做的事情完全不同！

---

### 1.4 "怪怪的"感觉的流程图

让我画出当前的混乱流程：

```
┌─────────────────────────────────────────────────────────────┐
│                     当前混乱的消息流转                          │
└─────────────────────────────────────────────────────────────┘

外部世界
  │
  ├─► TransportMessage (应用层消息)
  │       │
  │       ├─► ORIGIN_MESSAGE (包装的 Raft 协议消息)
  │       │       └─► handle_raft_message()  ← 这是"接收"
  │       │               └─► raw_node.step(msg)
  │       │
  │       ├─► PROPOSE_MESSAGE (业务提议)
  │       │       └─► handle_propose_message()
  │       │               └─► raw_node.propose(data)
  │       │
  │       └─► CONF_CHANGE_REQUEST (配置变更)
  │               └─► handle_conf_change()
  │                       └─► raw_node.propose_conf_change(cc)
  │
  └─► Raft 状态机处理
          │
          └─► handle_all_ready()
                  │
                  ├─► handle_messages(ready.messages())  ← 这是"发送"！
                  │       └─► 发送到其他节点（变成 ORIGIN_MESSAGE）
                  │
                  └─► handle_entries(ready.committed_entries())  ← 这是"应用"
                          └─► 应用到状态机
```

**困惑点：**
1. `handle_raft_message()` 和 `handle_messages()` 名字相似，但一个接收，一个发送
2. `handle_messages()` 发送的消息，会被其他节点的 `handle_raft_message()` 接收
3. `handle_entries()` 看起来也在"处理消息"，但实际是处理日志条目

**这就是"怪怪的"根本原因！**

---

## 2. 核心混淆点深度剖析

### 2.1 Raft 的三层消息模型

为了理解清楚，我们需要建立 **Raft 的三层消息模型**：

```
┌────────────────────────────────────────────────────────────┐
│                    第 1 层：应用层消息                         │
│  (Application Messages - 外部输入，驱动 Raft)                 │
├────────────────────────────────────────────────────────────┤
│  • 客户端提议 (Propose)                                       │
│  • 配置变更请求 (ConfChange)                                  │
│  • 元数据请求 (MetaRequest)                                   │
│  • Raft 协议消息的传输载体 (ORIGIN_MESSAGE)                    │
└───────────┬────────────────────────────────────────────────┘
            │ 输入到
            ▼
┌────────────────────────────────────────────────────────────┐
│                  第 2 层：Raft 协议消息                        │
│  (Raft Protocol Messages - Raft 节点间通信)                  │
├────────────────────────────────────────────────────────────┤
│  • MsgRequestVote (投票请求)                                 │
│  • MsgAppend (日志复制)                                      │
│  • MsgHeartbeat (心跳)                                       │
│  • MsgSnapshot (快照)                                        │
│  • ... (共 19 种)                                            │
└───────────┬────────────────────────────────────────────────┘
            │ 处理后产生
            ▼
┌────────────────────────────────────────────────────────────┐
│                   第 3 层：日志条目                            │
│  (Log Entries - Raft 输出，需要应用到状态机)                   │
├────────────────────────────────────────────────────────────┤
│  • EntryNormal (业务数据)                                    │
│  • EntryConfChange (配置变更)                                │
│  • EntryConfChangeV2 (批量配置变更)                           │
└────────────────────────────────────────────────────────────┘
```

**关键理解：**
- **第 1 层** → **输入**：驱动 Raft 状态机
- **第 2 层** → **流通**：Raft 节点间通信
- **第 3 层** → **输出**：最终要应用的数据

---

### 2.2 当前代码的混淆点详解

#### 问题 1: `handle_messages()` 命名错误

```rust
async fn handle_messages(&self, messages: Vec<Message>) {
    // 实际上是"发送"，不是"处理"
}
```

**应该叫什么？**
- `send_raft_messages()` - 清晰表示"发送 Raft 协议消息"
- 或 `broadcast_to_peers()` - 表示"广播给对等节点"

---

#### 问题 2: `handle_raft_message()` 和 `handle_messages()` 对称性错误

```rust
// 接收
async fn handle_raft_message(&self, req: &CooRaftOriginMessage) {
    // 从其他节点接收 Raft 协议消息
}

// 发送
async fn handle_messages(&self, messages: Vec<Message>) {
    // 发送 Raft 协议消息到其他节点
}
```

**问题：** 一个是单数 `message`，一个是复数 `messages`，但做的是相反的事情！

**应该改成：**
```rust
async fn receive_raft_message(&self, msg: Message)  // 接收（单向）
async fn send_raft_messages(&self, msgs: Vec<Message>)  // 发送（单向）
```

---

#### 问题 3: `handle_entries()` 的混淆

```rust
async fn handle_entries(&self, entries: Vec<Entry>, store: &SledStorage) {
    // 实际上是"应用到状态机"
}
```

**问题：** `entries` 不是消息，是日志条目。`handle` 太泛化。

**应该叫什么？**
- `apply_committed_entries()` - 清晰表示"应用已提交的日志"
- 或 `commit_to_state_machine()` - 表示"提交到状态机"

---

### 2.3 消息流转的真实路径

让我画出**真实且清晰**的流程：

```
┌──────────────────────────────────────────────────────────────┐
│                   真实的消息流转路径                            │
└──────────────────────────────────────────────────────────────┘

第 1 层：应用层消息（外部输入）
─────────────────────────────────

  客户端/其他节点
       │
       │ 发送 TransportMessage
       │
       ▼
  ┌─────────────────────┐
  │   Run Loop          │
  │   (tokio::select!)  │
  └─────────┬───────────┘
            │
            │ match msg.index
            │
    ┌───────┴────────┬──────────────┬─────────────┐
    │                │              │             │
    ▼                ▼              ▼             ▼
 ORIGIN_MSG    PROPOSE_MSG   CONF_CHANGE   META_REQUEST
    │                │              │             │
    │                │              │             │
    ▼                ▼              ▼             ▼
receive_         propose_      propose_conf_   handle_meta
raft_msg()       data()        change()        request()
    │                │              │             │
    └────────────────┴──────────────┴─────────────┘
                     │
                     │ 输入到 Raft
                     ▼

第 2 层：Raft 协议消息（内部处理）
─────────────────────────────────

         ┌───────────────────┐
         │  RawNode (Raft)   │
         │                   │
         │  • step(msg)      │ ← 接收协议消息
         │  • propose(data)  │ ← 接收业务提议
         │  • tick()         │ ← 定时器驱动
         │                   │
         │  [Raft 算法处理]   │
         │                   │
         │  • 选举           │
         │  • 日志复制        │
         │  • 提交决策        │
         └─────────┬─────────┘
                   │
                   │ 生成 Ready
                   │
                   ▼
         ┌──────────────────────┐
         │  Ready 输出           │
         ├──────────────────────┤
         │  • messages          │ ← Raft 协议消息（输出）
         │  • committed_entries │ ← 已提交日志（输出）
         │  • entries           │ ← 未持久化日志（输出）
         │  • snapshot          │ ← 快照（输出）
         └─────────┬────────────┘
                   │
                   │ handle_all_ready()
                   │
    ┌──────────────┴──────────────┬──────────────┐
    │                             │              │
    ▼                             ▼              ▼
send_raft_         apply_         persist_
messages()         committed_     entries()
                   entries()
    │                             │              │
    │                             │              │
    └─────────────────────────────┴──────────────┘

第 3 层：输出处理
─────────────────

send_raft_messages():
    └─► 发送到其他节点
        └─► 变成对方的 ORIGIN_MESSAGE
            └─► 对方调用 receive_raft_msg()
                └─► 循环回第 2 层

apply_committed_entries():
    └─► 应用到状态机
        └─► 业务逻辑处理
        └─► 回调客户端

persist_entries():
    └─► 写入存储
        └─► 数据持久化
```

**清晰的关键点：**
1. **输入** → Run Loop 接收 TransportMessage
2. **驱动** → 转换为 Raft 的 step/propose 调用
3. **处理** → Raft 内部算法，生成 Ready
4. **输出** → 发送消息 + 应用日志 + 持久化

---

## 3. 重构架构设计

### 3.1 设计原则

1. **单一职责：** 每个组件只做一件事
2. **清晰命名：** 函数名准确表达意图（send/receive/apply/persist）
3. **明确边界：** 清晰的输入/输出接口
4. **类型安全：** 使用类型系统区分不同的消息
5. **可测试性：** 每层可独立测试

---

### 3.2 新架构分层

```
┌──────────────────────────────────────────────────────────┐
│                      应用层 (Node)                         │
│  • 对外提供 API (propose, add_peer, etc.)                 │
│  • 管理业务逻辑和状态机                                      │
└────────────┬─────────────────────────────────────────────┘
             │
             │ 使用
             ▼
┌──────────────────────────────────────────────────────────┐
│                   Raft 层 (RaftCore)                      │
│  • 封装 RawNode                                           │
│  • 处理 Ready 流程                                         │
│  • 管理 Raft 生命周期                                      │
└────────────┬─────────────────────────────────────────────┘
             │
             │ 依赖
             ▼
┌──────────────────────────────────────────────────────────┐
│                  传输层 (MessageRouter)                    │
│  • 收发网络消息                                            │
│  • 消息序列化/反序列化                                      │
│  • 连接管理                                               │
└────────────┬─────────────────────────────────────────────┘
             │
             │ 依赖
             ▼
┌──────────────────────────────────────────────────────────┐
│                  存储层 (Storage)                          │
│  • 日志持久化                                             │
│  • 状态持久化                                             │
│  • 快照管理                                               │
└──────────────────────────────────────────────────────────┘
```

---

### 3.3 消息类型设计

#### 3.3.1 应用层消息（清晰的枚举）

```rust
/// 应用层输入消息 - 外部请求
pub enum ApplicationInput {
    /// 客户端提议业务数据
    ClientProposal {
        data: Vec<u8>,
        callback: oneshot::Sender<Result<()>>,
    },

    /// 配置变更请求
    ConfigChange {
        change: ConfChange,
    },

    /// Raft 协议消息（从其他节点接收）
    RaftProtocol {
        from: u64,
        message: raft::Message,
    },

    /// 节点发现请求
    PeerDiscovery {
        peer_id: u64,
        address: String,
    },
}
```

---

#### 3.3.2 Raft 层输出（清晰的结构）

```rust
/// Raft 层输出 - 需要处理的操作
pub struct RaftOutput {
    /// 需要发送到其他节点的 Raft 协议消息
    pub outbound_messages: Vec<OutboundMessage>,

    /// 需要持久化的数据
    pub persist_ops: PersistOperations,

    /// 需要应用到状态机的日志条目
    pub committed_entries: Vec<Entry>,

    /// 需要应用的快照
    pub snapshot: Option<Snapshot>,
}

/// 出站消息 - 明确目标
pub struct OutboundMessage {
    pub to: u64,
    pub message: raft::Message,
    pub require_persistence: bool,  // 是否需要先持久化再发送
}

/// 持久化操作 - 原子批量
pub struct PersistOperations {
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub snapshot: Option<Snapshot>,
    pub must_sync: bool,  // 是否需要 fsync
}
```

---

### 3.4 核心组件设计

#### 3.4.1 MessageRouter - 消息路由器

**职责：** 接收外部消息，路由到正确的处理函数

```rust
pub struct MessageRouter {
    node_id: u64,
    raft_core: Arc<RaftCore>,
    transport: Arc<Transport>,
}

impl MessageRouter {
    /// 启动消息循环
    pub async fn run(&self) {
        loop {
            select! {
                // 从网络接收
                msg = self.transport.receive() => {
                    self.route_message(msg).await;
                }

                // 定时器
                _ = self.ticker.tick() => {
                    self.raft_core.tick().await;
                }
            }

            // 处理 Raft 输出
            self.process_raft_output().await;
        }
    }

    /// 路由消息到正确的处理器
    async fn route_message(&self, msg: TransportMessage) {
        match msg.payload {
            Payload::RaftProtocol(raft_msg) => {
                self.raft_core.receive_raft_message(raft_msg).await;
            }
            Payload::ClientProposal(data) => {
                self.raft_core.propose(data).await;
            }
            Payload::ConfigChange(cc) => {
                self.raft_core.propose_conf_change(cc).await;
            }
            Payload::PeerDiscovery(info) => {
                self.handle_peer_discovery(info).await;
            }
        }
    }
}
```

---

#### 3.4.2 RaftCore - Raft 核心

**职责：** 封装 RawNode，处理 Ready 流程

```rust
pub struct RaftCore {
    raw_node: Mutex<RawNode<Storage>>,
    storage: Arc<Storage>,
    ready_queue: Mutex<VecDeque<RaftOutput>>,
}

impl RaftCore {
    /// 接收 Raft 协议消息
    pub async fn receive_raft_message(&self, msg: raft::Message) {
        let mut node = self.raw_node.lock().await;
        node.step(msg).unwrap();
    }

    /// 提议业务数据
    pub async fn propose(&self, data: Vec<u8>) {
        let mut node = self.raw_node.lock().await;
        node.propose(vec![], data).unwrap();
    }

    /// 处理 Ready（核心逻辑）
    pub async fn process_ready(&self) -> Option<RaftOutput> {
        let mut node = self.raw_node.lock().await;
        if !node.has_ready() {
            return None;
        }

        let mut ready = node.ready();

        // 构造输出
        let output = RaftOutput {
            outbound_messages: self.extract_outbound_messages(&ready),
            persist_ops: self.extract_persist_ops(&ready),
            committed_entries: ready.take_committed_entries(),
            snapshot: if ready.snapshot().is_empty() {
                None
            } else {
                Some(ready.snapshot().clone())
            },
        };

        // Advance
        let light_rd = node.advance(ready);
        // ... 处理 light_ready

        Some(output)
    }

    /// 提取出站消息（区分是否需要持久化）
    fn extract_outbound_messages(&self, ready: &Ready) -> Vec<OutboundMessage> {
        let mut messages = Vec::new();

        // 普通消息（立即发送）
        for msg in ready.messages() {
            messages.push(OutboundMessage {
                to: msg.to,
                message: msg.clone(),
                require_persistence: false,
            });
        }

        // 持久化消息（需要先持久化）
        for msg in ready.persisted_messages() {
            messages.push(OutboundMessage {
                to: msg.to,
                message: msg.clone(),
                require_persistence: true,
            });
        }

        messages
    }
}
```

---

#### 3.4.3 OutputProcessor - 输出处理器

**职责：** 按正确顺序处理 Raft 输出

```rust
pub struct OutputProcessor {
    storage: Arc<Storage>,
    transport: Arc<Transport>,
    state_machine: Arc<Mutex<dyn StateMachine>>,
}

impl OutputProcessor {
    /// 处理 Raft 输出（严格按顺序）
    pub async fn process(&self, output: RaftOutput) -> Result<()> {
        // 阶段 1: 发送不需要持久化的消息（并行）
        self.send_immediate_messages(&output).await;

        // 阶段 2: 应用快照
        if let Some(snapshot) = output.snapshot {
            self.apply_snapshot(&snapshot).await?;
        }

        // 阶段 3: 持久化（同步，确保完成）
        if !output.persist_ops.is_empty() {
            self.persist(&output.persist_ops).await?;
        }

        // 阶段 4: 发送需要持久化的消息（在持久化后）
        self.send_persisted_messages(&output).await;

        // 阶段 5: 应用已提交的日志
        if !output.committed_entries.is_empty() {
            self.apply_entries(&output.committed_entries).await?;
        }

        Ok(())
    }

    /// 发送立即消息（不需要等待持久化）
    async fn send_immediate_messages(&self, output: &RaftOutput) {
        let immediate_msgs: Vec<_> = output.outbound_messages
            .iter()
            .filter(|m| !m.require_persistence)
            .collect();

        // 并行发送（提高性能）
        futures::future::join_all(
            immediate_msgs.into_iter().map(|m| {
                self.transport.send(m.to, &m.message)
            })
        ).await;
    }

    /// 持久化数据（同步，确保完成）
    async fn persist(&self, ops: &PersistOperations) -> Result<()> {
        // 1. 持久化日志条目
        if !ops.entries.is_empty() {
            self.storage.append_entries(&ops.entries)?;
        }

        // 2. 持久化 HardState
        if let Some(hs) = &ops.hard_state {
            self.storage.save_hard_state(hs)?;
        }

        // 3. 持久化快照
        if let Some(snapshot) = &ops.snapshot {
            self.storage.save_snapshot(snapshot)?;
        }

        // 4. 确保刷盘
        if ops.must_sync {
            self.storage.flush()?;
        }

        Ok(())
    }

    /// 发送持久化消息（在持久化后）
    async fn send_persisted_messages(&self, output: &RaftOutput) {
        let persisted_msgs: Vec<_> = output.outbound_messages
            .iter()
            .filter(|m| m.require_persistence)
            .collect();

        for msg in persisted_msgs {
            self.transport.send(msg.to, &msg.message).await;
        }
    }

    /// 应用已提交的日志
    async fn apply_entries(&self, entries: &[Entry]) -> Result<()> {
        let mut sm = self.state_machine.lock().await;

        for entry in entries {
            if entry.data.is_empty() {
                // 空条目，更新 applied index
                self.storage.set_applied_index(entry.index)?;
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    sm.apply(entry.index, &entry.data).await?;
                }
                EntryType::EntryConfChange => {
                    self.apply_conf_change(entry).await?;
                }
                EntryType::EntryConfChangeV2 => {
                    self.apply_conf_change_v2(entry).await?;
                }
            }

            // 持久化 applied index
            self.storage.set_applied_index(entry.index)?;
        }

        Ok(())
    }
}
```

---

## 4. 消息流转完整图解

### 4.1 完整的端到端流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        完整的消息流转（清晰版）                             │
└─────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════
阶段 1: 外部输入 → Raft 核心
═══════════════════════════════════════════════════════════════════════════

  [客户端]                [其他 Raft 节点]
      │                         │
      │ propose(data)           │ send MsgAppend
      │                         │
      ▼                         ▼
┌─────────────────────────────────────────┐
│      Transport (网络层)                  │
│  • 接收 TCP/QUIC 消息                    │
│  • 反序列化                              │
└──────────┬──────────────────────────────┘
           │
           │ TransportMessage
           │
           ▼
┌─────────────────────────────────────────┐
│   MessageRouter (消息路由)               │
│                                         │
│   match msg.payload:                   │
│   ├─ RaftProtocol → receive_raft_msg   │
│   ├─ ClientProposal → propose          │
│   ├─ ConfigChange → propose_conf_change│
│   └─ PeerDiscovery → handle_discovery  │
└──────────┬──────────────────────────────┘
           │
           │ 分发到具体函数
           │
    ┌──────┴──────┬───────────┬───────────┐
    │             │           │           │
    ▼             ▼           ▼           ▼
receive_raft  propose()  propose_conf  handle_peer
_message()                _change()     _discovery()
    │             │           │
    └─────────────┴───────────┴───────────┐
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  RaftCore             │
                              │  raw_node.step(msg)   │
                              │  raw_node.propose()   │
                              └───────────┬───────────┘
                                          │
                                          │ Raft 算法处理
                                          │
═══════════════════════════════════════════════════════════════════════════
阶段 2: Raft 处理 → 生成 Ready
═══════════════════════════════════════════════════════════════════════════
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  RawNode::ready()     │
                              └───────────┬───────────┘
                                          │
                                          ▼
                              ┌───────────────────────────────┐
                              │  Ready 结构                    │
                              ├───────────────────────────────┤
                              │  • messages              ──┐  │
                              │    (立即发送的消息)         │  │
                              │                           │  │
                              │  • persisted_messages   ──┤  │
                              │    (需要持久化后发送)      │  │
                              │                           │  │
                              │  • entries              ──┤  │
                              │    (需要持久化的日志)      │  │
                              │                           │  │
                              │  • committed_entries    ──┤  │
                              │    (需要应用的日志)        │  │
                              │                           │  │
                              │  • hard_state           ──┤  │
                              │    (需要持久化的状态)      │  │
                              │                           │  │
                              │  • snapshot             ──┘  │
                              │    (快照)                     │
                              └───────────┬───────────────────┘
                                          │
                                          │ 转换为
                                          │
                                          ▼
                              ┌───────────────────────────────┐
                              │  RaftOutput                   │
                              ├───────────────────────────────┤
                              │  outbound_messages:           │
                              │  [                            │
                              │    OutboundMessage {          │
                              │      to: 2,                   │
                              │      message: MsgAppend,      │
                              │      require_persistence: false│
                              │    },                         │
                              │    OutboundMessage {          │
                              │      to: 3,                   │
                              │      message: MsgAppend,      │
                              │      require_persistence: true │
                              │    }                          │
                              │  ]                            │
                              │                               │
                              │  persist_ops:                 │
                              │    - entries: [Entry{...}]    │
                              │    - hard_state: Some(...)    │
                              │    - must_sync: true          │
                              │                               │
                              │  committed_entries: [...]     │
                              └───────────┬───────────────────┘
                                          │
                                          │
═══════════════════════════════════════════════════════════════════════════
阶段 3: 输出处理（严格顺序）
═══════════════════════════════════════════════════════════════════════════
                                          │
                                          ▼
                              ┌───────────────────────────┐
                              │  OutputProcessor         │
                              └───────────┬───────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    │  Step 1: 发送立即消息  │  Step 2: 应用快照   │
                    │  (并行)              │                     │
                    │                     │                     │
        ┌───────────▼───────────┐         ▼                     │
        │  send_immediate_msgs  │   ┌──────────────┐            │
        │                       │   │ apply_       │            │
        │  for msg in messages  │   │ snapshot()   │            │
        │    if !require_persist│   └──────────────┘            │
        │      send(msg)        │                               │
        └───────────────────────┘                               │
                    │                                           │
                    └───────────────────┬───────────────────────┘
                                        │
                                        ▼
                            ┌────────────────────────┐
                            │  Step 3: 持久化         │
                            │  (同步，阻塞)           │
                            └────────────┬───────────┘
                                         │
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
                    ▼                    ▼                    ▼
          ┌──────────────────┐  ┌──────────────┐  ┌──────────────┐
          │ storage.append   │  │ storage.save │  │ storage.save │
          │ _entries()       │  │ _hard_state()│  │ _snapshot()  │
          └──────────────────┘  └──────────────┘  └──────────────┘
                    │                    │                    │
                    └────────────────────┼────────────────────┘
                                         │
                                         ▼
                                ┌─────────────────┐
                                │ storage.flush() │
                                │ (fsync)         │
                                └────────┬────────┘
                                         │
                    ┌────────────────────┴────────────────────┐
                    │                                         │
                    │  Step 4: 发送持久化消息                   │
                    │  (在持久化完成后)                         │
                    │                                         │
        ┌───────────▼───────────┐                             │
        │  send_persisted_msgs  │                             │
        │                       │                             │
        │  for msg in messages  │                             │
        │    if require_persist │                             │
        │      send(msg)        │                             │
        └───────────┬───────────┘                             │
                    │                                         │
                    └─────────────────────┬───────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │  Step 5: 应用日志      │
                              │  apply_entries()      │
                              └───────────┬───────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
          ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
          │ EntryNormal      │  │ EntryConf    │  │ storage.set_     │
          │ state_machine    │  │ Change       │  │ applied_index()  │
          │ .apply()         │  │ apply_cc()   │  │                  │
          └──────────────────┘  └──────────────┘  └──────────────────┘
                    │                     │                     │
                    └─────────────────────┴─────────────────────┘
                                          │
                                          ▼
                                    [ 完成 ]
```

---

### 4.2 对比：旧架构 vs 新架构

#### 旧架构的问题

```
❌ 混淆的旧架构：

外部消息 → handle_raft_message()  (接收)
              ↓
         raw_node.step()
              ↓
         handle_all_ready()
              ↓
         handle_messages()  (发送？？？名字一样！)
              ↓
         其他节点的 handle_raft_message()  (又接收？？？)

问题：
1. handle_raft_message 和 handle_messages 名字相似，但做相反的事
2. 不清楚是接收还是发送
3. 不清楚是处理消息还是处理日志
```

---

#### 新架构的清晰性

```
✅ 清晰的新架构：

外部消息 → receive_raft_message()  (明确：接收)
              ↓
         raw_node.step()
              ↓
         process_ready()  (明确：处理 Ready)
              ↓
         ├─ send_raft_messages()  (明确：发送)
         ├─ persist_data()  (明确：持久化)
         └─ apply_committed_entries()  (明确：应用)
              ↓
         其他节点的 receive_raft_message()  (对称！)

优点：
1. 函数名精确：receive/send/apply/persist
2. 流向清晰：输入 → 处理 → 输出
3. 职责明确：每个函数只做一件事
```

---

## 5. 详细设计方案

### 5.1 模块划分

```
crabmq/raftxv2/src/
├── lib.rs                    # 公共接口
├── node.rs                   # 顶层节点（删除）→ 拆分为：
│
├── core/
│   ├── mod.rs
│   ├── raft_core.rs          # Raft 核心封装
│   ├── ready_processor.rs    # Ready 处理逻辑
│   └── output_handler.rs     # 输出处理器
│
├── messaging/
│   ├── mod.rs
│   ├── router.rs             # 消息路由
│   ├── types.rs              # 消息类型定义
│   └── transport.rs          # 网络传输
│
├── storage/
│   ├── mod.rs
│   ├── sled_storage.rs       # Sled 存储实现
│   └── traits.rs             # Storage trait
│
├── state_machine/
│   ├── mod.rs
│   └── traits.rs             # StateMachine trait
│
└── utils/
    ├── mod.rs
    └── metrics.rs            # 监控指标
```

---

### 5.2 关键接口定义

#### Storage Trait

```rust
#[async_trait]
pub trait Storage: Send + Sync {
    /// 追加日志条目
    async fn append_entries(&self, entries: &[Entry]) -> Result<()>;

    /// 保存 HardState
    async fn save_hard_state(&self, hs: &HardState) -> Result<()>;

    /// 保存快照
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()>;

    /// 确保数据刷盘
    async fn flush(&self) -> Result<()>;

    /// 设置 applied index
    async fn set_applied_index(&self, index: u64) -> Result<()>;

    /// 获取 applied index
    async fn get_applied_index(&self) -> Result<u64>;
}
```

---

#### StateMachine Trait

```rust
#[async_trait]
pub trait StateMachine: Send + Sync {
    /// 应用日志条目到状态机
    async fn apply(&mut self, index: u64, data: &[u8]) -> Result<()>;

    /// 应用快照
    async fn apply_snapshot(&mut self, snapshot: &[u8]) -> Result<()>;

    /// 生成快照
    async fn create_snapshot(&self) -> Result<Vec<u8>>;

    /// 获取当前 applied index
    fn applied_index(&self) -> u64;
}
```

---

### 5.3 错误处理设计

```rust
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("Raft error: {0}")]
    Raft(#[from] raft::Error),

    #[error("State machine error: {0}")]
    StateMachine(String),

    #[error("Node not leader")]
    NotLeader { leader_id: Option<u64> },

    #[error("Proposal dropped")]
    ProposalDropped,
}

pub type Result<T> = std::result::Result<T, RaftError>;
```

---

## 6. 代码实现

### 6.1 核心类型定义

```rust
// messaging/types.rs

/// 应用层输入消息
#[derive(Debug, Clone)]
pub enum ApplicationInput {
    /// Raft 协议消息（从其他节点）
    RaftProtocol {
        from: u64,
        message: raft::Message,
    },

    /// 客户端提议
    ClientProposal {
        data: Vec<u8>,
        callback: Option<oneshot::Sender<Result<u64>>>,
    },

    /// 配置变更
    ConfigChange {
        change: ConfChangeRequest,
    },

    /// 节点发现
    PeerDiscovery {
        peer_id: u64,
        address: String,
    },
}

/// 配置变更请求
#[derive(Debug, Clone)]
pub enum ConfChangeRequest {
    V1(ConfChange),
    V2(ConfChangeV2),
}

/// 出站消息
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// 目标节点 ID
    pub to: u64,
    /// Raft 协议消息
    pub message: raft::Message,
    /// 是否需要先持久化再发送
    pub require_persistence: bool,
}

/// Raft 输出
#[derive(Debug, Default)]
pub struct RaftOutput {
    /// 出站消息
    pub outbound_messages: Vec<OutboundMessage>,
    /// 持久化操作
    pub persist_ops: PersistOperations,
    /// 已提交的日志条目
    pub committed_entries: Vec<Entry>,
    /// 快照
    pub snapshot: Option<Snapshot>,
}

/// 持久化操作
#[derive(Debug, Default)]
pub struct PersistOperations {
    /// 日志条目
    pub entries: Vec<Entry>,
    /// HardState
    pub hard_state: Option<HardState>,
    /// 快照
    pub snapshot: Option<Snapshot>,
    /// 提交索引
    pub commit_index: Option<u64>,
    /// 是否需要 fsync
    pub must_sync: bool,
}

impl PersistOperations {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.hard_state.is_none()
            && self.snapshot.is_none()
    }
}
```

---

### 6.2 RaftCore 实现

```rust
// core/raft_core.rs

use crate::messaging::types::*;
use crate::storage::Storage;
use raft::{RawNode, Ready};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RaftCore<S: Storage> {
    /// Raft 状态机
    raw_node: Mutex<RawNode<S>>,
    /// 存储引擎
    storage: Arc<S>,
    /// 已处理的 committed_entries 的最后索引
    commit_cursor: Mutex<u64>,
}

impl<S: Storage + 'static> RaftCore<S> {
    pub fn new(raw_node: RawNode<S>, storage: Arc<S>) -> Self {
        Self {
            raw_node: Mutex::new(raw_node),
            storage,
            commit_cursor: Mutex::new(0),
        }
    }

    /// 接收 Raft 协议消息
    pub async fn receive_raft_message(&self, msg: raft::Message) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        node.step(msg)?;
        Ok(())
    }

    /// 提议数据
    pub async fn propose(&self, data: Vec<u8>) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        node.propose(vec![], data)?;
        Ok(())
    }

    /// 提议配置变更
    pub async fn propose_conf_change(&self, cc: ConfChangeRequest) -> Result<()> {
        let mut node = self.raw_node.lock().await;
        match cc {
            ConfChangeRequest::V1(cc) => {
                node.propose_conf_change(vec![], cc)?;
            }
            ConfChangeRequest::V2(ccv2) => {
                node.propose_conf_change(vec![], ccv2)?;
            }
        }
        Ok(())
    }

    /// Tick
    pub async fn tick(&self) {
        let mut node = self.raw_node.lock().await;
        node.tick();
    }

    /// 检查是否有 Ready
    pub async fn has_ready(&self) -> bool {
        let node = self.raw_node.lock().await;
        node.has_ready()
    }

    /// 处理 Ready
    pub async fn process_ready(&self) -> Result<Option<RaftOutput>> {
        let mut node = self.raw_node.lock().await;

        if !node.has_ready() {
            return Ok(None);
        }

        let mut ready = node.ready();

        // 构造输出
        let mut output = RaftOutput::default();

        // 1. 提取出站消息
        output.outbound_messages = self.extract_outbound_messages(&ready);

        // 2. 提取持久化操作
        output.persist_ops = self.extract_persist_operations(&mut ready);

        // 3. 提取已提交的条目
        output.committed_entries = ready.take_committed_entries();

        // 4. 提取快照
        if !ready.snapshot().is_empty() {
            output.snapshot = Some(ready.snapshot().clone());
        }

        // 5. Advance
        let mut light_rd = node.advance(ready);

        // 6. 处理 light_ready
        if let Some(commit) = light_rd.commit_index() {
            output.persist_ops.commit_index = Some(commit);
        }

        // 合并 light_ready 的消息
        for msg in light_rd.take_messages() {
            output.outbound_messages.push(OutboundMessage {
                to: msg.to,
                message: msg,
                require_persistence: false,
            });
        }

        // 合并 light_ready 的 committed_entries
        output.committed_entries.extend(light_rd.take_committed_entries());

        // 7. Advance apply
        node.advance_apply();

        Ok(Some(output))
    }

    /// 提取出站消息
    fn extract_outbound_messages(&self, ready: &Ready) -> Vec<OutboundMessage> {
        let mut messages = Vec::new();

        // 普通消息（立即发送）
        for msg in ready.messages() {
            messages.push(OutboundMessage {
                to: msg.to,
                message: msg.clone(),
                require_persistence: false,
            });
        }

        // 持久化消息（需要先持久化）
        for msg in ready.persisted_messages() {
            messages.push(OutboundMessage {
                to: msg.to,
                message: msg.clone(),
                require_persistence: true,
            });
        }

        messages
    }

    /// 提取持久化操作
    fn extract_persist_operations(&self, ready: &mut Ready) -> PersistOperations {
        PersistOperations {
            entries: ready.take_entries(),
            hard_state: ready.hs().cloned(),
            snapshot: if ready.snapshot().is_empty() {
                None
            } else {
                Some(ready.snapshot().clone())
            },
            commit_index: None,  // 稍后从 light_ready 获取
            must_sync: ready.must_sync(),
        }
    }

    /// 获取节点状态
    pub async fn status(&self) -> raft::Status {
        let node = self.raw_node.lock().await;
        node.status()
    }

    /// 是否为 Leader
    pub async fn is_leader(&self) -> bool {
        let node = self.raw_node.lock().await;
        node.raft.state == raft::StateRole::Leader
    }
}
```

---

### 6.3 OutputProcessor 实现

```rust
// core/output_handler.rs

use crate::messaging::types::*;
use crate::messaging::transport::Transport;
use crate::storage::Storage;
use crate::state_machine::StateMachine;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct OutputProcessor<S, SM>
where
    S: Storage,
    SM: StateMachine,
{
    storage: Arc<S>,
    transport: Arc<Transport>,
    state_machine: Arc<Mutex<SM>>,
}

impl<S, SM> OutputProcessor<S, SM>
where
    S: Storage + 'static,
    SM: StateMachine + 'static,
{
    pub fn new(
        storage: Arc<S>,
        transport: Arc<Transport>,
        state_machine: Arc<Mutex<SM>>,
    ) -> Self {
        Self {
            storage,
            transport,
            state_machine,
        }
    }

    /// 处理 Raft 输出（按正确顺序）
    pub async fn process(&self, output: RaftOutput) -> Result<()> {
        // Phase 1: 发送立即消息（并行）
        self.send_immediate_messages(&output).await;

        // Phase 2: 应用快照
        if let Some(snapshot) = &output.snapshot {
            self.apply_snapshot(snapshot).await?;
        }

        // Phase 3: 持久化（同步，确保完成）
        if !output.persist_ops.is_empty() {
            self.persist(&output.persist_ops).await?;
        }

        // Phase 4: 发送持久化消息（在持久化后）
        self.send_persisted_messages(&output).await;

        // Phase 5: 应用已提交的日志
        if !output.committed_entries.is_empty() {
            self.apply_committed_entries(&output.committed_entries).await?;
        }

        Ok(())
    }

    /// 发送立即消息
    async fn send_immediate_messages(&self, output: &RaftOutput) {
        let immediate_msgs: Vec<_> = output
            .outbound_messages
            .iter()
            .filter(|m| !m.require_persistence)
            .collect();

        if immediate_msgs.is_empty() {
            return;
        }

        // 并行发送
        let tasks: Vec<_> = immediate_msgs
            .into_iter()
            .map(|m| {
                let transport = self.transport.clone();
                let msg = m.clone();
                tokio::spawn(async move {
                    if let Err(e) = transport.send(msg.to, &msg.message).await {
                        error!("Failed to send message to {}: {:?}", msg.to, e);
                    }
                })
            })
            .collect();

        // 等待所有发送完成
        for task in tasks {
            let _ = task.await;
        }
    }

    /// 持久化数据
    async fn persist(&self, ops: &PersistOperations) -> Result<()> {
        // 1. 持久化日志条目
        if !ops.entries.is_empty() {
            debug!("Persisting {} entries", ops.entries.len());
            self.storage.append_entries(&ops.entries).await?;
        }

        // 2. 持久化 HardState
        if let Some(hs) = &ops.hard_state {
            debug!("Persisting HardState: {:?}", hs);
            self.storage.save_hard_state(hs).await?;
        }

        // 3. 持久化快照
        if let Some(snapshot) = &ops.snapshot {
            debug!("Persisting snapshot at index {}", snapshot.get_metadata().index);
            self.storage.save_snapshot(snapshot).await?;
        }

        // 4. 更新 commit index
        if let Some(commit) = ops.commit_index {
            debug!("Updating commit index to {}", commit);
            self.storage.set_commit_index(commit).await?;
        }

        // 5. 确保刷盘
        if ops.must_sync {
            debug!("Flushing storage (fsync)");
            self.storage.flush().await?;
        }

        Ok(())
    }

    /// 发送持久化消息
    async fn send_persisted_messages(&self, output: &RaftOutput) {
        let persisted_msgs: Vec<_> = output
            .outbound_messages
            .iter()
            .filter(|m| m.require_persistence)
            .collect();

        if persisted_msgs.is_empty() {
            return;
        }

        debug!("Sending {} persisted messages", persisted_msgs.len());

        // 串行发送（确保顺序）
        for msg in persisted_msgs {
            if let Err(e) = self.transport.send(msg.to, &msg.message).await {
                error!("Failed to send persisted message to {}: {:?}", msg.to, e);
            }
        }
    }

    /// 应用快照
    async fn apply_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        debug!("Applying snapshot at index {}", index);

        let mut sm = self.state_machine.lock().await;
        sm.apply_snapshot(snapshot.get_data()).await?;

        // 更新 applied index
        self.storage.set_applied_index(index).await?;

        Ok(())
    }

    /// 应用已提交的日志
    async fn apply_committed_entries(&self, entries: &[Entry]) -> Result<()> {
        debug!("Applying {} committed entries", entries.len());

        for entry in entries {
            // 空条目（Leader 切换时产生）
            if entry.data.is_empty() {
                self.storage.set_applied_index(entry.index).await?;
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    self.apply_normal_entry(entry).await?;
                }
                EntryType::EntryConfChange => {
                    self.apply_conf_change(entry).await?;
                }
                EntryType::EntryConfChangeV2 => {
                    self.apply_conf_change_v2(entry).await?;
                }
            }

            // 持久化 applied index
            self.storage.set_applied_index(entry.index).await?;
        }

        Ok(())
    }

    /// 应用普通日志条目
    async fn apply_normal_entry(&self, entry: &Entry) -> Result<()> {
        debug!("Applying normal entry at index {}", entry.index);

        let mut sm = self.state_machine.lock().await;
        sm.apply(entry.index, &entry.data).await?;

        Ok(())
    }

    /// 应用配置变更
    async fn apply_conf_change(&self, entry: &Entry) -> Result<()> {
        debug!("Applying ConfChange at index {}", entry.index);

        let mut cc = ConfChange::default();
        cc.merge_from_bytes(&entry.data)
            .map_err(|e| RaftError::StateMachine(format!("Failed to parse ConfChange: {}", e)))?;

        // TODO: 实际的配置变更逻辑（添加/删除节点）

        Ok(())
    }

    /// 应用配置变更 V2
    async fn apply_conf_change_v2(&self, entry: &Entry) -> Result<()> {
        debug!("Applying ConfChangeV2 at index {}", entry.index);

        let mut ccv2 = ConfChangeV2::default();
        ccv2.merge_from_bytes(&entry.data)
            .map_err(|e| RaftError::StateMachine(format!("Failed to parse ConfChangeV2: {}", e)))?;

        // TODO: 实际的配置变更逻辑

        Ok(())
    }
}
```

---

### 6.4 MessageRouter 实现

```rust
// messaging/router.rs

use crate::core::raft_core::RaftCore;
use crate::core::output_handler::OutputProcessor;
use crate::messaging::types::*;
use crate::messaging::transport::Transport;
use std::sync::Arc;
use tokio::time::{self, Duration};

pub struct MessageRouter<S, SM>
where
    S: Storage + 'static,
    SM: StateMachine + 'static,
{
    node_id: u64,
    raft_core: Arc<RaftCore<S>>,
    output_processor: Arc<OutputProcessor<S, SM>>,
    transport: Arc<Transport>,
    tick_interval: Duration,
}

impl<S, SM> MessageRouter<S, SM>
where
    S: Storage + 'static,
    SM: StateMachine + 'static,
{
    pub fn new(
        node_id: u64,
        raft_core: Arc<RaftCore<S>>,
        output_processor: Arc<OutputProcessor<S, SM>>,
        transport: Arc<Transport>,
        tick_interval: Duration,
    ) -> Self {
        Self {
            node_id,
            raft_core,
            output_processor,
            transport,
            tick_interval,
        }
    }

    /// 启动消息循环
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut ticker = time::interval(self.tick_interval);
        let mut message_rx = self.transport.message_receiver();

        loop {
            tokio::select! {
                // 接收外部消息
                Some(msg) = message_rx.recv() => {
                    if let Err(e) = self.route_message(msg).await {
                        error!("Failed to route message: {:?}", e);
                    }
                }

                // 定时器 tick
                _ = ticker.tick() => {
                    self.raft_core.tick().await;
                }
            }

            // 处理 Raft 输出
            if let Err(e) = self.process_raft_output().await {
                error!("Failed to process raft output: {:?}", e);
            }
        }
    }

    /// 路由消息到正确的处理器
    async fn route_message(&self, msg: ApplicationInput) -> Result<()> {
        match msg {
            ApplicationInput::RaftProtocol { message, .. } => {
                self.raft_core.receive_raft_message(message).await?;
            }

            ApplicationInput::ClientProposal { data, callback } => {
                match self.raft_core.propose(data).await {
                    Ok(_) => {
                        if let Some(cb) = callback {
                            let _ = cb.send(Ok(0));  // TODO: 返回实际的 index
                        }
                    }
                    Err(e) => {
                        if let Some(cb) = callback {
                            let _ = cb.send(Err(e));
                        }
                    }
                }
            }

            ApplicationInput::ConfigChange { change } => {
                self.raft_core.propose_conf_change(change).await?;
            }

            ApplicationInput::PeerDiscovery { peer_id, address } => {
                // TODO: 处理节点发现
                debug!("Discovered peer {} at {}", peer_id, address);
            }
        }

        Ok(())
    }

    /// 处理 Raft 输出
    async fn process_raft_output(&self) -> Result<()> {
        if let Some(output) = self.raft_core.process_ready().await? {
            self.output_processor.process(output).await?;
        }
        Ok(())
    }
}
```

---

### 6.5 顶层 Node API

```rust
// lib.rs 或 node_api.rs

use crate::core::raft_core::RaftCore;
use crate::core::output_handler::OutputProcessor;
use crate::messaging::router::MessageRouter;
use crate::messaging::types::*;
use std::sync::Arc;

/// Raft 节点 - 顶层 API
pub struct RaftNode<S, SM>
where
    S: Storage + 'static,
    SM: StateMachine + 'static,
{
    node_id: u64,
    raft_core: Arc<RaftCore<S>>,
    router: Arc<MessageRouter<S, SM>>,
}

impl<S, SM> RaftNode<S, SM>
where
    S: Storage + 'static,
    SM: StateMachine + 'static,
{
    /// 创建新节点
    pub fn new(
        node_id: u64,
        raw_node: raft::RawNode<S>,
        storage: Arc<S>,
        state_machine: Arc<Mutex<SM>>,
        transport: Arc<Transport>,
    ) -> Self {
        let raft_core = Arc::new(RaftCore::new(raw_node, storage.clone()));
        let output_processor = Arc::new(OutputProcessor::new(
            storage,
            transport.clone(),
            state_machine,
        ));
        let router = Arc::new(MessageRouter::new(
            node_id,
            raft_core.clone(),
            output_processor,
            transport,
            Duration::from_millis(100),
        ));

        Self {
            node_id,
            raft_core,
            router,
        }
    }

    /// 启动节点
    pub async fn run(self) -> Result<()> {
        self.router.run().await
    }

    /// 提议数据（客户端 API）
    pub async fn propose(&self, data: Vec<u8>) -> Result<u64> {
        self.raft_core.propose(data).await?;
        // TODO: 返回实际的 index
        Ok(0)
    }

    /// 添加节点
    pub async fn add_peer(&self, peer_id: u64, address: String) -> Result<()> {
        let cc = ConfChange {
            change_type: ConfChangeType::AddNode,
            node_id: peer_id,
            context: address.into_bytes().into(),
            ..Default::default()
        };

        self.raft_core.propose_conf_change(ConfChangeRequest::V1(cc)).await
    }

    /// 删除节点
    pub async fn remove_peer(&self, peer_id: u64) -> Result<()> {
        let cc = ConfChange {
            change_type: ConfChangeType::RemoveNode,
            node_id: peer_id,
            ..Default::default()
        };

        self.raft_core.propose_conf_change(ConfChangeRequest::V1(cc)).await
    }

    /// 获取节点状态
    pub async fn status(&self) -> raft::Status {
        self.raft_core.status().await
    }

    /// 是否为 Leader
    pub async fn is_leader(&self) -> bool {
        self.raft_core.is_leader().await
    }
}
```

---

## 7. 性能与健壮性保证

### 7.1 性能优化

#### 7.1.1 并行发送消息

```rust
// 立即消息并行发送（不阻塞）
async fn send_immediate_messages(&self, output: &RaftOutput) {
    let tasks: Vec<_> = output
        .outbound_messages
        .iter()
        .filter(|m| !m.require_persistence)
        .map(|m| {
            let transport = self.transport.clone();
            let msg = m.clone();
            tokio::spawn(async move {
                transport.send(msg.to, &msg.message).await
            })
        })
        .collect();

    futures::future::join_all(tasks).await;
}
```

**优势：**
- 多个消息并行发送，减少延迟
- 不阻塞主流程

---

#### 7.1.2 批量持久化

```rust
// 批量写入，减少 I/O
async fn persist(&self, ops: &PersistOperations) -> Result<()> {
    // 构造批量写入
    let mut batch = WriteBatch::new();

    if !ops.entries.is_empty() {
        batch.append_entries(&ops.entries);
    }

    if let Some(hs) = &ops.hard_state {
        batch.save_hard_state(hs);
    }

    // 一次性写入
    self.storage.write_batch(batch).await?;

    // 根据需要 fsync
    if ops.must_sync {
        self.storage.flush().await?;
    }

    Ok(())
}
```

**优势：**
- 减少 I/O 次数
- 提高吞吐量

---

### 7.2 健壮性保证

#### 7.2.1 错误重试机制

```rust
// 带重试的消息发送
async fn send_with_retry(
    &self,
    to: u64,
    message: &raft::Message,
    max_retries: usize,
) -> Result<()> {
    let mut retries = 0;

    loop {
        match self.transport.send(to, message).await {
            Ok(_) => return Ok(()),
            Err(e) if retries < max_retries => {
                warn!("Failed to send message to {}, retry {}/{}: {:?}",
                      to, retries + 1, max_retries, e);
                retries += 1;
                tokio::time::sleep(Duration::from_millis(100 * (1 << retries))).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
```

---

#### 7.2.2 持久化失败处理

```rust
async fn persist(&self, ops: &PersistOperations) -> Result<()> {
    match self.storage.write_batch(ops).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("FATAL: Failed to persist data: {:?}", e);
            error!("Data: {:?}", ops);

            // 选项 1: Panic（确保不会继续）
            panic!("Cannot continue without persisting critical data");

            // 选项 2: 返回错误（调用者决定）
            // Err(e.into())
        }
    }
}
```

---

#### 7.2.3 监控与指标

```rust
// 添加 metrics
pub struct RaftMetrics {
    pub proposals: Counter,
    pub messages_sent: Counter,
    pub messages_received: Counter,
    pub persist_duration: Histogram,
    pub apply_duration: Histogram,
}

impl OutputProcessor {
    async fn persist(&self, ops: &PersistOperations) -> Result<()> {
        let start = Instant::now();

        // 持久化逻辑...

        self.metrics.persist_duration.observe(start.elapsed().as_secs_f64());
        Ok(())
    }
}
```

---

### 7.3 测试策略

#### 7.3.1 单元测试

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_immediate_messages_sent_before_persistence() {
        let (processor, mock_storage, mock_transport) = setup_test();

        let output = RaftOutput {
            outbound_messages: vec![
                OutboundMessage {
                    to: 2,
                    message: create_test_message(),
                    require_persistence: false,
                },
            ],
            persist_ops: PersistOperations {
                entries: vec![create_test_entry()],
                must_sync: true,
                ..Default::default()
            },
            ..Default::default()
        };

        processor.process(output).await.unwrap();

        // 验证消息在持久化前发送
        assert!(mock_transport.sent_before_persist(2));
    }

    #[tokio::test]
    async fn test_persisted_messages_sent_after_persistence() {
        let (processor, mock_storage, mock_transport) = setup_test();

        let output = RaftOutput {
            outbound_messages: vec![
                OutboundMessage {
                    to: 3,
                    message: create_test_message(),
                    require_persistence: true,
                },
            ],
            persist_ops: PersistOperations {
                entries: vec![create_test_entry()],
                must_sync: true,
                ..Default::default()
            },
            ..Default::default()
        };

        processor.process(output).await.unwrap();

        // 验证消息在持久化后发送
        assert!(mock_transport.sent_after_persist(3));
        assert!(mock_storage.flush_called());
    }
}
```

---

#### 7.3.2 集成测试

```rust
#[tokio::test]
async fn test_end_to_end_proposal() {
    // 创建 3 节点集群
    let cluster = TestCluster::new(3).await;

    // 等待选举完成
    cluster.wait_for_leader().await;

    // 提议数据
    let leader = cluster.leader();
    let index = leader.propose(b"test data".to_vec()).await.unwrap();

    // 等待提交
    cluster.wait_for_commit(index).await;

    // 验证所有节点都应用了
    for node in cluster.nodes() {
        let sm = node.state_machine();
        assert_eq!(sm.get(index), Some(b"test data"));
    }
}
```

---

## 总结

### 重构前后对比

| 维度 | 旧架构 | 新架构 |
|-----|-------|-------|
| **命名清晰度** | ❌ handle_messages 做相反的事 | ✅ receive/send/apply/persist 明确 |
| **职责分离** | ❌ Node 承担所有责任 | ✅ Router/Core/Processor 各司其职 |
| **消息分层** | ❌ 三种消息混用 | ✅ 清晰的类型系统区分 |
| **流程清晰** | ❌ 难以理解的循环 | ✅ 线性流程：输入→处理→输出 |
| **测试性** | ❌ 难以测试 | ✅ 每层可独立测试 |
| **健壮性** | ⚠️ 错误处理不足 | ✅ 完善的错误处理和重试 |
| **性能** | ⚠️ 串行发送 | ✅ 并行发送 + 批量持久化 |

---

### 核心改进

1. **消息类型清晰**
   - `ApplicationInput` - 外部输入
   - `RaftOutput` - Raft 输出
   - `OutboundMessage` - 明确是否需要持久化

2. **函数命名精确**
   - `receive_raft_message()` - 接收
   - `send_raft_messages()` - 发送
   - `apply_committed_entries()` - 应用
   - `persist()` - 持久化

3. **流程严格顺序**
   - 立即消息 → 持久化 → 持久化消息 → 应用日志
   - 确保 Raft 安全性

4. **架构清晰分层**
   - Router - 消息路由
   - Core - Raft 核心
   - Processor - 输出处理
   - Storage - 数据持久化

---

**这就是清晰、合理、高效、健壮的 Raft 架构！**
