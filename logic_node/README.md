# LogicNode Broker-Coo 通信设计

## 概述

LogicNode 中的 broker 与 coo 通信支持两种模式：
1. **本地通信**：当同 logic_node 内的 coo 是 leader 时，broker 与 coo 使用内部通信
2. **外部通信**：当同 logic_node 内的 coo 不是 leader 时，broker 与外部 leader coo 使用 gRPC 通信

## 设计目标

1. **性能优化**：同 logic_node 内的 broker 与 coo 通信避免网络开销
2. **动态切换**：根据 coo leader 状态自动切换通信方式
3. **容错性**：支持 coo leader 切换时的无缝通信转换

## 核心组件

### CommunicationMode 枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
enum CommunicationMode {
    Local,    // 本地通信
    External, // 外部通信
}
```

### 主要方法

#### `handle_broker_coo_communication`
- 处理有 broker 和 coo 的 logic_node
- 动态监控 coo leader 状态
- 根据状态切换通信模式

#### `start_local_communication`
- 启动本地通信模式
- 使用 `broker.get_state_reciever()` 和 `coo.broker_pull()` 进行内部通信
- 监控 leader 状态变化，自动切换到外部通信

#### `start_external_communication`
- 启动外部通信模式
- 使用 gRPC 与外部 leader coo 通信
- 监控切换到本地通信的信号

## 通信流程

### 1. 初始化阶段
```
LogicNode 启动
    ↓
检查配置（是否有 broker 和 coo）
    ↓
启动模块（broker 和 coo 服务）
    ↓
进入通信循环
```

### 2. 通信模式判断
```
检查当前 coo 是否为 leader
    ↓
是 leader → 使用本地通信
    ↓
不是 leader → 使用外部通信
```

### 3. 模式切换
```
检测到 leader 状态变化
    ↓
停止当前通信模式
    ↓
清理资源（unleash_state_reciever）
    ↓
启动新的通信模式
```

## 内部通信机制

### Broker → Coo（状态报告）
```rust
// 获取 broker 状态接收器
let mut state_recv = broker.get_state_reciever("logic_node".to_string());

// 在循环中接收并处理状态
state_recv.recv() => {
    coo.apply_broker_report(state.unwrap());
}
```

### Coo → Broker（Topic 信息推送）
```rust
// 获取 coo 推送接收器
let mut pull_recv = coo.broker_pull(broker.get_id()).await?;

// 在循环中接收并处理 topic 信息
pull_recv.recv() => {
    broker.apply_topic_infos(tl);
}
```

## 外部通信机制

### gRPC 服务
- **Report**：broker 向 coo 报告状态
- **Pull**：coo 向 broker 推送 topic 信息
- **SyncConsumerAssignments**：同步消费者组分配

### 智能客户端
- 使用 `SmartClient` 自动处理 leader 切换
- 支持端点自动刷新
- 错误处理和重试机制

## 状态监控

### Watcher 机制
```rust
fn start_watcher(&self, tx_watcher: watch::Sender<bool>) {
    // 每秒检查 coo leader 状态
    // 只在状态变化时发送通知
}
```

### 状态变化处理
- 本地通信检测到 `NotLeader` 错误时自动切换
- 外部通信通过 watcher 信号切换到本地通信
- 支持优雅的资源清理

## 优势

1. **性能提升**：本地通信避免网络延迟和序列化开销
2. **自动切换**：无需手动干预，根据 leader 状态自动切换
3. **资源管理**：完善的资源清理机制，避免内存泄漏
4. **容错性**：支持各种异常情况的处理
5. **可观测性**：详细的日志记录，便于调试和监控

## 使用示例

```rust
let mut logic_node = LogicNode::builder()
    .id(1)
    .broker(broker)
    .coo(coo)
    .build();

logic_node.run(coo_leader_addr).await?;
```

## 注意事项

1. **资源清理**：切换通信模式时会自动清理相关资源
2. **错误处理**：网络错误和 leader 切换错误都有相应的处理机制
3. **性能监控**：可以通过日志观察通信模式切换的频率和效果
4. **配置调优**：可以根据实际需求调整检查间隔和超时时间
