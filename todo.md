# TODO List

## Action

- [x] action: pub
- [x] action: sub
- [ ] action: fin
- [ ] action: rdy
- [ ] action: req
- [ ] action: nop
- [ ] action: touch
- [ ] action: cls
- [ ] action: auth

## Feature

- [ ] ephemeral topic

## Protocol

- [ ] protocol: 延时消息支持设定过期时间

## Message

- [ ] 引入[rs-raft](https://github.com/tikv/raft-rs) 实现消息的分布式存储
- [ ] 实现消息的分片存储

## Tests

- [ ] Unit Test
- [ ] **吞吐量测试**：测量 MQ 在单位时间内能够处理的消息数量。可以使用工具或编写测试脚本来发送大量的消息，并记录发送和接收的时间，计算吞吐量。
- [ ] **延迟测试**：评估 MQ 传递消息的延迟。发送一系列消息，并测量从发送到接收的时间延迟。可以分析延迟的分布情况，包括最小值、最大值和平均值。、
- [ ] **可靠性测试**：验证 MQ 在面对网络故障、节点故障或其他异常情况时是否能够可靠地传递消息。可以模拟故障情况，检查消息是否丢失或重复。
- [ ] **扩展性测试**：考察 MQ 在处理大量并发连接和消息时的性能表现。可以逐渐增加并发连接数和消息量，观察 MQ 的响应时间和资源使用情况。
- [ ] **持久化测试**：如果 MQ 支持消息持久化，测试其在存储和恢复消息方面的性能。可以模拟存储介质故障，验证消息是否能够正确恢复。
- [ ] **兼容性测试**：确保 MQ 能够与不同的客户端和应用程序进行正常交互。测试与各种编程语言和框架的集成，以及与其他系统的兼容性。
- [ ] **压力测试**：对 MQ 施加高负载，观察其在极限情况下的性能和稳定性。可以使用多台机器或并发线程来模拟大量的消息发送和接收。
- [ ] **资源消耗测试**：监测 MQ 在运行过程中对系统资源（如内存、CPU、磁盘 I/O 等）的消耗情况。确保 MQ 在高负载下不会过度消耗资源。
- [ ] **可管理性测试**：评估 MQ 的管理界面和工具的易用性，包括监控、配置、故障排除等功能。
- [ ] **比较测试**：将待测试的 MQ 与其他类似的 MQ 进行比较，以了解其在性能、功能和特性方面的优势和劣势。

## Client

- [ ] rust client
- [ ] go client

## Web

- [ ] 官网
- [ ] 监控

## K8s

- [ ] Docker Image
- [ ] Operator
- [ ] CNI
- [ ] CSI?
