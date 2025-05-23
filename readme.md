# CrabMQ

A Distribution Message Queue written by Rust.

## Usage

### Build

```shell
cargo clean && cd ./cmd/crabmqd && cargo build
```

### Start CrabMQ Cluster

Start First LogicNode

```shell
RUST_LOG=info ./target/debug/crabmqd --id 1 --coo-raft 127.0.0.1:17001  -c 127.0.0.1:16001  -b 127.0.0.1:15001 
```

Start Second LogicNode and Join the First LogicNode

```shell
RUST_LOG=info ./target/debug/crabmqd --id 2 --coo-raft 127.0.0.1:17002 -c 127.0.0.1:16002 -b 127.0.0.1:15002 --raft-leader 127.0.0.1:17001 --coo-leader 127.0.0.1:16001
```

Start Third LogicNode and Join the First LogicNode To Become Cluster

```shell
RUST_LOG=info ./target/debug/crabmqd --id 3 --coo-raft 127.0.0.1:17003 -c 127.0.0.1:16003 -b 127.0.0.1:15003 --raft-leader 127.0.0.1:17001 --coo-leader 127.0.0.1:16001
```

...
