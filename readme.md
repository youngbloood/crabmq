# CrabMQ

A distribution Message Queue written by Rust.

## Usage

### Step 1

Start a `crabmqd` server.

```shell
cd crabmqd && cargo run
```

### Step 2

Build a Client.

```shell
cargo build --bin client
```

Open a TCP connection in another terminal.

```shell
./target/debug/client 127.0.0.1:3890
```

Then we enter the interactive terminal. And we build the message to send to `crabmqd`.

```shell
head --action pub
msg --body aaaa --persist --ack
send
```
