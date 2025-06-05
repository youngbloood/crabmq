# Storage to Disk

## Bench

- Test local disk storage rate

```shell
cargo test run_flush_benchmark --release -- --show-output --nocapture                
```

- Test with Flamegraph

```shell
cargo flamegraph --unit-test --release -- disk::writer::test::run_flush_benchmark
```

Ensure you had installed `flamegraph` by

```shell
cargo install flamegraph
```
