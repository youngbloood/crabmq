fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("构建所有的 proto 文件");

    tonic_build::configure().compile_protos(
        &[
            "../protos/broker_coo.proto",
            "../protos/client_broker.proto",
            "../protos/client_coo.proto",
            "../protos/coo_raft.proto",
            "../protos/common.proto",
        ],
        &["../protos"],
    )?;

    Ok(())
}
