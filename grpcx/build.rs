fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("构建所有的 proto 文件");

    tonic_build::configure()
        // .message_attribute(path, attribute)
        .type_attribute("../protos/broker_coo.proto.PullReq", "#[derive(Default, Clone, Serialize, Deserialize)]")
        .compile_protos(
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
