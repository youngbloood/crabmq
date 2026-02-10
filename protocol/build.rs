use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")

        .out_dir(out_dir) // Explicitly set output directory if needed, otherwise default is used
        .compile_protos(
            &[
                "proto/v1/common.proto",
                "proto/v1/client_coo.proto",
                "proto/v1/broker_coo.proto",
                "proto/v1/client_broker.proto",
                "proto/v1/coo_raft.proto",
                "proto/v1/err.proto",
            ],
            &["proto/v1"], // Include path
        )?;

    Ok(())
}
