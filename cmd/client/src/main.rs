use anyhow::Result;
use grpcx::{
    clientcoosvc::{self, PullReq, client_coo_service_client::ClientCooServiceClient},
    smart_client::SmartClient,
};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Args {
    /// Coordinator 模块自身监听的 grpc 地址
    #[structopt(short = "c", long = "coo", default_value = "")]
    coo: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args();

    let mut client = ClientCooServiceClient::connect(args.coo)
        .await
        .expect("connect to dst failed");
    let topic_list = client
        .pull(PullReq::default())
        .await
        .expect("invoke pull failed");

    let mut resp_strm = topic_list.into_inner();
    while let Ok(Some(tl)) = resp_strm.message().await {
        println!("topic_list = {:?}", tl);
    }

    Ok(())
}
