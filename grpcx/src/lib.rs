// mod leader;
// TODO: 实现grpc智能客户端，链接至 coo 集群支持自动切换至 leader 节点
pub mod smart_client;
pub mod brokercoosvc {
    tonic::include_proto!("brokercoosvc");
}

pub mod clientcoosvc {
    tonic::include_proto!("clientcoosvc");
}

pub mod commonsvc {
    tonic::include_proto!("commonsvc");
}

pub mod cooraftsvc {
    tonic::include_proto!("cooraftsvc");
}

pub mod clientbrokersvc {
    tonic::include_proto!("clientbrokersvc");
}
