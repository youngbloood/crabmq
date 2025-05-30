pub mod smart_client;
pub mod topic_meta;

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
