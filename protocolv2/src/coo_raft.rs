use std::collections::HashMap;

pub struct CooRaftGetMetaRequest {
    pub id: u64,
}

pub struct CooRaftGetMetaResponse {
    pub id: u64,
    pub raft_addr: String,
    pub meta: HashMap<String, String>,
}

// 支持 ConfChange 和 ConfChangeV2
pub struct CooRaftConfChangeRequest {
    pub version: u8,
    pub message: Vec<u8>,
}
