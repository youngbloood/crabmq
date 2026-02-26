use std::any::Any;

use crate::EnDecoder;
use super::{
    COO_RAFT_CONF_CHANGE_REQUEST_INDEX, COO_RAFT_GET_META_REQUEST_INDEX,
    COO_RAFT_GET_META_RESPONSE_INDEX, COO_RAFT_ORIGIN_MESSAGE_INDEX,
    COO_RAFT_PROPOSE_MESSAGE_INDEX,
};

pub use crate::pbv1::{
    ConfChangeVersion, CooRaftConfChangeRequest, CooRaftGetMetaRequest, CooRaftGetMetaResponse,
    CooRaftOriginMessage, CooRaftProposeMessage, CooRaftProposeType,
};

impl EnDecoder for CooRaftGetMetaRequest {
    fn index(&self) -> u16 {
        COO_RAFT_GET_META_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooRaftGetMetaResponse {
    fn index(&self) -> u16 {
        COO_RAFT_GET_META_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooRaftConfChangeRequest {
    fn index(&self) -> u16 {
        COO_RAFT_CONF_CHANGE_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooRaftOriginMessage {
    fn index(&self) -> u16 {
        COO_RAFT_ORIGIN_MESSAGE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooRaftProposeMessage {
    fn index(&self) -> u16 {
        COO_RAFT_PROPOSE_MESSAGE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
