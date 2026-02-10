use std::any::Any;

use crate::{
    CLIENT_COO_ADD_PARTITION_REQUEST_INDEX, CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX,
    CLIENT_COO_AUTH_REQUEST_INDEX, CLIENT_COO_AUTH_RESPONSE_INDEX,
    CLIENT_COO_HEARTBEAT_REQUEST_INDEX, CLIENT_COO_HEARTBEAT_RESPONSE_INDEX,
    CLIENT_COO_NEW_TOPIC_REQUEST_INDEX, CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX,
    CLIENT_COO_SUB_REQUEST_INDEX, CLIENT_COO_SUB_RESPONSE_INDEX, EnDecoder,
};

pub use crate::pbv1::{
    ClientCooAddPartitionRequest, ClientCooAuthRequest, ClientCooAuthResponse,
    ClientCooCommitRequest, ClientCooHeartbeatRequest, ClientCooNewTopicRequest,
    ClientCooSubOption, ClientCooSubRequest, ClientCooSubTopic, CooClientAddPartitionResponse,
    CooClientCommitResponse, CooClientHeartbeatResponse, CooClientNewTopicResponse,
    CooClientSubResponse,
};

impl EnDecoder for ClientCooAuthRequest {
    fn index(&self) -> u16 {
        CLIENT_COO_AUTH_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for ClientCooAuthResponse {
    fn index(&self) -> u16 {
        CLIENT_COO_AUTH_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for ClientCooHeartbeatRequest {
    fn index(&self) -> u16 {
        CLIENT_COO_HEARTBEAT_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooClientHeartbeatResponse {
    fn index(&self) -> u16 {
        CLIENT_COO_HEARTBEAT_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for ClientCooNewTopicRequest {
    fn index(&self) -> u16 {
        CLIENT_COO_NEW_TOPIC_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooClientNewTopicResponse {
    fn index(&self) -> u16 {
        CLIENT_COO_NEW_TOPIC_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for ClientCooAddPartitionRequest {
    fn index(&self) -> u16 {
        CLIENT_COO_ADD_PARTITION_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooClientAddPartitionResponse {
    fn index(&self) -> u16 {
        CLIENT_COO_ADD_PARTITION_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for ClientCooSubRequest {
    fn index(&self) -> u16 {
        CLIENT_COO_SUB_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for CooClientSubResponse {
    fn index(&self) -> u16 {
        CLIENT_COO_SUB_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
