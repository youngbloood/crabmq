use std::any::Any;

use crate::EnDecoder;
use super::{
    BROKER_COO_HEARTBEAT_REQUEST_INDEX, BROKER_COO_HEARTBEAT_RESPONSE_INDEX,
};

pub use crate::pbv1::{BrokerCooHeartbeatRequest, BrokerCooHeartbeatResponse};

impl EnDecoder for BrokerCooHeartbeatRequest {
    fn index(&self) -> u16 {
        BROKER_COO_HEARTBEAT_REQUEST_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EnDecoder for BrokerCooHeartbeatResponse {
    fn index(&self) -> u16 {
        BROKER_COO_HEARTBEAT_RESPONSE_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
