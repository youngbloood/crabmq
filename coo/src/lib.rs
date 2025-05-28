pub mod config;
pub mod coo;
mod event_bus;
mod partition;
mod raftx;

pub use config::*;
use grpcx::brokercoosvc;
struct BrokerNode {
    state: brokercoosvc::BrokerState,
}

struct ClientNode {}

pub trait Filter {}

#[cfg(test)]
mod test {}
