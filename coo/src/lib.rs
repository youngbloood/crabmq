pub mod coo;
mod event_bus;
mod partition;
mod raftx;

use grpcx::brokercoosvc;
struct BrokerNode {
    state: brokercoosvc::BrokerState,
}

struct ClientNode {}

#[cfg(test)]
mod test {}
