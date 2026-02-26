#[derive(Debug, Clone)]
pub enum Event {
    EventV1(crate::pbv1::Event),
}

pub enum CooRaftProposeMessage {
    ProposeV1(crate::pbv1::CooRaftProposeMessage),
}
