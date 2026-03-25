#[derive(Debug, Clone)]

pub enum Event {
    #[cfg(feature = "v1")]
    ProposeV1(crate::pbv1::Event),
}

pub enum CooRaftProposeMessage {
    #[cfg(feature = "v1")]
    ProposeV1(crate::pbv1::CooRaftProposeMessage),
}
