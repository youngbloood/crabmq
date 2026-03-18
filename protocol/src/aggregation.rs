#[derive(Debug, Clone)]

pub enum CooRaftProposeMessage {
    #[cfg(feature = "v1")]
    ProposeV1(crate::pbv1::CooRaftProposeMessage),
}
