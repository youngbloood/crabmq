use protocolv2::EnDecoder;

pub struct Command {
    pub addr: String,
    pub index: u16,
    pub endecoder: Box<dyn EnDecoder>,
}
