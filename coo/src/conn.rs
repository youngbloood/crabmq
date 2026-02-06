pub struct Conn {
    pub t: Transporter,
    pub role: ConnRole,
}

impl Conn {
    pub fn new(t: Transporter) -> Self {
        Self {
            t,
            role: ConnRole::Unknown,
        }
    }
}

pub enum ConnRole {
    Unknown,
    Client,
    Broker,
    Cli,
    Web,
}
