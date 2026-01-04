use tokio::net::tcp::OwnedWriteHalf;

pub struct Conn {
    pub w: OwnedWriteHalf,
    pub role: ConnRole,
}

impl Conn {
    pub fn new(w: OwnedWriteHalf) -> Self {
        Self {
            w,
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
