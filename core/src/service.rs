use anyhow::Result;
pub trait Service {
    fn init() -> Result<()>;
    fn start(&mut self) -> Result<()>;
    fn stop(&mut self) -> Result<()>;
}
