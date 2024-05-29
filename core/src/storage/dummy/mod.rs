use crate::cache::memory::Memory;

use super::StorageOperation;

pub struct Dummy(Memory);

impl StorageOperation for Dummy {
    async fn init(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    async fn next(&mut self) -> Option<crate::message::Message> {
        todo!()
    }

    async fn push(&mut self, _: crate::message::Message) -> anyhow::Result<()> {
        todo!()
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
