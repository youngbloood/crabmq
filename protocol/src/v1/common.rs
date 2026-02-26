use std::any::Any;

use crate::{EnDecoder};
use super::TOPICS_INDEX;

pub use crate::pbv1::{PartitionCluster, PartitionInfo, SegmentOffset, TopicInfo, Topics};

impl EnDecoder for Topics {
    fn index(&self) -> u16 {
        TOPICS_INDEX
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PartitionInfo {
    pub fn can_write(&self) -> bool {
        self.is_leader
    }

    pub fn can_read(&self) -> bool {
        self.is_leader || (!self.is_leader && self.readble)
    }
}
