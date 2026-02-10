use std::any::Any;

use crate::EnDecoder;

pub use crate::pbv1::{ErrorCode, ErrorResponse};

impl EnDecoder for ErrorResponse {
    fn index(&self) -> u16 {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
