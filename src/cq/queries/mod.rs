mod error;
pub use error::{Error, Result};

mod dto;

mod list_collections;
pub(super) use list_collections::*;

mod search;
pub(super) use search::*;
mod search_all;
pub(super) use search_all::*;
