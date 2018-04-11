#[macro_use]
extern crate lazy_static;

extern crate url;

extern crate serde;
extern crate serde_json;

use std::fmt::Debug;

mod context;
mod creation;
mod expand;
mod compact;

mod api;
pub use api::*;

pub mod helper;

pub mod error {
    pub use creation::{ContextCreationError, TermCreationError};
    pub use expand::ExpansionError;
    pub use compact::CompactionError;
}

/// This trait is implemented by consumers of the API, to provide remote contexts.
pub trait RemoteContextLoader: Debug {
    /// Loads a remote JSON-LD context into memory.
    fn load_context(&self, url: &str) -> Result<serde_json::Value, Box<std::error::Error>>;
}
