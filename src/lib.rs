#[macro_use]
extern crate lazy_static;

extern crate url;

extern crate serde;
extern crate serde_json;

mod compact;
mod context;
mod creation;
mod expand;
pub mod rdf;

mod api;
pub use api::*;

pub mod error {
    pub use compact::CompactionError;
    pub use creation::{ContextCreationError, TermCreationError};
    pub use expand::ExpansionError;
}

/// This trait is implemented by consumers of the API, to provide remote contexts.
pub trait RemoteContextLoader {
    /// Loads a remote JSON-LD context into memory.
    fn load_context(&self, url: &str) -> Result<serde_json::Value, Box<std::error::Error>>;
}
