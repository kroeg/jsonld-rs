#![feature(generators, const_vec_new)]

#[macro_use]
extern crate lazy_static;

extern crate url;

extern crate serde;
extern crate serde_json;

extern crate futures_await as futures;

mod compact;
mod context;
mod creation;
mod expand;
pub mod nodemap;
pub mod rdf;

mod api;
pub use api::*;

use std::error::Error;
use std::fmt::Debug;

/// All the errors that may be returned by specific parts of the API.
pub mod error {
    pub use compact::CompactionError;
    pub use creation::{ContextCreationError, TermCreationError};
    pub use expand::ExpansionError;
}

use futures::prelude::*;

/// This trait is implemented by consumers of the API, to provide remote contexts.
pub trait RemoteContextLoader: Debug {
    type Error: Error + Send + Debug;
    type Future: Future<Item = serde_json::Value, Error = Self::Error> + Send + 'static;

    /// Loads a remote JSON-LD context into memory.
    fn load_context(url: String) -> Self::Future;
}
