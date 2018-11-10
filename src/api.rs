use serde_json::Value;
use url::Url;

use std::collections::HashMap;

use super::RemoteContextLoader;

use compact::CompactionError;
use context::Context;
use expand::ExpansionError;

use futures::prelude::{await, *};

/// Options that may be passed to either `compact` or `expand`.
pub struct JsonLdOptions {
    /// The base IRI of the document. Used to resolve relative references.
    pub base: Option<String>,

    /// When compacting, if single-element arrays should be unpacked.
    pub compact_arrays: Option<bool>,

    /// The context to use when expanding the JSON-LD structures.
    pub expand_context: Option<Value>,

    /// The processing mode, currently unused.
    pub processing_mode: Option<String>,
}

/// Compacts a JSON-LD structure according to the API specification.
#[async]
pub fn compact<T: RemoteContextLoader>(
    input: Value,
    context: Value,
    options: JsonLdOptions,
) -> Result<Value, CompactionError<T>> {
    // 3
    let mut ctx = Context::new();
    ctx.base_iri = options
        .base
        .as_ref()
        .and_then::<&str, _>(|f| Some(f))
        .or_else(|| input.as_str())
        .and_then(|f| Url::parse(f).ok());

    // 4
    if let Some(val) = options.expand_context {
        let (_, c) = if let Value::Object(mut val) = val {
            if let Some(val) = val.remove("@context") {
                await!(ctx.process_context::<T>(val, HashMap::new(),))
            } else {
                await!(ctx.process_context::<T>(Value::Object(val), HashMap::new()))
            }
        } else {
            await!(ctx.process_context::<T>(val, HashMap::new()))
        }
        .map_err(|e| CompactionError::ContextError(e))?;

        ctx = c;
    }

    let expanded =
        await!(ctx.expand::<T>(input)).map_err(|e| CompactionError::ExpansionError(e))?;

    let context = if let Value::Object(mut val) = context {
        if let Some(val) = val.remove("@context") {
            val
        } else {
            Value::Object(val)
        }
    } else {
        context
    };

    await!(Context::compact::<T>(
        context,
        expanded,
        options.compact_arrays.unwrap_or(true),
    ))
}

/// Expands a JSON-LD structure according to the API specification.
#[async]
pub fn expand<T: RemoteContextLoader>(
    input: Value,
    options: JsonLdOptions,
) -> Result<Value, ExpansionError<T>> {
    // 3
    let mut ctx = Context::new();
    ctx.base_iri = options
        .base
        .as_ref()
        .and_then::<&str, _>(|f| Some(f))
        .or_else(|| input.as_str())
        .and_then(|f| Url::parse(f).ok());

    // 4
    if let Some(val) = options.expand_context {
        let (_, c) = if let Value::Object(mut val) = val {
            if let Some(val) = val.remove("@context") {
                await!(ctx.process_context::<T>(val, HashMap::new(),))
            } else {
                await!(ctx.process_context::<T>(Value::Object(val), HashMap::new()))
            }
        } else {
            await!(ctx.process_context::<T>(val, HashMap::new()))
        }
        .map_err(|e| ExpansionError::ContextExpansionError(e))?;

        ctx = c;
    }

    await!(ctx.expand::<T>(input))
}
