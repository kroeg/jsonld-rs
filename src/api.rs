use serde_json::Value;
use std::collections::HashMap;
use url::Url;

use crate::compact::CompactionError;
use crate::context::Context;
use crate::creation::ContextCreationError;
use crate::expand::ExpansionError;
use crate::RemoteContextLoader;

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

/// Takes a JSON value, pulls out the relevant @context value (XXX: is this specced?)
async fn unwrap_context<T: RemoteContextLoader>(
    ctx: &mut Context,
    to_expand: &Value,
) -> Result<(), ContextCreationError<T>> {
    let to_expand = match to_expand {
        Value::Object(ref val) => val.get("@context").unwrap_or(to_expand),

        _ => to_expand,
    };

    ctx.process_context::<T>(to_expand, &mut HashMap::new())
        .await
}

/// Compacts a JSON-LD structure according to the API specification.
pub async fn compact<T: RemoteContextLoader>(
    input: &Value,
    context: &Value,
    options: &JsonLdOptions,
) -> Result<Value, CompactionError<T>> {
    let mut ctx = Context::new();
    ctx.base_iri = options
        .base
        .as_ref()
        .map(|f| f as &str)
        .or_else(|| input.as_str())
        .and_then(|url| Url::parse(url).ok());

    if let Some(val) = &options.expand_context {
        unwrap_context(&mut ctx, &val)
            .await
            .map_err(CompactionError::ContextError)?;
    }

    let expanded = crate::expand::expand::<T>(&ctx, input)
        .await
        .map_err(CompactionError::ExpansionError)?;

    let context = context
        .as_object()
        .and_then(|f| f.get("@context"))
        .unwrap_or(context);

    crate::compact::compact::<T>(context, expanded, options.compact_arrays.unwrap_or(true)).await
}

/// Expands a JSON-LD structure according to the API specification.
pub async fn expand<T: RemoteContextLoader>(
    input: &Value,
    options: &JsonLdOptions,
) -> Result<Value, ExpansionError<T>> {
    let mut ctx = Context::new();
    ctx.base_iri = options
        .base
        .as_ref()
        .map(|f| f as &str)
        .or_else(|| input.as_str())
        .and_then(|url| Url::parse(url).ok());

    if let Some(val) = &options.expand_context {
        unwrap_context(&mut ctx, &val)
            .await
            .map_err(ExpansionError::ContextExpansionError)?;
    }

    crate::expand::expand::<T>(&ctx, input).await
}
