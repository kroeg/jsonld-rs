use serde_json::Value;
use std::rc::Rc;
use url::Url;

use std::collections::HashSet;

use super::RemoteContextLoader;

use context::Context;
use compact::CompactionError;
use expand::ExpansionError;

/// Options that may be passed to either `compact` or `expand`.
pub struct JsonLdOptions<'a> {
    /// The base IRI of the document. Used to resolve relative references.
    pub base: Option<String>,

    /// When compacting, if single-element arrays should be unpacked.
    pub compact_arrays: Option<bool>,

    /// The RemoteContextLoader to load remote contexts.
    pub document_loader: Rc<RemoteContextLoader>,

    /// The context to use when expanding the JSON-LD structures.
    pub expand_context: Option<&'a Value>,

    /// The processing mode, currently unused.
    pub processing_mode: Option<String>,
}

/// Compacts a JSON-LD structure according to the API specification.
pub fn compact(
    input: &Value,
    context: &Value,
    options: JsonLdOptions,
) -> Result<Value, CompactionError> {
    // 3
    let mut ctx = Context::new(options.document_loader.clone());
    ctx.base_iri = options
        .base
        .as_ref()
        .and_then::<&str, _>(|f| Some(f))
        .or_else(|| input.as_str())
        .and_then(|f| Url::parse(f).ok());

    // 4
    if let Some(val) = options.expand_context {
        ctx = if val.is_object() && val.as_object().unwrap().contains_key("@context") {
            ctx.process_context(
                val.as_object().unwrap().get("@context").unwrap(),
                &mut HashSet::new(),
            )
        } else {
            ctx.process_context(val, &mut HashSet::new())
        }.map_err(|e| CompactionError::ContextError(e))?;
    }

    let expanded = ctx.expand(input.clone())
        .map_err(|e| CompactionError::ExpansionError(e))?;

    let context = if context.is_object() && context.as_object().unwrap().contains_key("@context") {
        context.as_object().unwrap().get("@context").unwrap()
    } else {
        context
    };

    Context::compact(
        context,
        options.document_loader,
        &expanded,
        options.compact_arrays.unwrap_or(true),
    )
}

/// Expands a JSON-LD structure according to the API specification.
pub fn expand(input: &Value, options: JsonLdOptions) -> Result<Value, ExpansionError> {
    // 3
    let mut ctx = Context::new(options.document_loader.clone());
    ctx.base_iri = options
        .base
        .as_ref()
        .and_then::<&str, _>(|f| Some(f))
        .or_else(|| input.as_str())
        .and_then(|f| Url::parse(f).ok());

    // 4
    if let Some(val) = options.expand_context {
        ctx = if val.is_object() && val.as_object().unwrap().contains_key("@context") {
            ctx.process_context(
                val.as_object().unwrap().get("@context").unwrap(),
                &mut HashSet::new(),
            )
        } else {
            ctx.process_context(val, &mut HashSet::new())
        }.map_err(|e| ExpansionError::ContextExpansionError(e))?;
    }

    ctx.expand(input.clone())
}
