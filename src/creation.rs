use lazy_static::lazy_static;
use serde_json::Map as JsonMap;
use serde_json::Value;
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::From;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use url::Url;

use crate::context::{Context, Term};
use crate::RemoteContextLoader;

pub enum DefineStatus {
    Defining,
    Defined,
    Invalid,
}

#[derive(Debug)]
pub enum TermCreationError {
    CyclicIRIMapping,      //"cyclic IRI mapping"
    KeywordRedefinition,   // "keyword redefinition";
    InvalidTermDefinition, // "invalid term definition"
    InvalidIRIMapping,     // "invalid IRI mapping"
    InvalidReverseProperty,
    InvalidKeywordAlias,
    InvalidContainerMapping,
    InvalidLanguageMapping,
    InvalidTypeMapping,
}

impl fmt::Display for TermCreationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TermCreationError::CyclicIRIMapping => write!(f, "cyclic IRI mapping"),
            TermCreationError::KeywordRedefinition => write!(f, "keyword redefinition"),
            TermCreationError::InvalidTermDefinition => write!(f, "invalid term definition"),
            TermCreationError::InvalidIRIMapping => write!(f, "invalid IRI mapping"),
            TermCreationError::InvalidReverseProperty => write!(f, "invalid reverse property"),
            TermCreationError::InvalidKeywordAlias => write!(f, "invalid keyword alias"),
            TermCreationError::InvalidContainerMapping => write!(f, "invalid container mapping"),
            TermCreationError::InvalidLanguageMapping => write!(f, "invalid language mapping"),
            TermCreationError::InvalidTypeMapping => write!(f, "invalid type mapping"),
        }
    }
}

impl Error for TermCreationError {}

#[derive(Debug)]
pub enum ContextCreationError<T: RemoteContextLoader> {
    InvalidTerm(TermCreationError),
    RemoteContextError(T::Error),
    RemoteContextNoObject,

    RecursiveContextInclusion,
    InvalidBaseIRI,
    InvalidVocabMapping,
    InvalidLanguageMapping,
    InvalidLocalContext,

    TooManyContexts,
}

impl<T: RemoteContextLoader> From<TermCreationError> for ContextCreationError<T> {
    fn from(item: TermCreationError) -> Self {
        ContextCreationError::InvalidTerm(item)
    }
}

impl<T: RemoteContextLoader> fmt::Display for ContextCreationError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ContextCreationError::InvalidTerm(ref err) => write!(f, "invalid term: {}", err),
            ContextCreationError::RemoteContextError(ref err) => {
                write!(f, "invalid remote context: {}", err.deref())
            }
            ContextCreationError::RemoteContextNoObject => write!(f, "invalid remote context"),
            ContextCreationError::RecursiveContextInclusion => {
                write!(f, "recursive context inclusion")
            }
            ContextCreationError::InvalidBaseIRI => write!(f, "invalid base IRI"),
            ContextCreationError::InvalidVocabMapping => write!(f, "invalid vocab mapping"),
            ContextCreationError::InvalidLanguageMapping => write!(f, "invalid language mapping"),
            ContextCreationError::InvalidLocalContext => write!(f, "invalid local context"),
            ContextCreationError::TooManyContexts => write!(f, "too many contexts"),
        }
    }
}

impl<T: RemoteContextLoader> Error for ContextCreationError<T> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            ContextCreationError::InvalidTerm(ref err) => Some(err),
            ContextCreationError::RemoteContextError(_) => None,
            _ => None,
        }
    }
}

lazy_static! {
    static ref KEYWORDS: HashSet<&'static str> = vec![
        "@context",
        "@id",
        "@value",
        "@language",
        "@type",
        "@container",
        "@list",
        "@set",
        "@reverse",
        "@index",
        "@base",
        "@vocab",
        "@graph",
    ]
    .into_iter()
    .collect();
}

impl Context {
    pub fn new() -> Context {
        Context {
            base_iri: None,
            vocabulary_mapping: None,
            language: None,
            terms: BTreeMap::new(),
        }
    }

    // This is very much duplicated code,
    //  but it cannot be deduplicated; this function is only used
    //  while processing the context.
    fn expand_iri_mut(
        &mut self,
        val: &str,
        document_relative: bool,
        vocab: bool,
        defined: &mut HashMap<String, DefineStatus>,
        context: &JsonMap<String, Value>,
    ) -> Result<String, TermCreationError> {
        if val.starts_with('@') {
            Ok(val.to_string())
        } else {
            if context.contains_key(val) && !defined.contains_key(val) {
                let unwrapped = context.get(val).unwrap();
                self.create_term(context, val, unwrapped, defined)?;
            }

            if vocab && self.terms.contains_key(val) {
                let term = self.terms.get(val).unwrap();

                Ok(term.iri_mapping.clone())
            } else if let Some(loc) = val.find(':') {
                let prefix = &val[..loc];
                let suffix = &val[loc + 1..];

                if prefix == "_" || suffix.starts_with("//") {
                    Ok(val.to_owned())
                } else {
                    if context.contains_key(prefix) && !defined.contains_key(prefix) {
                        let unwrapped = context.get(prefix).unwrap();
                        self.create_term(context, prefix, unwrapped, defined)?;
                    }

                    if let Some(ref term) = self.terms.get(prefix).as_ref() {
                        Ok(term.iri_mapping.clone() + suffix)
                    } else {
                        Ok(val.to_owned())
                    }
                }
            } else if vocab && self.vocabulary_mapping.is_some() {
                Ok(self.vocabulary_mapping.as_ref().unwrap().to_string() + val)
            } else if document_relative && self.base_iri.is_some() {
                let base_iri = self.base_iri.as_ref().unwrap();
                let joined = base_iri.join(val).unwrap();
                Ok(joined.to_string())
            } else {
                Ok(val.to_string())
            }
        }
    }

    pub(crate) fn expand_iri(&self, val: &str, document_relative: bool, vocab: bool) -> String {
        if val.starts_with('@') {
            val.to_string()
        } else if vocab && self.terms.contains_key(val) {
            let term = self.terms.get(val).unwrap();

            term.iri_mapping.clone()
        } else if let Some(loc) = val.find(':') {
            let prefix = &val[..loc];
            let suffix = &val[loc + 1..];

            if prefix == "_" || suffix.starts_with("//") {
                val.to_owned()
            } else if let Some(term) = self.terms.get(prefix).as_ref() {
                term.iri_mapping.clone() + suffix
            } else {
                val.to_owned()
            }
        } else if vocab && self.vocabulary_mapping.is_some() {
            self.vocabulary_mapping.as_ref().unwrap().to_string() + val
        } else if document_relative && self.base_iri.is_some() {
            let base_iri = self.base_iri.as_ref().unwrap();
            let joined = base_iri.join(val).unwrap();
            joined.to_string()
        } else {
            val.to_string()
        }
    }

    fn create_term(
        &mut self,
        context: &JsonMap<String, Value>,
        term: &str,
        value: &Value,
        defined: &mut HashMap<String, DefineStatus>,
    ) -> Result<(), TermCreationError> {
        match defined.get(term) {
            Some(DefineStatus::Defining) => return Err(TermCreationError::CyclicIRIMapping),
            Some(DefineStatus::Defined) => return Ok(()),
            Some(DefineStatus::Invalid) => return Ok(()),
            None => (),
        }

        defined.insert(term.to_owned(), DefineStatus::Defining);

        if KEYWORDS.contains(term) {
            return Err(TermCreationError::KeywordRedefinition);
        }

        self.terms.remove(term);
        let mut value = Cow::Borrowed(value);

        // 7
        if let Value::String(string) = value.as_ref() {
            let mut map = JsonMap::new();
            map.insert("@id".to_owned(), Value::String(string.clone()));
            value = Cow::Owned(Value::Object(map));
        };

        match value.as_ref() {
            Value::Null => {
                self.terms.insert(
                    term.to_owned(),
                    Term {
                        type_mapping: None,
                        iri_mapping: term.to_owned(),
                        reverse: false,
                        container_mapping: None,
                        language_mapping: None,
                    },
                );

                defined.insert(term.to_string(), DefineStatus::Defined);
            }
            Value::String(_) => unreachable!(),
            Value::Object(map) => {
                if map.len() == 1 && map.get("@id") == Some(&Value::Null) {
                    self.terms.insert(
                        term.to_owned(),
                        Term {
                            type_mapping: None,
                            iri_mapping: term.to_owned(),
                            reverse: false,
                            container_mapping: None,
                            language_mapping: None,
                        },
                    );

                    defined.insert(term.to_string(), DefineStatus::Defined);

                    return Ok(());
                }

                let type_mapping = if let Some(at_type) = map.get("@type") {
                    match at_type {
                        Value::String(string) => {
                            let res = self.expand_iri_mut(string, false, true, defined, context)?;
                            if !res.contains(':') && res != "@id" && res != "@vocab" {
                                return Err(TermCreationError::InvalidTypeMapping);
                            }
                            Some(res)
                        }

                        _ => return Err(TermCreationError::InvalidTypeMapping),
                    }
                } else {
                    None
                };

                if let Some(at_reverse) = map.get("@reverse") {
                    if map.contains_key("@id") {
                        return Err(TermCreationError::InvalidReverseProperty);
                    }

                    let reverse_map = match at_reverse {
                        Value::String(string) => {
                            let res =
                                self.expand_iri_mut(&string, false, true, defined, context)?;
                            if !res.contains(':') {
                                return Err(TermCreationError::InvalidIRIMapping);
                            }
                            Some(res)
                        }

                        _ => return Err(TermCreationError::InvalidIRIMapping),
                    };

                    let container_mapping = if let Some(at_container) = map.get("@container") {
                        match at_container {
                            Value::String(string) => {
                                if string == "@set" || string == "@index" {
                                    Some(string.clone())
                                } else {
                                    return Err(TermCreationError::InvalidReverseProperty);
                                }
                            }
                            Value::Null => None,
                            _ => return Err(TermCreationError::InvalidReverseProperty),
                        }
                    } else {
                        None
                    };

                    // 11.6
                    defined.insert(term.to_owned(), DefineStatus::Defined);

                    self.terms.insert(
                        term.to_owned(),
                        Term {
                            type_mapping,
                            iri_mapping: reverse_map.unwrap(),
                            reverse: true,
                            container_mapping,
                            language_mapping: None,
                        },
                    );
                } else {
                    let mut iri_mapping = if let Some(at_id) = map.get("@id") {
                        match at_id {
                            Value::String(string) => {
                                if string == term {
                                    None
                                } else {
                                    let expanded = self
                                        .expand_iri_mut(&string, false, true, defined, context)?;
                                    if expanded == "@context" {
                                        return Err(TermCreationError::InvalidKeywordAlias);
                                    } else if !expanded.starts_with('@')
                                        && !expanded.starts_with("_:")
                                        && !expanded.contains("://")
                                    {
                                        return Err(TermCreationError::InvalidIRIMapping);
                                    } else {
                                        Some(expanded)
                                    }
                                }
                            }

                            Value::Null => Some(term.to_owned()),
                            _ => return Err(TermCreationError::InvalidIRIMapping),
                        }
                    } else {
                        None
                    };

                    if let (None, Some(location)) = (&iri_mapping, term.find(':')) {
                        let (first, second) = term.split_at(location);
                        let second = &second[1..];

                        if context.contains_key(first) {
                            let term_value = context.get(first).unwrap();
                            self.create_term(context, first, &term_value, defined)?;
                        }

                        if self.terms.contains_key(first) {
                            let mut new_iri_mapping = "".to_owned();
                            let first_term = self.terms.get(first).unwrap();
                            new_iri_mapping.push_str(&first_term.iri_mapping);
                            new_iri_mapping.push_str(second);

                            iri_mapping = Some(new_iri_mapping);
                        } else {
                            iri_mapping = Some(term.to_string());
                        }
                    }

                    if iri_mapping == None {
                        if let Some(ref vocab) = self.vocabulary_mapping {
                            let mut new_iri_mapping = vocab.to_string();
                            new_iri_mapping.push_str(term);
                            iri_mapping = Some(new_iri_mapping);
                        } else {
                            return Err(TermCreationError::InvalidIRIMapping);
                        }
                    }

                    let container_mapping = if let Some(at_container) = map.get("@container") {
                        match at_container {
                            Value::String(string) => {
                                if string == "@list"
                                    || string == "@set"
                                    || string == "@index"
                                    || string == "@language"
                                {
                                    Some(string.clone())
                                } else {
                                    return Err(TermCreationError::InvalidContainerMapping);
                                }
                            }
                            _ => return Err(TermCreationError::InvalidContainerMapping),
                        }
                    } else {
                        None
                    };

                    let language_mapping = if type_mapping != None {
                        None
                    } else if let Some(language) = map.get("@language") {
                        match language {
                            Value::String(string) => Some(string.to_lowercase()),
                            Value::Null => Some("@null".to_owned()),

                            _ => return Err(TermCreationError::InvalidLanguageMapping),
                        }
                    } else {
                        None
                    };

                    defined.insert(term.to_string(), DefineStatus::Defined);
                    self.terms.insert(
                        term.to_string(),
                        Term {
                            type_mapping,
                            iri_mapping: iri_mapping.unwrap(),
                            reverse: false,
                            container_mapping,
                            language_mapping,
                        },
                    );
                }
            }

            _ => return Err(TermCreationError::InvalidTermDefinition),
        };

        Ok(())
    }

    pub async fn process_context<T: RemoteContextLoader>(
        &mut self,
        local_context: &Value,
        remote_contexts: &mut HashMap<String, Option<Value>>,
    ) -> Result<(), ContextCreationError<T>> {
        self._process_context(&local_context, remote_contexts).await
    }

    fn _process_context<'d, 'a: 'd, 'b: 'd, 'c: 'd, T: RemoteContextLoader>(
        &'a mut self,
        local_context: &'c Value,
        remote_contexts: &'b mut HashMap<String, Option<Value>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ContextCreationError<T>>> + 'd + Send>> {
        Box::pin(async move {
            let local_context = match local_context {
                Value::Array(a) => a.iter().collect(),
                _ => vec![local_context],
            };

            for context in local_context {
                match context {
                    Value::Null => {
                        *self = Context::new();
                        self.base_iri = self.base_iri.clone();
                    }

                    Value::String(val) => {
                        if remote_contexts.len() > 4 {
                            return Err(ContextCreationError::TooManyContexts);
                        }

                        match remote_contexts.get(val) {
                            Some(None) => {
                                return Err(ContextCreationError::RecursiveContextInclusion)
                            }
                            Some(Some(context)) => {
                                self.process_context::<T>(&context.clone(), remote_contexts)
                                    .await?;
                            }

                            None => {
                                let dereferenced = T::load_context(val.to_owned())
                                    .await
                                    .map_err(ContextCreationError::RemoteContextError)?;
                                remote_contexts.insert(val.to_owned(), None);

                                if let Value::Object(mut obj) = dereferenced {
                                    let context = obj
                                        .remove("@context")
                                        .unwrap_or_else(|| Value::Object(JsonMap::new()));

                                    self.process_context::<T>(&context, remote_contexts).await?;
                                    remote_contexts.insert(val.clone(), Some(context));
                                } else {
                                    return Err(ContextCreationError::RemoteContextNoObject);
                                }
                            }
                        }
                    }
                    Value::Object(ref map) => {
                        let mut defined: HashMap<String, DefineStatus> = HashMap::new();

                        let base = map.get("@base");
                        if base != None && remote_contexts.is_empty() {
                            let value = base.unwrap();
                            match value {
                                Value::Null => self.base_iri = None,
                                Value::String(val) => {
                                    if let Some(iri) = &self.base_iri {
                                        self.base_iri =
                                            Some(iri.join(val).map_err(|_| {
                                                ContextCreationError::InvalidBaseIRI
                                            })?);
                                    } else {
                                        self.base_iri =
                                            Some(Url::parse(val).map_err(|_| {
                                                ContextCreationError::InvalidBaseIRI
                                            })?);
                                    }
                                }
                                _ => return Err(ContextCreationError::InvalidBaseIRI),
                            }
                        }

                        if base != None {
                            defined.insert("@base".to_owned(), DefineStatus::Invalid);
                        }

                        if let Some(vocab) = map.get("@vocab") {
                            match vocab {
                                Value::Null => self.vocabulary_mapping = None,
                                Value::String(data) => {
                                    self.vocabulary_mapping = Some(data.clone());
                                }
                                _ => return Err(ContextCreationError::InvalidVocabMapping),
                            }

                            defined.insert("@vocab".to_owned(), DefineStatus::Invalid);
                        }

                        if let Some(language) = map.get("@language") {
                            match language {
                                Value::Null => self.language = None,
                                Value::String(data) => {
                                    self.language = Some(data.to_lowercase());
                                }

                                _ => return Err(ContextCreationError::InvalidLanguageMapping),
                            }

                            defined.insert("@language".to_owned(), DefineStatus::Invalid);
                        }

                        while let Some((key, value)) =
                            map.iter().find(|(k, _)| !defined.contains_key(k as &str))
                        {
                            self.create_term(map, &key, value, &mut defined)?;
                        }
                    }

                    _ => return Err(ContextCreationError::InvalidLocalContext),
                }
            }

            Ok(())
        })
    }
}
