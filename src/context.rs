use std::collections::BTreeMap;
use std::rc::Rc;
use url::Url;

use super::RemoteContextLoader;

#[derive(Clone)]
pub(crate) struct Term {
    pub type_mapping: Option<String>,
    pub iri_mapping: String,
    pub reverse: bool,
    pub container_mapping: Option<String>,
    pub language_mapping: Option<String>,
}

#[derive(Clone)]
pub struct Context {
    pub(crate) context_loader: Rc<RemoteContextLoader>,
    pub base_iri: Option<Url>,
    pub(crate) vocabulary_mapping: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) terms: BTreeMap<String, Term>,
}
