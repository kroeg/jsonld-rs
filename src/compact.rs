use super::RemoteContextLoader;
use context::{Context, Term};

use creation::ContextCreationError;
use expand::ExpansionError;
use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use futures::prelude::{*, await};

#[derive(Debug)]
/// Errors that might occur when compacting a JSON-LD structure.
pub enum CompactionError<T: RemoteContextLoader> {
    /// Expected a specific value to be a string, but it wasn't.
    IdNotString,
    TypeNotString,
    IdOrTypeNotString,
    LanguageOrIndexNotString,
    LanguageNotString,

    /// The value of a `@list` property isn't an array
    ListObjectNotArray,

    /// The item inside a `@list` array isn't an object
    ListItemNotObject,

    /// The value of `@reverse` is not an object.
    TermNotObject,

    /// An error occured parsing the context to use when compacting.
    ContextError(ContextCreationError<T>),

    /// Compaction ended up compacting a list of lists, which is verboten.
    CompactionToListOfLists,

    /// Expanding the object to compact failed.
    ExpansionError(ExpansionError<T>),
}

impl<T: RemoteContextLoader> fmt::Display for CompactionError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl<T: RemoteContextLoader> Error for CompactionError<T> {
    fn description(&self) -> &str {
        match *self {
            CompactionError::IdNotString => "@id is not string",
            CompactionError::TypeNotString => "@type value is not a string",
            CompactionError::IdOrTypeNotString => "@id or @type value is not a string",
            CompactionError::LanguageOrIndexNotString => "@language or @type value is not a string",
            CompactionError::LanguageNotString => "@language value is not a string",
            CompactionError::ListObjectNotArray => "value of @list is not an array",
            CompactionError::ListItemNotObject => "item in @list is not an object",
            CompactionError::TermNotObject => "value of @reverse is not an object",
            CompactionError::ContextError(_) => "error parsing the context",
            CompactionError::CompactionToListOfLists => "compaction to list of lists",
            CompactionError::ExpansionError(_) => "error expanding the input",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            CompactionError::ContextError(ref err) => Some(err),
            CompactionError::ExpansionError(ref err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct TypeLanguageMap {
    pub type_map: HashMap<String, String>,
    pub language_map: HashMap<String, String>,
}

#[derive(Debug)]
struct InverseContext {
    container_map: HashMap<String, HashMap<String, TypeLanguageMap>>,
}

fn _sort_term(x: &(&String, &Term), y: &(&String, &Term)) -> Ordering {
    let (ref a, _) = *x;
    let (ref b, _) = *y;

    let len_order = a.len().cmp(&b.len());
    match len_order {
        Ordering::Equal => a.cmp(&b),
        _ => len_order,
    }
}

#[derive(PartialEq, Eq)]
enum TypeOrLanguage {
    Type,
    Language,
}

impl InverseContext {
    pub fn new(ctx: &Context) -> InverseContext {
        // 1
        let mut result = InverseContext {
            container_map: HashMap::new(),
        };

        // 2
        let default_language = ctx.language.as_ref().map_or("@none", |f| &f);

        let mut term_order: Vec<_> = ctx.terms.iter().collect();
        term_order.sort_by(_sort_term);

        // 3
        for (term, value) in term_order {
            // 3.2
            let container = value.container_mapping.as_ref().map_or("@none", |f| &f);

            // 3.3
            let iri = &value.iri_mapping;

            // 3.4
            if !result.container_map.contains_key(iri) {
                result.container_map.insert(iri.to_owned(), HashMap::new());
            }

            // 3.5
            let container_map = result.container_map.get_mut(iri).unwrap();

            // 3.6
            if !container_map.contains_key(container) {
                container_map.insert(
                    container.to_owned(),
                    TypeLanguageMap {
                        type_map: HashMap::new(),
                        language_map: HashMap::new(),
                    },
                );
            }

            // 3.7
            let type_language_map = container_map.get_mut(container).unwrap();

            // 3.8
            if value.reverse {
                let type_map = &mut type_language_map.type_map;
                if !type_map.contains_key("@reverse") {
                    type_map.insert("@reverse".to_owned(), term.to_owned());
                }
            }
            // 3.9
            else if let Some(ref type_mapping) = value.type_mapping {
                let type_map = &mut type_language_map.type_map;
                if !type_map.contains_key(type_mapping) {
                    type_map.insert(type_mapping.to_owned(), term.to_owned());
                }
            }
            // 3.10
            else if let Some(ref language_mapping) = value.language_mapping {
                let language_map = &mut type_language_map.language_map;
                if !language_map.contains_key(language_mapping) {
                    language_map.insert(language_mapping.to_owned(), term.to_owned());
                }
            }
            // 3.11
            else {
                let language_map = &mut type_language_map.language_map;
                if !language_map.contains_key(default_language) {
                    language_map.insert(default_language.to_owned(), term.to_owned());
                }

                if !language_map.contains_key("@none") {
                    language_map.insert("@none".to_owned(), term.to_owned());
                }

                let type_map = &mut type_language_map.type_map;
                if !type_map.contains_key("@none") {
                    type_map.insert("@none".to_owned(), term.to_owned());
                }
            }
        }

        result
    }

    fn _select_term(
        &self,
        iri: &str,
        containers: &[&str],
        type_language: TypeOrLanguage,
        preferred_values: &[&str],
    ) -> Option<&str> {
        if let Some(container_map) = self.container_map.get(iri) {
            for container in containers {
                if let Some(type_language_map) = container_map.get(*container) {
                    let value_map = if type_language == TypeOrLanguage::Language {
                        &type_language_map.language_map
                    } else {
                        &type_language_map.type_map
                    };

                    for item in preferred_values {
                        if let Some(item) = value_map.get(*item) {
                            return Some(item);
                        }
                    }
                }
            }
        }

        None
    }
}

impl Context {
    #[async]
    pub fn compact<T: RemoteContextLoader>(
        context: Value,
        element: Value,
        compact_arrays: bool,
    ) -> Result<Value, CompactionError<T>> {
        let (_, ctx) = await!(Context::new().process_context::<T>(context.clone(), HashSet::new()))
            .map_err(|e| CompactionError::ContextError(e))?;

        let inverse = InverseContext::new(&ctx);
        let mut res = Context::_compact(&ctx, &inverse, None, &element, compact_arrays)?;
        if res.is_array() {
            let mut map = Map::new();
            map.insert(
                ctx._compact_iri(
                    &inverse, "@graph", None, /* XXX Some(res) */
                    true, // XXX is this right???
                    false,
                )?,
                res,
            );
            res = Value::Object(map);
        }

        if res.is_object()
            && !context.is_null()
            && (!context.is_object() || context.as_object().unwrap().len() > 0)
        {
            res.as_object_mut()
                .unwrap()
                .insert("@context".to_owned(), context.clone());
        }

        Ok(res)
    }

    fn _compact<T: RemoteContextLoader>(
        active_context: &Context,
        inverse_context: &InverseContext,
        active_property: Option<&str>,
        element: &Value,
        compact_arrays: bool,
    ) -> Result<Value, CompactionError<T>> {
        match *element {
            Value::Array(ref arr) => {
                // 2

                let mut result = Vec::new();
                for item in arr {
                    let compacted = Context::_compact(
                        active_context,
                        inverse_context,
                        active_property,
                        item,
                        compact_arrays, // XXX right?
                    )?;
                    if compacted != Value::Null {
                        result.push(compacted);
                    }
                }

                if result.len() == 1 && compact_arrays {
                    if let Some(prop) = active_property {
                        if active_context.terms.contains_key(prop) {
                            if active_context.terms[prop].container_mapping == None {
                                return Ok(result.remove(0));
                            }
                        } else {
                            return Ok(result.remove(0));
                        }
                    } else {
                        return Ok(result.remove(0));
                    }
                }

                Ok(Value::Array(result))
            }

            Value::Object(ref obj) => {
                // 4
                if obj.contains_key("@value") || obj.contains_key("@id") {
                    let res = active_context._compact_value(inverse_context, active_property, obj)?;
                    if !res.is_array() && !res.is_object() {
                        return Ok(res);
                    }
                }

                // 5
                let inside_reverse = active_property == Some("@reverse");

                // 6
                let mut result = Map::new();

                // 7, should be implicitly ordered??
                for (expanded_property, expanded_value) in obj {
                    if expanded_property == "@id" || expanded_property == "@type" {
                        let compacted_value = match *expanded_value {
                            Value::String(ref strval) => {
                                Value::String(active_context._compact_iri(
                                    inverse_context,
                                    strval,
                                    None,
                                    expanded_property == "@type",
                                    false,
                                )?)
                            }
                            Value::Array(ref arval) => {
                                if expanded_property != "@type" {
                                    return Err(CompactionError::IdNotString);
                                }

                                let mut res = Vec::new();
                                for item in arval {
                                    match *item {
                                        Value::String(ref stv) => {
                                            res.push(Value::String(active_context._compact_iri(
                                                inverse_context,
                                                stv,
                                                None,
                                                true,
                                                false,
                                            )?))
                                        }
                                        _ => return Err(CompactionError::TypeNotString),
                                    }
                                }

                                if res.len() == 1 {
                                    res.remove(0)
                                } else {
                                    Value::Array(res)
                                }
                            }
                            _ => return Err(CompactionError::IdOrTypeNotString),
                        };

                        let alias = active_context._compact_iri(
                            inverse_context,
                            expanded_property,
                            None,
                            true,
                            false,
                        )?;
                        result.insert(alias, compacted_value);
                        continue;
                    } else if expanded_property == "@reverse" {
                        let compacted_value = Context::_compact(
                            active_context,
                            inverse_context,
                            Some("@reverse"),
                            &expanded_value,
                            compact_arrays,
                        )?;
                        let mut new_map = Map::new();
                        if let Value::Object(obj) = compacted_value {
                            for (property, mut value) in obj {
                                if let Some(term) = active_context.terms.get(&property) {
                                    if term.reverse {
                                        if (term
                                            .container_mapping
                                            .as_ref()
                                            .and_then(|e| Some(e == "@set"))
                                            == Some(true)
                                            || !compact_arrays)
                                            && !value.is_array()
                                        {
                                            value = Value::Array(vec![value].into());
                                        }

                                        if !result.contains_key(&property) {
                                            result.insert(property, value);
                                        } else {
                                            let mut val = result.remove(&property).unwrap();
                                            if !val.is_array() {
                                                val = Value::Array(vec![val].into());
                                            }

                                            if value.is_array() {
                                                val.as_array_mut()
                                                    .unwrap()
                                                    .append(value.as_array_mut().unwrap());
                                            } else {
                                                val.as_array_mut().unwrap().push(value);
                                            }

                                            result.insert(property, val);
                                        }
                                    } else {
                                        new_map.insert(property, value);
                                    }
                                } else {
                                    new_map.insert(property, value);
                                }
                            }

                            if new_map.len() > 0 {
                                let alias = active_context._compact_iri(
                                    inverse_context,
                                    "@reverse",
                                    None,
                                    true,
                                    false,
                                )?;
                                result.insert(alias, Value::Object(new_map));
                            }

                            continue;
                        } else {
                            return Err(CompactionError::TermNotObject);
                        }
                    }

                    // 7.3
                    if expanded_property == "@index" {
                        if let Some(container_mapping) = active_property
                            .and_then(|f| active_context.terms.get(f))
                            .and_then(|f| f.container_mapping.as_ref())
                        {
                            if container_mapping == "@index" {
                                continue;
                            }
                        }
                    }

                    // 7.4
                    if expanded_property == "@index"
                        || expanded_property == "@value"
                        || expanded_property == "@language"
                    {
                        let alias = active_context._compact_iri(
                            inverse_context,
                            expanded_property,
                            None,
                            true,
                            false,
                        )?;
                        result.insert(alias, expanded_value.clone());
                        continue;
                    }

                    // 7.5
                    if expanded_value.as_array().and_then(|f| Some(f.len())) == Some(0) {
                        let item_active_property = active_context._compact_iri(
                            inverse_context,
                            expanded_property,
                            /*Some(expanded_value) XXX*/ None,
                            true,
                            inside_reverse,
                        )?;
                        if !result.contains_key(&item_active_property) {
                            result.insert(item_active_property, Value::Array(Vec::new()));
                        } else {
                            let val = Value::Array(
                                vec![result.remove(&item_active_property).unwrap()].into(),
                            );
                            result.insert(item_active_property, val);
                        }
                    }

                    // 7.6
                    for expanded_item in expanded_value.as_array().unwrap() {
                        let item_active_property = active_context._compact_iri(
                            inverse_context,
                            expanded_property,
                            expanded_item.as_object(),
                            true,
                            inside_reverse,
                        )?;
                        let mut container: Option<&str> = None;
                        if let Some(container_mapping) = active_context
                            .terms
                            .get(&item_active_property)
                            .and_then(|f| f.container_mapping.as_ref())
                        {
                            container = Some(container_mapping);
                        }

                        let data = expanded_item.as_object().unwrap();
                        let to_pass = if data.contains_key("@list") {
                            &data["@list"]
                        } else {
                            expanded_item
                        };

                        let mut compacted_item = Context::_compact(
                            active_context,
                            inverse_context,
                            Some(&item_active_property),
                            to_pass,
                            compact_arrays,
                        )?;
                        if data.contains_key("@list") {
                            if !compacted_item.is_array() {
                                compacted_item = Value::Array(vec![compacted_item].into());
                            }

                            if container != Some("@list") {
                                // 7.6.4.2
                                let mut m = Map::new();
                                m.insert(
                                    active_context._compact_iri(
                                        inverse_context,
                                        "@list",
                                        /*Some(compacted_item) XXX*/ None,
                                        true,
                                        false,
                                    )?,
                                    compacted_item,
                                );
                                if data.contains_key("@index") {
                                    m.insert(
                                        active_context._compact_iri(
                                            inverse_context,
                                            "@index",
                                            /*Some(compacted_item) XXX*/ None,
                                            true,
                                            false,
                                        )?,
                                        data["@index"].clone(),
                                    );
                                }

                                compacted_item = Value::Object(m);
                            } else {
                                // 7.6.4.3
                                if result.contains_key(&item_active_property) {
                                    return Err(CompactionError::CompactionToListOfLists);
                                }
                            }
                        }

                        if container == Some("@language") || container == Some("@index") {
                            // 7.6.5
                            if !result.contains_key(&item_active_property) {
                                let map = Map::new();
                                result.insert(item_active_property.clone(), Value::Object(map));
                            }

                            let map_object = result
                                .get_mut(&item_active_property)
                                .and_then(|f| f.as_object_mut())
                                .unwrap();

                            if container == Some("@language")
                                && compacted_item
                                    .as_object()
                                    .and_then(|f| Some(f.contains_key("@value")))
                                    == Some(true)
                            {
                                compacted_item = compacted_item
                                    .as_object_mut()
                                    .and_then(|f| f.remove("@value"))
                                    .unwrap();
                            }

                            let map_key = data[container.unwrap()]
                                .as_str()
                                .ok_or(CompactionError::LanguageOrIndexNotString)?;
                            if !map_object.contains_key(map_key) {
                                map_object.insert(map_key.to_owned(), compacted_item);
                            } else {
                                let mut val = map_object.remove(map_key).unwrap();
                                if val.is_array() {
                                    val.as_array_mut().unwrap().push(compacted_item);
                                } else {
                                    val = Value::Array(vec![val, compacted_item].into());
                                }

                                map_object.insert(map_key.to_owned(), val);
                            }
                        } else {
                            // 7.6.6.1
                            if (!compact_arrays
                                || container == Some("@set")
                                || container == Some("@list")
                                || expanded_property == "@list"
                                || expanded_property == "@graph")
                                && !compacted_item.is_array()
                            {
                                compacted_item = Value::Array(vec![compacted_item].into());
                            }

                            if !result.contains_key(&item_active_property) {
                                result.insert(item_active_property, compacted_item);
                            } else {
                                let mut val = result.remove(&item_active_property).unwrap();
                                let mut varr = if let Value::Array(ar) = compacted_item {
                                    ar
                                } else {
                                    vec![compacted_item].into()
                                };

                                if val.is_array() {
                                    val.as_array_mut().unwrap().append(&mut varr);
                                } else {
                                    varr.insert(0, val);
                                    val = Value::Array(varr);
                                }

                                result.insert(item_active_property, val);
                            }
                        }
                    }
                }

                Ok(Value::Object(result))
            }

            _ => Ok(element.clone()),
        }
    }

    fn _compact_iri<T: RemoteContextLoader>(
        &self,
        inverse_context: &InverseContext,
        iri: &str,
        value: Option<&Map<String, Value>>,
        vocab: bool,
        reverse: bool,
    ) -> Result<String, CompactionError<T>> {
        if vocab && inverse_context.container_map.contains_key(iri) {
            let default_language = self.language.as_ref().map_or("@none", |f| &f);
            let mut containers = Vec::new();
            let mut type_language = TypeOrLanguage::Language;
            let mut type_language_value = "@null";

            if let Some(ref item) = value {
                if item.contains_key("@index") {
                    containers.push("@index");
                }
            }

            // 2.5
            if reverse {
                type_language = TypeOrLanguage::Type;
                type_language_value = "@reverse";
                containers.push("@set");
            } else if let Some(ref item) = value {
                // 2.6
                if let Some(lv) = item.get("@list") {
                    // 2.6.1
                    if !item.contains_key("@index") {
                        containers.push("@list");
                    }

                    // 2.6.2
                    let list = lv.as_array().ok_or(CompactionError::ListObjectNotArray)?;

                    let mut common_type = None;
                    let mut common_language = None;

                    // 2.6.3
                    if list.len() == 0 {
                        common_language = Some(default_language)
                    }

                    // 2.6.4
                    for vitem in list {
                        // 2.6.4.1
                        let mut item_language: Option<&str> = None;
                        let mut item_type: Option<&str> = None;

                        if let Value::Object(ref item) = *vitem {
                            // 2.6.4.2
                            if item.contains_key("@value") {
                                if item.contains_key("@language") {
                                    // 2.6.4.2.1
                                    match item["@language"] {
                                        Value::String(ref string) => {
                                            item_language = Some(string);
                                        }
                                        Value::Null => item_language = Some("@null"),
                                        _ => return Err(CompactionError::LanguageNotString),
                                    };
                                } else if item.contains_key("@type") {
                                    // 2.6.4.2.2 -- warning! @type will be string
                                    item_type = Some(
                                        item["@type"]
                                            .as_str()
                                            .ok_or(CompactionError::TypeNotString)?,
                                    );
                                } else {
                                    // 2.6.4.2.3
                                    item_language = Some("@null")
                                }
                            } else {
                                // 2.6.4.3
                                item_type = Some("@id");
                            }

                            if common_language == None {
                                // 2.6.4.4
                                common_language = item_language;
                            } else if item_language != common_language
                                && item.contains_key("@value")
                            {
                                // 2.6.4.5
                                common_language = Some("@none");
                            }

                            if common_type == None {
                                // 2.6.4.6
                                common_type = item_type;
                            } else if common_type != item_type {
                                // 2.6.4.7
                                common_type = Some("@none")
                            }

                            if common_type == Some("@none") && common_language == Some("@none") {
                                // 2.6.4.8
                                break;
                            }
                        } else {
                            return Err(CompactionError::ListItemNotObject);
                        }
                    }

                    // 2.6.5
                    let common_language = match common_language {
                        None => "@none",
                        Some(val) => val,
                    };

                    // 2.6.6
                    let common_type = match common_type {
                        None => "@none",
                        Some(val) => val,
                    };

                    if common_type != "@none" {
                        // 2.6.7
                        type_language = TypeOrLanguage::Type;
                        type_language_value = common_type
                    } else {
                        // 2.6.8
                        type_language_value = common_language
                    }
                } else {
                    if item.contains_key("@value") {
                        // 2.7.1
                        if item.contains_key("@language") && !item.contains_key("@index") {
                            // 2.7.1.1
                            type_language_value = item["@language"]
                                .as_str()
                                .ok_or(CompactionError::LanguageNotString)?;
                            containers.push("@language");
                        } else if item.contains_key("@type") {
                            // 2.7.1.2
                            type_language = TypeOrLanguage::Type;
                            type_language_value = item["@type"]
                                .as_str()
                                .ok_or(CompactionError::TypeNotString)?;
                        }
                    } else {
                        // 2.7.2
                        type_language = TypeOrLanguage::Type;
                        type_language_value = "@id";
                    }

                    // 2.7.3
                    containers.push("@set");
                }
            } else {
                // 2.7.2
                type_language = TypeOrLanguage::Type;
                type_language_value = "@id";

                // 2.7.3
                containers.push("@set");
            }

            // 2.8
            containers.push("@none");

            // 2.10
            let mut preferred_values = Vec::new();

            // 2.11
            if type_language_value == "@reverse" {
                preferred_values.push("@reverse");
            }
            let mut set = false;

            if let Some(ref item) = value {
                if (type_language_value == "@id" || type_language_value == "@reverse")
                    && item.contains_key("@id")
                {
                    // 2.12
                    let idval = item["@id"].as_str().ok_or(CompactionError::IdNotString)?;
                    let result = self._compact_iri(inverse_context, idval, None, true, true)?;
                    if self.terms.contains_key(&result) && self.terms[&result].iri_mapping == idval
                    {
                        // 2.12.1
                        preferred_values.push("@vocab");
                        preferred_values.push("@id");
                        preferred_values.push("@none");
                    } else {
                        // 2.12.2
                        preferred_values.push("@id");
                        preferred_values.push("@vocab");
                        preferred_values.push("@none");
                    }
                    set = true;
                }
            }

            if !set {
                // 2.13
                preferred_values.push(type_language_value);
                preferred_values.push("@none");
            }

            let term =
                inverse_context._select_term(iri, &containers, type_language, &preferred_values);
            if term != None {
                return Ok(term.unwrap().to_owned());
            }
        }
        // 3
        if vocab && self.vocabulary_mapping != None {
            let vocabmapping = self.vocabulary_mapping.as_ref().unwrap();
            if iri.starts_with(vocabmapping) && iri.len() > vocabmapping.len() {
                // 3.1
                let suffix = &iri[vocabmapping.len()..];
                if !self.terms.contains_key(suffix) {
                    return Ok(suffix.to_owned());
                }
            }
        }

        let mut compact_iri: Option<String> = None;
        for (term, def) in &self.terms {
            // 5.1
            if term.contains(":") {
                continue;
            }

            // 5.2
            if &def.iri_mapping == iri || !iri.starts_with(&def.iri_mapping) {
                continue;
            }

            // 5.3
            let mut candidate = term.to_owned() + ":" + &iri[def.iri_mapping.len()..];

            let is_less = compact_iri == None
                || (candidate.len() < compact_iri.as_ref().unwrap().len())
                || (candidate.len() == compact_iri.as_ref().unwrap().len()
                    && &candidate < compact_iri.as_ref().unwrap());
            if is_less
                && (!self.terms.contains_key(&candidate)
                    || (self.terms[&candidate].iri_mapping == iri && value == None))
            {
                compact_iri = Some(candidate);
            }
        }

        // 6
        if compact_iri != None {
            return Ok(compact_iri.unwrap());
        }

        if !vocab && self.base_iri != None {
            //            println!("todo transform IRI this is bad");
        }

        return Ok(iri.to_owned());
    }

    fn _compact_value<T: RemoteContextLoader>(
        &self,
        inverse_context: &InverseContext,
        active_property: Option<&str>,
        value: &Map<String, Value>,
    ) -> Result<Value, CompactionError<T>> {
        // 1
        let mut number_members = value.len();

        // 2. bleh.
        if let Some(prop) = active_property {
            if let Some(item) = self.terms.get(prop) {
                if let Some(ref container_mapping) = item.container_mapping {
                    if value.contains_key("@index") && container_mapping == "@index" {
                        number_members -= 1;
                    }
                }
            }
        }

        let mut null_lang_map = true;

        if number_members > 2 {
            // 3
            Ok(Value::Object(value.clone()))
        } else {
            if let Some(prop) = active_property {
                if let Some(item) = self.terms.get(prop) {
                    if let Some(ref type_mapping) = item.type_mapping {
                        // 4
                        if value.contains_key("@id") {
                            let idstr = match *value.get("@id").unwrap() {
                                Value::String(ref a) => a.clone(),
                                _ => return Err(CompactionError::IdNotString),
                            };

                            return Ok(if number_members == 1 && type_mapping == "@id" {
                                Value::String(self._compact_iri(
                                    inverse_context,
                                    &idstr,
                                    None,
                                    false,
                                    false,
                                )?)
                            } else if number_members == 1 && type_mapping == "@vocab" {
                                Value::String(self._compact_iri(
                                    inverse_context,
                                    &idstr,
                                    None,
                                    true,
                                    false,
                                )?)
                            } else {
                                Value::Object(value.clone())
                            });
                        } else if value.contains_key("@type") {
                            let typstr = &match value["@type"] {
                                Value::String(ref a) => a.clone(),
                                _ => return Err(CompactionError::TypeNotString),
                            };

                            if type_mapping == typstr {
                                return Ok(value["@value"].clone());
                            }
                        }
                    }

                    if let Some(ref language_mapping) = item.language_mapping {
                        if value.contains_key("@language") {
                            let langmap = &match value["@language"] {
                                Value::String(ref a) => a.clone(),
                                Value::Null => "@null".to_owned(),
                                _ => return Err(CompactionError::LanguageNotString),
                            };

                            if language_mapping == langmap {
                                return Ok(value["@value"].clone());
                            }
                        }

                        if number_members == 1 && language_mapping == "@null" {
                            return Ok(value["@value"].clone());
                        }

                        null_lang_map = language_mapping == "@null";
                    }
                }
            }

            if value.contains_key("@language") {
                // XXX I don't even know.
                let langmap = &match value["@language"] {
                    Value::String(ref a) => a.clone(),
                    Value::Null => "@null".to_owned(),
                    _ => return Err(CompactionError::LanguageNotString),
                };

                if let Some(ref lm) = self.language {
                    if lm == langmap {
                        return Ok(value["@value"].clone());
                    }
                }
            }

            if number_members == 1
                && value.contains_key("@value")
                && (!value["@value"].is_string() || self.language.is_none() || !null_lang_map)
            {
                return Ok(value["@value"].clone());
            }

            Ok(Value::Object(value.clone()))
        }
    }
}
