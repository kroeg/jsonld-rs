use serde_json::{Map, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::From;
use std::error::Error;
use std::fmt;

use crate::context::{Context, Term};
use crate::creation::ContextCreationError;
use crate::expand::ExpansionError;
use crate::RemoteContextLoader;

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

impl<T: RemoteContextLoader> From<ContextCreationError<T>> for CompactionError<T> {
    fn from(err: ContextCreationError<T>) -> Self {
        Self::ContextError(err)
    }
}

impl<T: RemoteContextLoader> From<ExpansionError<T>> for CompactionError<T> {
    fn from(err: ExpansionError<T>) -> Self {
        Self::ExpansionError(err)
    }
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

    fn cause(&self) -> Option<&dyn Error> {
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

/// The JSON-LD API specification requires that terms first get sorted
/// by length, and then by contents. This is the inverse of String's Ord
/// implementation, so we manually implement a sorting function here.
fn _sort_term((a, _): &(&String, &Term), (b, _): &(&String, &Term)) -> Ordering {
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
        let mut result = InverseContext {
            container_map: HashMap::new(),
        };

        let default_language = ctx.language.as_ref().map_or("@none", String::as_str);

        let mut term_order: Vec<_> = ctx.terms.iter().collect();
        term_order.sort_by(_sort_term);

        for (term, value) in term_order {
            let container = value
                .container_mapping
                .as_ref()
                .map_or("@none", String::as_str);
            let iri = &value.iri_mapping;

            if !result.container_map.contains_key(iri) {
                result.container_map.insert(iri.to_owned(), HashMap::new());
            }

            let container_map = result.container_map.get_mut(iri).unwrap();
            if !container_map.contains_key(container) {
                container_map.insert(
                    container.to_owned(),
                    TypeLanguageMap {
                        type_map: HashMap::new(),
                        language_map: HashMap::new(),
                    },
                );
            }

            let type_language_map = container_map.get_mut(container).unwrap();

            if value.reverse {
                let type_map = &mut type_language_map.type_map;
                if !type_map.contains_key("@reverse") {
                    type_map.insert("@reverse".to_owned(), term.to_owned());
                }
            } else if let Some(ref type_mapping) = value.type_mapping {
                let type_map = &mut type_language_map.type_map;
                if !type_map.contains_key(type_mapping) {
                    type_map.insert(type_mapping.to_owned(), term.to_owned());
                }
            } else if let Some(ref language_mapping) = value.language_mapping {
                let language_map = &mut type_language_map.language_map;
                if !language_map.contains_key(language_mapping) {
                    language_map.insert(language_mapping.to_owned(), term.to_owned());
                }
            } else {
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

            // Specification does not actually mention this, but it is
            //  necessary for proper handling of empty lists.
            // IRI compaction, if a @list is empty, will fall back to
            //  `@none` always. Place a random term there for sensible
            //  compaction. This is valid, as the term will expand
            //  back into the same IRI.
            let type_map = &type_language_map.type_map;
            let language_map = &mut type_language_map.language_map;
            if container == "@list" && !language_map.contains_key("@none") {
                let term = if !language_map.is_empty() {
                    language_map.iter().next().unwrap().1.clone()
                } else {
                    type_map.iter().next().unwrap().1.clone()
                };

                language_map.insert("@none".to_owned(), term);
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

pub async fn compact<T: RemoteContextLoader>(
    context: &Value,
    element: Value,
    compact_arrays: bool,
) -> Result<Value, CompactionError<T>> {
    let mut ctx = Context::new();
    ctx.process_context::<T>(context, &mut HashMap::new())
        .await?;

    let inverse = InverseContext::new(&ctx);
    let mut res = _compact(&ctx, &inverse, None, &element, compact_arrays)?;
    if let Some(arr) = res.as_array_mut() {
        // *shakes fist at single canonical implementation*
        //  (this is unspecified but necessary for the JSON-LD API tests.)
        if compact_arrays && arr.is_empty() {
            res = Value::Object(Map::new());
        } else if compact_arrays && arr.len() == 1 {
            res = arr.remove(0);
        } else {
            let mut map = Map::new();
            map.insert(
                // The specification does not imply vocab = true here. Again.
                //  *shakes other fist too*
                _compact_iri(&ctx, &inverse, "@graph", None, true, false)?,
                res,
            );

            res = Value::Object(map);
        }
    }

    if res.is_object()
        && !context.is_null()
        && (!context.is_object() || !context.as_object().unwrap().is_empty())
    {
        res.as_object_mut()
            .unwrap()
            .insert("@context".to_owned(), context.clone());
    }

    Ok(res)
}

#[allow(clippy::cognitive_complexity)]
fn _compact<T: RemoteContextLoader>(
    active_context: &Context,
    inverse_context: &InverseContext,
    active_property: Option<&str>,
    element: &Value,
    compact_arrays: bool,
) -> Result<Value, CompactionError<T>> {
    match *element {
        Value::Array(ref arr) => {
            let mut result = Vec::new();
            for item in arr {
                let compacted = _compact(
                    active_context,
                    inverse_context,
                    active_property,
                    item,
                    compact_arrays,
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
            if obj.contains_key("@value") || obj.contains_key("@id") {
                let res = _compact_value(active_context, inverse_context, active_property, obj)?;
                if !res.is_array() && !res.is_object() {
                    return Ok(res);
                }
            }

            let inside_reverse = active_property == Some("@reverse");

            let mut result = Map::new();

            for (expanded_property, expanded_value) in obj {
                if expanded_property == "@id" || expanded_property == "@type" {
                    let compacted_value = match *expanded_value {
                        Value::String(ref strval) => Value::String(_compact_iri(
                            active_context,
                            inverse_context,
                            strval,
                            None,
                            expanded_property == "@type",
                            false,
                        )?),

                        Value::Array(ref arval) => {
                            if expanded_property != "@type" {
                                return Err(CompactionError::IdNotString);
                            }

                            let mut res = Vec::new();
                            for item in arval {
                                match *item {
                                    Value::String(ref stv) => {
                                        res.push(Value::String(_compact_iri(
                                            active_context,
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

                    let alias = _compact_iri(
                        active_context,
                        inverse_context,
                        expanded_property,
                        None,
                        true,
                        false,
                    )?;
                    result.insert(alias, compacted_value);

                    continue;
                } else if expanded_property == "@reverse" {
                    let compacted_value = _compact(
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
                                    if (term.container_mapping.as_ref().map(String::as_str)
                                        == Some("@set")
                                        || !compact_arrays)
                                        && !value.is_array()
                                    {
                                        value = Value::Array(vec![value]);
                                    }

                                    if result.contains_key(&property) {
                                        let val = result.get_mut(&property).unwrap();

                                        if !val.is_array() {
                                            let tmp = std::mem::replace(val, Value::Null);
                                            *val = Value::Array(vec![tmp]);
                                        }

                                        if value.is_array() {
                                            val.as_array_mut()
                                                .unwrap()
                                                .append(value.as_array_mut().unwrap());
                                        } else {
                                            val.as_array_mut().unwrap().push(value);
                                        }

                                        continue;
                                    } else {
                                        result.insert(property, value);
                                        continue;
                                    }
                                }
                            }

                            new_map.insert(property, value);
                        }

                        if !new_map.is_empty() {
                            let alias = _compact_iri(
                                active_context,
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

                if expanded_property == "@index"
                    && active_property
                        .and_then(|f| active_context.terms.get(f))
                        .and_then(|f| f.container_mapping.as_ref())
                        .map(String::as_str)
                        == Some("@index")
                {
                    continue;
                }

                if expanded_property == "@index"
                    || expanded_property == "@value"
                    || expanded_property == "@language"
                {
                    let alias = _compact_iri(
                        active_context,
                        inverse_context,
                        expanded_property,
                        None,
                        true,
                        false,
                    )?;

                    result.insert(alias, expanded_value.clone());
                    continue;
                }

                if expanded_value.as_array().map(Vec::is_empty) == Some(true) {
                    let item_active_property = _compact_iri(
                        active_context,
                        inverse_context,
                        expanded_property,
                        None,
                        true,
                        inside_reverse,
                    )?;

                    if !result.contains_key(&item_active_property) {
                        result.insert(item_active_property, Value::Array(Vec::new()));
                    } else {
                        let val = Value::Array(vec![result.remove(&item_active_property).unwrap()]);
                        result.insert(item_active_property, val);
                    }
                }

                for expanded_item in expanded_value.as_array().unwrap() {
                    let item_active_property = _compact_iri(
                        active_context,
                        inverse_context,
                        expanded_property,
                        expanded_item.as_object(),
                        true,
                        inside_reverse,
                    )?;

                    let container = active_context
                        .terms
                        .get(&item_active_property)
                        .and_then(|f| f.container_mapping.as_ref())
                        .map(String::as_str);

                    let data = expanded_item.as_object().unwrap();
                    let to_pass = data.get("@list").unwrap_or(expanded_item);

                    let mut compacted_item = _compact(
                        active_context,
                        inverse_context,
                        Some(&item_active_property),
                        to_pass,
                        compact_arrays,
                    )?;

                    if data.contains_key("@list") {
                        if !compacted_item.is_array() {
                            compacted_item = Value::Array(vec![compacted_item]);
                        }

                        if container != Some("@list") {
                            let mut m = Map::new();
                            m.insert(
                                _compact_iri(
                                    active_context,
                                    inverse_context,
                                    "@list",
                                    compacted_item.as_object(),
                                    true,
                                    false,
                                )?,
                                compacted_item,
                            );
                            if data.contains_key("@index") {
                                m.insert(
                                    _compact_iri(
                                        active_context,
                                        inverse_context,
                                        "@index",
                                        None,
                                        true,
                                        false,
                                    )?,
                                    data["@index"].clone(),
                                );
                            }

                            compacted_item = Value::Object(m);
                        } else if result.contains_key(&item_active_property) {
                            return Err(CompactionError::CompactionToListOfLists);
                        }
                    }

                    if container == Some("@language") || container == Some("@index") {
                        if !result.contains_key(&item_active_property) {
                            let map = Map::new();
                            result.insert(item_active_property.clone(), Value::Object(map));
                        }

                        let map_object = result
                            .get_mut(&item_active_property)
                            .and_then(|f| f.as_object_mut())
                            .unwrap();

                        if container == Some("@language")
                            && compacted_item.as_object().map(|f| f.contains_key("@value"))
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
                                val = Value::Array(vec![val, compacted_item]);
                            }

                            map_object.insert(map_key.to_owned(), val);
                        }
                    } else {
                        if (!compact_arrays
                            || container == Some("@set")
                            || container == Some("@list")
                            || expanded_property == "@list"
                            || expanded_property == "@graph")
                            && !compacted_item.is_array()
                        {
                            compacted_item = Value::Array(vec![compacted_item]);
                        }

                        if !result.contains_key(&item_active_property) {
                            result.insert(item_active_property, compacted_item);
                        } else {
                            let mut val = result.remove(&item_active_property).unwrap();
                            let mut varr = if let Value::Array(ar) = compacted_item {
                                ar
                            } else {
                                vec![compacted_item]
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

#[allow(clippy::cognitive_complexity)]
fn _compact_iri<T: RemoteContextLoader>(
    context: &Context,
    inverse_context: &InverseContext,
    iri: &str,
    value: Option<&Map<String, Value>>,
    vocab: bool,
    reverse: bool,
) -> Result<String, CompactionError<T>> {
    if vocab && inverse_context.container_map.contains_key(iri) {
        let default_language = context.language.as_ref().map_or("@none", |f| &f);
        let mut containers = Vec::new();
        let mut type_language = TypeOrLanguage::Language;
        let mut type_language_value = "@null";

        if let Some(ref item) = value {
            if item.contains_key("@index") {
                containers.push("@index");
            }
        }

        if reverse {
            type_language = TypeOrLanguage::Type;
            type_language_value = "@reverse";
            containers.push("@set");
        } else if let Some(ref item) = value {
            if let Some(lv) = item.get("@list") {
                if !item.contains_key("@index") {
                    containers.push("@list");
                }

                let list = lv.as_array().ok_or(CompactionError::ListObjectNotArray)?;

                let mut common_type = None;
                let mut common_language = None;

                if list.is_empty() {
                    common_language = Some(default_language)
                }

                for vitem in list {
                    let mut item_language: Option<&str> = None;
                    let mut item_type: Option<&str> = None;

                    if let Value::Object(ref item) = *vitem {
                        if item.contains_key("@value") {
                            if item.contains_key("@language") {
                                match item["@language"] {
                                    Value::String(ref string) => {
                                        item_language = Some(string);
                                    }
                                    Value::Null => item_language = Some("@null"),
                                    _ => return Err(CompactionError::LanguageNotString),
                                };
                            } else if item.contains_key("@type") {
                                item_type = Some(
                                    item["@type"]
                                        .as_str()
                                        .ok_or(CompactionError::TypeNotString)?,
                                );
                            } else {
                                item_language = Some("@null")
                            }
                        } else {
                            item_type = Some("@id");
                        }

                        if common_language == None {
                            common_language = item_language;
                        } else if item_language != common_language && item.contains_key("@value") {
                            common_language = Some("@none");
                        }

                        if common_type == None {
                            common_type = item_type;
                        } else if common_type != item_type {
                            common_type = Some("@none")
                        }

                        if common_type == Some("@none") && common_language == Some("@none") {
                            break;
                        }
                    } else {
                        return Err(CompactionError::ListItemNotObject);
                    }
                }

                let common_language = match common_language {
                    None => "@none",
                    Some(val) => val,
                };

                let common_type = match common_type {
                    None => "@none",
                    Some(val) => val,
                };

                if common_type != "@none" {
                    type_language = TypeOrLanguage::Type;
                    type_language_value = common_type
                } else {
                    type_language_value = common_language
                }
            } else {
                if item.contains_key("@value") {
                    if item.contains_key("@language") && !item.contains_key("@index") {
                        type_language_value = item["@language"]
                            .as_str()
                            .ok_or(CompactionError::LanguageNotString)?;
                        containers.push("@language");
                    } else if item.contains_key("@type") {
                        type_language = TypeOrLanguage::Type;
                        type_language_value = item["@type"]
                            .as_str()
                            .ok_or(CompactionError::TypeNotString)?;
                    }
                } else {
                    type_language = TypeOrLanguage::Type;
                    type_language_value = "@id";
                }

                containers.push("@set");
            }
        } else {
            type_language = TypeOrLanguage::Type;
            type_language_value = "@id";

            containers.push("@set");
        }

        containers.push("@none");

        let mut preferred_values = Vec::new();
        if type_language_value == "@reverse" {
            preferred_values.push("@reverse");
        }

        let mut set = false;
        if let Some(ref item) = value {
            if (type_language_value == "@id" || type_language_value == "@reverse")
                && item.contains_key("@id")
            {
                let idval = item["@id"].as_str().ok_or(CompactionError::IdNotString)?;
                let result = _compact_iri(context, inverse_context, idval, None, true, true)?;
                if context.terms.contains_key(&result)
                    && context.terms[&result].iri_mapping == idval
                {
                    preferred_values.push("@vocab");
                    preferred_values.push("@id");
                    preferred_values.push("@none");
                } else {
                    preferred_values.push("@id");
                    preferred_values.push("@vocab");
                    preferred_values.push("@none");
                }
                set = true;
            }
        }

        if !set {
            preferred_values.push(type_language_value);
            preferred_values.push("@none");
        }

        let term = inverse_context._select_term(iri, &containers, type_language, &preferred_values);
        if let Some(term) = term {
            return Ok(term.to_owned());
        }
    }

    if vocab && context.vocabulary_mapping != None {
        let vocabmapping = context.vocabulary_mapping.as_ref().unwrap();
        if iri.starts_with(vocabmapping) && iri.len() > vocabmapping.len() {
            let suffix = &iri[vocabmapping.len()..];
            if !context.terms.contains_key(suffix) {
                return Ok(suffix.to_owned());
            }
        }
    }

    let mut compact_iri: Option<String> = None;
    for (term, def) in &context.terms {
        if term.contains(':') {
            continue;
        }

        if def.iri_mapping == iri || !iri.starts_with(&def.iri_mapping) {
            continue;
        }

        let candidate = term.to_owned() + ":" + &iri[def.iri_mapping.len()..];

        let is_less = match compact_iri {
            None => true,
            Some(ref compact_iri) => {
                (candidate.len() < compact_iri.len())
                    || (candidate.len() == compact_iri.len() && candidate < *compact_iri)
            }
        };

        if is_less
            && (!context.terms.contains_key(&candidate)
                || (context.terms[&candidate].iri_mapping == iri && value == None))
        {
            compact_iri = Some(candidate);
        }
    }

    if let Some(compact_iri) = compact_iri {
        return Ok(compact_iri);
    }

    if !vocab && context.base_iri != None {
        // Technically, the IRI should be compacted relatively to the base IRI.
        // I have not had any use for this and as such have been too lazy to
        //  implement it.
    }

    Ok(iri.to_owned())
}

fn _compact_value<T: RemoteContextLoader>(
    context: &Context,
    inverse_context: &InverseContext,
    active_property: Option<&str>,
    value: &Map<String, Value>,
) -> Result<Value, CompactionError<T>> {
    let mut number_members = value.len();

    if let Some(prop) = active_property {
        if let Some(item) = context.terms.get(prop) {
            if let Some(ref container_mapping) = item.container_mapping {
                if value.contains_key("@index") && container_mapping == "@index" {
                    number_members -= 1;
                }
            }
        }
    }

    let mut null_lang_map = true;

    if number_members > 2 {
        Ok(Value::Object(value.clone()))
    } else {
        if let Some(prop) = active_property {
            if let Some(item) = context.terms.get(prop) {
                if let Some(ref type_mapping) = item.type_mapping {
                    if value.contains_key("@id") {
                        let idstr = match *value.get("@id").unwrap() {
                            Value::String(ref a) => a.clone(),
                            _ => return Err(CompactionError::IdNotString),
                        };

                        return Ok(if number_members == 1 && type_mapping == "@id" {
                            Value::String(_compact_iri(
                                context,
                                inverse_context,
                                &idstr,
                                None,
                                false,
                                false,
                            )?)
                        } else if number_members == 1 && type_mapping == "@vocab" {
                            Value::String(_compact_iri(
                                context,
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
            let langmap = &match value["@language"] {
                Value::String(ref a) => a,
                Value::Null => "@null",
                _ => return Err(CompactionError::LanguageNotString),
            };

            if context.language.as_ref().map(String::as_str) == Some(langmap) {
                return Ok(value["@value"].clone());
            }
        }

        if number_members == 1
            && value.contains_key("@value")
            && (!value["@value"].is_string() || context.language.is_none() || !null_lang_map)
        {
            return Ok(value["@value"].clone());
        }

        Ok(Value::Object(value.clone()))
    }
}
