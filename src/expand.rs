use serde_json::{Map, Value};
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::context::Context;
use crate::creation::ContextCreationError;
use crate::RemoteContextLoader;

#[derive(Debug)]
/// Errors that may occur when expanding a JSON-LD structure.
pub enum ExpansionError<T: RemoteContextLoader> {
    /// Expanded into a list of lists
    ListOfLists,

    /// Tried to use a keyword as key for reverse map.
    InvalidReversePropertyMap,

    /// Keyword has been used before
    CollidingKeywords,

    /// Value inside a `@language` map is not a string
    InvalidLanguageMapValue,

    /// Value inside `@language` inside a value object is not a string.
    InvalidLanguageTaggedString,

    /// Value in `@index` is not a string.
    InvalidIndexValue,

    /// Object inside reverse property is value/list object.
    InvalidReversePropertyValue,

    /// `@id` value is not a string.
    InvalidIdValue,

    /// Value object contains invalid keys, or both `@type` and `@language`.
    InvalidValueObject,

    /// `@type` is not a string, or (`@value` is not a string and `@type` is set)
    InvalidTypedValue,

    /// Object contains keys other than `@set` and `@index`.
    InvalidSetObject,

    /// Object contains keys other than `@list` and `@index`.
    InvalidListObject,

    /// Object's `@type` is neither a string nor a list of string.
    InvalidTypeValue,

    /// Value object's value is a list or object.
    InvalidValueObjectValue,

    /// `@reverse` is not an object.
    InvalidReverseValue,

    /// An error when parsing the context.
    ContextExpansionError(ContextCreationError<T>),
}

impl<T: RemoteContextLoader> fmt::Display for ExpansionError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ExpansionError::ContextExpansionError(ref err) => {
                write!(f, "context expansion error: {}", err)
            }

            ExpansionError::ListOfLists => write!(f, "list of lists error"),
            ExpansionError::InvalidReversePropertyMap => write!(f, "invalid reverse property map"),
            ExpansionError::CollidingKeywords => write!(f, "colliding keywords"),
            ExpansionError::InvalidIdValue => write!(f, "invalid @id value"),
            ExpansionError::InvalidLanguageMapValue => write!(f, "invalid language map value"),
            ExpansionError::InvalidLanguageTaggedString => {
                write!(f, "invalid language-tagged string")
            }
            ExpansionError::InvalidIndexValue => write!(f, "invalid @index value"),
            ExpansionError::InvalidReversePropertyValue => {
                write!(f, "invalid reverse property value")
            }
            ExpansionError::InvalidValueObject => write!(f, "invalid value object"),
            ExpansionError::InvalidTypedValue => write!(f, "invalid typed value"),
            ExpansionError::InvalidSetObject => write!(f, "invalid set object"),
            ExpansionError::InvalidListObject => write!(f, "invalid list object"),
            ExpansionError::InvalidTypeValue => write!(f, "invalid @type value"),
            ExpansionError::InvalidValueObjectValue => write!(f, "invalid value object value"),
            ExpansionError::InvalidReverseValue => write!(f, "invalid @reverse value"),
        }
    }
}

impl<T: RemoteContextLoader> Error for ExpansionError<T> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            ExpansionError::ContextExpansionError(ref err) => Some(err),
            _ => None,
        }
    }
}

/// Helper function that expands a "primitive" JSON-LD value, aka anything that isn't
///  an array or an object.
fn _expand_value(ctx: &Context, active_property: &str, elem: &Value) -> Value {
    let mut resmap = Map::new();
    let mut set_language_mapping = false;

    if let Some(term) = ctx.terms.get(active_property) {
        if let Some(ref map) = term.type_mapping {
            if map == "@id" || map == "@vocab" {
                if let Value::String(value) = elem {
                    // 1, 2
                    resmap.insert(
                        "@id".to_owned(),
                        Value::String(ctx.expand_iri(&value, true, map == "@vocab")),
                    );
                    return Value::Object(resmap);
                }
            } else {
                resmap.insert("@type".to_owned(), Value::String(map.to_owned()));
            }
        } else if elem.is_string() {
            if let Some(ref lang) = term.language_mapping {
                if lang != "@null" {
                    resmap.insert("@language".to_owned(), Value::String(lang.to_owned()));
                }
            } else {
                set_language_mapping = true;
            }
        }
    } else if elem.is_string() {
        set_language_mapping = true;
    }

    if set_language_mapping {
        if let Some(ref lang) = ctx.language {
            resmap.insert("@language".to_owned(), Value::String(lang.to_owned()));
        }
    }

    resmap.insert("@value".to_owned(), elem.clone());

    Value::Object(resmap)
}

#[allow(clippy::cognitive_complexity)]
fn _expand<'d, 'a: 'd, 'b: 'd, 'c: 'd, T: RemoteContextLoader>(
    active_context: &'a Context,
    active_property: Option<&'b str>,
    elem: &'c Value,
) -> Pin<Box<dyn Future<Output = Result<Value, ExpansionError<T>>> + 'd + Send>> {
    Box::pin(async move {
        match elem {
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                let mut res = Vec::new();
                for item in arr {
                    let expanded_item = _expand::<T>(active_context, active_property, item).await?;

                    if expanded_item.is_array()
                        || expanded_item.as_object().map(|f| f.contains_key("@list")) == Some(true)
                    {
                        if let Some(string) = active_property {
                            if string == "@list" {
                                return Err(ExpansionError::ListOfLists);
                            }

                            if active_context
                                .terms
                                .get(string)
                                .and_then(|a| a.container_mapping.as_ref().map(|f| f as &str))
                                == Some("@list")
                            {
                                return Err(ExpansionError::ListOfLists);
                            }
                        }
                    }

                    match expanded_item {
                        Value::Array(arr) => {
                            for item in arr {
                                res.push(item)
                            }
                        }
                        Value::Null => (),
                        _ => res.push(expanded_item),
                    }
                }

                Ok(Value::Array(res))
            }

            Value::Object(ref map) => {
                let active_context = if let Some(context) = map.get("@context") {
                    let mut cloned = active_context.clone();
                    cloned
                        .process_context::<T>(context, &mut HashMap::new())
                        .await
                        .map_err(ExpansionError::ContextExpansionError)?;

                    Cow::Owned(cloned)
                } else {
                    Cow::Borrowed(active_context)
                };

                let mut result: Map<String, Value> = Map::new();

                for (key, value) in map {
                    if key == "@context" {
                        continue;
                    }

                    let prop = active_context.expand_iri(key, false, true);

                    if !prop.contains(':') && !prop.starts_with('@') {
                        // There's no relevant IRI mapping for this key and it isn't a keyword, so skip it.
                        continue;
                    }

                    if prop.starts_with('@') {
                        if active_property == Some("@reverse") {
                            return Err(ExpansionError::InvalidReversePropertyMap);
                        }

                        // Two IRIs can map to the same keyword, e.g. if you use `@id` and your context also defines `"id": "@id"`.
                        if result.contains_key(&prop) {
                            return Err(ExpansionError::CollidingKeywords);
                        }

                        let expanded_value: Value;

                        match prop.as_str() {
                            // Expand IRI values used for the ID (IDs use the same namespace
                            //  as keys, this can cause interesting issues.)
                            "@id" => {
                                if let Value::String(idval) = value {
                                    expanded_value =
                                        Value::String(active_context.expand_iri(idval, true, false))
                                } else {
                                    return Err(ExpansionError::InvalidIdValue);
                                }
                            }

                            "@type" => {
                                expanded_value = match value {
                                    Value::String(typeval) => Value::String(
                                        active_context.expand_iri(&typeval, true, true),
                                    ),
                                    Value::Array(typevals) => {
                                        let mut result = Vec::new();

                                        for a in typevals {
                                            let typeval = a
                                                .as_str()
                                                .ok_or(ExpansionError::InvalidTypeValue)?;

                                            result.push(Value::String(
                                                active_context.expand_iri(typeval, true, true),
                                            ));
                                        }

                                        Value::Array(result)
                                    }
                                    _ => return Err(ExpansionError::InvalidTypeValue),
                                }
                            }

                            "@graph" => {
                                expanded_value =
                                    _expand::<T>(active_context.as_ref(), Some(&prop), value)
                                        .await?
                            }

                            "@value" => {
                                expanded_value = match value {
                                    Value::Object(_) | Value::Array(_) => {
                                        return Err(ExpansionError::InvalidValueObjectValue)
                                    }
                                    _ => value.clone(),
                                }
                            }

                            "@language" => {
                                expanded_value = match value {
                                    Value::String(string) => Value::String(string.to_lowercase()),
                                    _ => return Err(ExpansionError::InvalidLanguageTaggedString),
                                }
                            }

                            "@index" => {
                                expanded_value = match value {
                                    Value::String(_) => value.clone(),
                                    _ => return Err(ExpansionError::InvalidIndexValue),
                                }
                            }

                            "@list" => {
                                if active_property == None || active_property == Some("@graph") {
                                    continue;
                                }

                                let tex =
                                    _expand::<T>(active_context.as_ref(), active_property, value)
                                        .await?;

                                if let Value::Object(ref obj) = tex {
                                    if obj.contains_key("@list") {
                                        return Err(ExpansionError::ListOfLists);
                                    }
                                }

                                if !tex.is_array() {
                                    expanded_value = Value::Array(vec![tex]);
                                } else {
                                    expanded_value = tex;
                                }
                            }

                            "@set" => {
                                expanded_value =
                                    _expand::<T>(active_context.as_ref(), active_property, value)
                                        .await?;
                            }

                            "@reverse" => {
                                if value.is_object() {
                                    expanded_value =
                                        _expand::<T>(active_context.as_ref(), Some(&prop), value)
                                            .await?;

                                    let mut expanded_value = match expanded_value {
                                        Value::Object(expv) => expv,
                                        _ => unreachable!(),
                                    };

                                    if let Some(reverse) = expanded_value.remove("@reverse") {
                                        if let Value::Object(ob) = reverse {
                                            for (property, mut item) in ob {
                                                if !result.contains_key(&property) {
                                                    result.insert(
                                                        property.to_string(),
                                                        Value::Array(Vec::new()),
                                                    );
                                                }

                                                if let Value::Array(ref mut val) = result[&property]
                                                {
                                                    if item.is_array() {
                                                        val.append(item.as_array_mut().unwrap())
                                                    } else {
                                                        val.push(item)
                                                    }
                                                } else {
                                                    unreachable!();
                                                }
                                            }
                                        } else {
                                            unreachable!()
                                        }
                                    }

                                    if !expanded_value.is_empty() {
                                        if !result.contains_key("@reverse") {
                                            result.insert(
                                                "@reverse".to_owned(),
                                                Value::Object(Map::new()),
                                            );
                                        }

                                        let reverse_map =
                                            result["@reverse"].as_object_mut().unwrap();

                                        for (property, items) in expanded_value {
                                            let items = match items {
                                                Value::Array(arr) => arr,
                                                _ => unreachable!(),
                                            };

                                            for item in items {
                                                if let Value::Object(ref vobj) = item {
                                                    if vobj.contains_key("@value")
                                                        || vobj.contains_key("@list")
                                                    {
                                                        return Err(ExpansionError::InvalidReversePropertyValue);
                                                    }
                                                }

                                                if !reverse_map.contains_key(&property) {
                                                    reverse_map.insert(
                                                        property.to_owned(),
                                                        Value::Array(Vec::new()),
                                                    );
                                                }

                                                let map =
                                                    reverse_map[&property].as_array_mut().unwrap();

                                                map.push(item)
                                            }
                                        }
                                    }

                                    continue;
                                } else {
                                    return Err(ExpansionError::InvalidReverseValue);
                                }
                            }

                            _ => continue,
                        }

                        result.insert(prop, expanded_value);
                    } else {
                        let container_mapping = active_context
                            .terms
                            .get(key)
                            .and_then(|f| f.container_mapping.as_ref())
                            .map(String::as_str);
                        let mut expanded_value = match (container_mapping, value) {
                            (Some("@language"), Value::Object(obj)) => {
                                let mut new_arr = Vec::new();

                                for (language, language_value) in obj {
                                    let language = language.to_lowercase();

                                    let language_values = match *language_value {
                                        Value::String(ref string) => {
                                            vec![Value::String(string.clone())]
                                        }
                                        Value::Array(ref arr) => arr.clone(),
                                        Value::Null => continue,
                                        _ => return Err(ExpansionError::InvalidLanguageMapValue),
                                    };

                                    for val in language_values {
                                        if val.is_null() {
                                            continue;
                                        }

                                        if !val.is_string() {
                                            return Err(ExpansionError::InvalidLanguageMapValue);
                                        }

                                        let mut map = Map::new();
                                        map.insert("@value".to_string(), val);
                                        map.insert(
                                            "@language".to_string(),
                                            Value::String(language.to_string()),
                                        );

                                        new_arr.push(Value::Object(map))
                                    }
                                }

                                Value::Array(new_arr)
                            }

                            (Some("@index"), Value::Object(obj)) => {
                                let mut ar = Vec::new();

                                for (index, index_value) in obj {
                                    let index_value = match index_value {
                                        Value::Array(_) => Cow::Borrowed(index_value),
                                        _ => Cow::Owned(Value::Array(vec![index_value.clone()])),
                                    };

                                    let index_value = _expand::<T>(
                                        active_context.as_ref(),
                                        Some(&key),
                                        index_value.as_ref(),
                                    )
                                    .await?;

                                    if let Value::Array(var) = index_value {
                                        for mut item in var {
                                            if !item.as_object().unwrap().contains_key("@index") {
                                                item.as_object_mut().unwrap().insert(
                                                    "@index".to_owned(),
                                                    Value::String(index.to_owned()),
                                                );
                                            }

                                            ar.push(item);
                                        }
                                    } else {
                                        unreachable!();
                                    }
                                }

                                Value::Array(ar)
                            }

                            _ => _expand::<T>(active_context.as_ref(), Some(&key), value).await?,
                        };

                        if expanded_value.is_null() {
                            continue;
                        }

                        if let Some(item) = active_context.terms.get(key) {
                            if item.container_mapping.as_ref().map(String::as_str) == Some("@list")
                            {
                                match expanded_value {
                                    Value::Object(obj) => {
                                        if !obj.contains_key("@list") {
                                            expanded_value = Value::Array(vec![Value::Object(obj)]);
                                            let mut map = Map::new();
                                            map.insert("@list".to_string(), expanded_value);
                                            expanded_value = Value::Object(map);
                                        } else {
                                            expanded_value = Value::Object(obj);
                                        }
                                    }

                                    Value::Array(_) => {
                                        let mut map = Map::new();
                                        map.insert("@list".to_string(), expanded_value);
                                        expanded_value = Value::Object(map);
                                    }

                                    _ => unreachable!(),
                                }
                            }

                            if item.reverse {
                                if !result.contains_key("@reverse") {
                                    result
                                        .insert("@reverse".to_string(), Value::Object(Map::new()));
                                }

                                let reverse_map = result["@reverse"].as_object_mut().unwrap();

                                let mut ar = if let Value::Array(array) = expanded_value {
                                    array
                                } else {
                                    vec![expanded_value]
                                };

                                for item in &ar {
                                    if let Value::Object(ref rm) = item {
                                        if rm.contains_key("@value") || rm.contains_key("@list") {
                                            return Err(
                                                ExpansionError::InvalidReversePropertyValue,
                                            );
                                        }
                                    }
                                }

                                if !reverse_map.contains_key(&prop) {
                                    reverse_map.insert(prop, Value::Array(ar));
                                } else if let Value::Array(ref mut arr) = reverse_map[&prop] {
                                    arr.append(&mut ar);
                                } else {
                                    unreachable!();
                                }

                                continue;
                            }
                        }

                        if !expanded_value.is_array() {
                            expanded_value = Value::Array(vec![expanded_value]);
                        }

                        if !result.contains_key(&prop) {
                            result.insert(prop, expanded_value);
                        } else if let Value::Array(ref mut arr) = result[&prop] {
                            arr.append(expanded_value.as_array_mut().unwrap());
                        } else {
                            unreachable!();
                        }
                    }
                }

                if let Some(val) = result.get("@value") {
                    for key in result.keys() {
                        if key == "@value"
                            || key == "@language"
                            || key == "@type"
                            || key == "@index"
                        {
                            continue;
                        }

                        return Err(ExpansionError::InvalidValueObject);
                    }

                    if result.contains_key("@type") && result.contains_key("@language") {
                        return Err(ExpansionError::InvalidValueObject);
                    }

                    match val {
                        Value::Null => return Ok(Value::Null),
                        Value::String(_) => (),
                        _ => {
                            if result.contains_key("@language") {
                                return Err(ExpansionError::InvalidTypedValue);
                            }
                        }
                    }

                    if let Some(typeval) = result.get("@type") {
                        if !typeval.is_string() {
                            return Err(ExpansionError::InvalidTypedValue);
                        }
                    }
                } else if let Some(typeval) = result.get_mut("@type") {
                    if typeval.is_string() {
                        let val = std::mem::replace(typeval, Value::Null);
                        *typeval = Value::Array(vec![val]);
                    }
                } else if let Some(set) = result.remove("@set") {
                    // We can only have one other item in the result if `@set` is used, namely `@index`.
                    if result.len() > 1 || (result.len() == 1 && !result.contains_key("@index")) {
                        return Err(ExpansionError::InvalidSetObject);
                    }

                    return Ok(set);
                } else if result.contains_key("@list")
                    && (result.len() > 2 || (result.len() == 2 && !result.contains_key("@index")))
                {
                    return Err(ExpansionError::InvalidListObject);
                }

                if result.len() == 1 && result.contains_key("@language") {
                    Ok(Value::Null)
                } else if active_property == None || active_property == Some("@graph") {
                    if result.is_empty()
                        || result.contains_key("@value")
                        || result.contains_key("@list")
                        || (result.len() == 1 && result.contains_key("@id"))
                    {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Object(result))
                    }
                } else {
                    Ok(Value::Object(result))
                }
            }

            _ => match active_property {
                Some("@graph") | None => Ok(Value::Null),
                Some(active_property) => Ok(_expand_value(active_context, active_property, elem)),
            },
        }
    })
}

pub async fn expand<T: RemoteContextLoader>(
    ctx: &Context,
    elem: &Value,
) -> Result<Value, ExpansionError<T>> {
    let mut val = _expand::<T>(ctx, None, elem).await?;

    if val
        .as_object()
        .map(|f| f.len() == 1 && f.contains_key("@graph"))
        == Some(true)
    {
        if let Value::Object(mut objd) = val {
            val = objd.remove("@graph").unwrap();
        }
    }

    if val.is_null() {
        Ok(Value::Array(Vec::new()))
    } else if !val.is_array() {
        Ok(Value::Array(vec![val]))
    } else {
        Ok(val)
    }
}
