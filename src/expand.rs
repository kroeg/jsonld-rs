use super::context::Context;
use serde_json::{Map, Value};
use std::fmt;
use std::error::Error;
use std::collections::HashSet;
use super::creation::ContextCreationError;

#[derive(Debug)]
/// Errors that may occur when expanding a JSON-LD structure.
pub enum ExpansionError {
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

    /// `@is` value is not a string.
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
    ContextExpansionError(ContextCreationError),
}

impl fmt::Display for ExpansionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ExpansionError::ContextExpansionError(ref err) => {
                write!(f, "context expansion error: {}", err)
            },
            _ => f.write_str(self.description())
        }
    }
}

impl Error for ExpansionError {
    fn description(&self) -> &str {
        match *self {
            ExpansionError::ListOfLists => "list of lists error",
            ExpansionError::InvalidReversePropertyMap => "invalid reverse property map",
            ExpansionError::CollidingKeywords => "colliding keywords",
            ExpansionError::InvalidIdValue => "invalid @id value",
            ExpansionError::InvalidLanguageMapValue => "invalid language map value",
            ExpansionError::InvalidLanguageTaggedString => "invalid language-tagged string",
            ExpansionError::InvalidIndexValue => "invalid @index value",
            ExpansionError::InvalidReversePropertyValue => "invalid reverse property value",
            ExpansionError::InvalidValueObject => "invalid value object",
            ExpansionError::InvalidTypedValue => "invalid typed value",
            ExpansionError::InvalidSetObject => "invalid set object",
            ExpansionError::InvalidListObject => "invalid list object",
            ExpansionError::InvalidTypeValue => "invalid @type value",
            ExpansionError::InvalidValueObjectValue => "invalid value object value",
            ExpansionError::InvalidReverseValue => "invalid @reverse value",
            ExpansionError::ContextExpansionError(_) => "Failed to expand context"
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ExpansionError::ContextExpansionError(ref err) => Some(err),
            _ => None
        }
    }
}

fn _array_or_list_object(elem: &Value) -> bool {
    elem.is_array() || (elem.is_object() && elem.as_object().unwrap().contains_key("@list"))
}

impl Context {
    fn _expand_value(&self, active_property: &str, elem: Value) -> Value {
        let mut resmap = Map::new();
        let mut set_language_mapping = false;

        if let Some(term) = self.terms.get(active_property) {
            if let Some(ref map) = term.type_mapping {
                if map == "@id" || map == "@vocab" {
                    if let Value::String(value) = elem {
                        // 1, 2
                        resmap.insert(
                            "@id".to_owned(),
                            Value::String(self.expand_iri(&value, true, map == "@vocab")),
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
            if let Some(ref lang) = self.language {
                resmap.insert("@language".to_owned(), Value::String(lang.to_owned()));
            }
        }

        resmap.insert("@value".to_owned(), elem);

        Value::Object(resmap)
    }

    fn _expand(
        active_context: &Context,
        active_property: Option<&str>,
        elem: Value,
    ) -> Result<Value, ExpansionError> {
        match elem {
            // 1
            Value::Null => Ok(Value::Null),
            Value::Array(arr) => {
                // 3
                let mut res = Vec::new();
                for item in arr {
                    // 3.2.1
                    let expanded_item = Context::_expand(active_context, active_property, item)?;

                    // 3.2.2
                    if _array_or_list_object(&expanded_item) {
                        if let Some(string) = active_property {
                            if string == "@list" {
                                return Err(ExpansionError::ListOfLists);
                            }

                            if let Some(data) = active_context
                                .terms
                                .get(string)
                                .and_then(|a| a.container_mapping.as_ref())
                            {
                                if data == "@list" {
                                    return Err(ExpansionError::ListOfLists);
                                }
                            }
                        }
                    }

                    // 3.2.3
                    match expanded_item {
                        Value::Array(arr) => for item in arr {
                            res.push(item)
                        },
                        Value::Null => {}
                        _ => res.push(expanded_item),
                    }
                }

                // 3.3
                Ok(Value::Array(res))
            }

            // 4
            Value::Object(map) => {
                // 5
                let mut _new_context = None;

                let active_context = if map.contains_key("@context") {
                    // ugly hack to make the active_context survive
                    let ctx = active_context
                        .process_context(&map["@context"], &mut HashSet::new())
                        .map_err(|e| ExpansionError::ContextExpansionError(e))?;
                    _new_context = Some(ctx);
                    _new_context.as_ref().unwrap()
                } else {
                    active_context
                };

                // 6
                let mut result: Map<String, Value> = Map::new();

                // 7
                for (key, mut value) in map {
                    // 7.1
                    if key == "@context" {
                        continue;
                    }

                    // 7.2
                    let prop = active_context.expand_iri(&key, false, true);

                    // 7.3
                    if !prop.contains(":") && !prop.starts_with("@") {
                        continue;
                    }

                    // 7.4
                    if prop.starts_with("@") {
                        let expanded_value: Value;

                        // 7.4.1
                        if active_property == Some("@reverse") {
                            return Err(ExpansionError::InvalidReversePropertyMap);
                        }

                        // 7.4.2
                        if result.contains_key(&prop) {
                            return Err(ExpansionError::CollidingKeywords);
                        }

                        match prop.as_str() {
                            // 7.4.3
                            "@id" => {
                                if let Value::String(idval) = value {
                                    expanded_value = Value::String(active_context.expand_iri(
                                        &idval,
                                        true,
                                        false,
                                    ))
                                } else {
                                    return Err(ExpansionError::InvalidIdValue);
                                }
                            }

                            // 7.4.4
                            "@type" => {
                                expanded_value = match value {
                                    Value::String(typeval) => Value::String(
                                        active_context.expand_iri(&typeval, true, true),
                                    ),
                                    Value::Array(typevals) => {
                                        let mut result = Vec::new();

                                        for a in typevals {
                                            if let Value::String(ref aval) = a {
                                                result.push(Value::String(
                                                    active_context.expand_iri(aval, true, true),
                                                ));
                                            } else {
                                                return Err(ExpansionError::InvalidTypeValue);
                                            }
                                        }

                                        Value::Array(result)
                                    }
                                    _ => return Err(ExpansionError::InvalidTypeValue),
                                }
                            }

                            // 7.4.5
                            "@graph" => {
                                expanded_value =
                                    Context::_expand(active_context, Some(&prop), value)?
                            }

                            // 7.4.6
                            "@value" => {
                                expanded_value = match value {
                                    Value::Object(_) => {
                                        return Err(ExpansionError::InvalidValueObjectValue)
                                    }
                                    Value::Array(_) => {
                                        return Err(ExpansionError::InvalidValueObjectValue)
                                    }
                                    _ => value,
                                }
                            }

                            // 7.4.7
                            "@language" => {
                                expanded_value = match value {
                                    Value::String(string) => Value::String(string.to_lowercase()),
                                    _ => return Err(ExpansionError::InvalidLanguageTaggedString),
                                }
                            }

                            // 7.4.8
                            "@index" => {
                                expanded_value = match value {
                                    Value::String(_) => value,
                                    _ => return Err(ExpansionError::InvalidIndexValue),
                                }
                            }

                            // 7.4.9
                            "@list" => {
                                // 7.4.9.1
                                if active_property == None || active_property == Some("@graph") {
                                    continue;
                                }

                                // 7.4.9.2
                                let tex = Context::_expand(active_context, active_property, value)?;

                                // 7.4.9.3
                                if let Value::Object(ref obj) = tex {
                                    if obj.contains_key("@list") {
                                        return Err(ExpansionError::ListOfLists);
                                    }
                                }

                                if !tex.is_array() {
                                    // XXX test compact-0004
                                    expanded_value = Value::Array(vec![tex].into());
                                } else {
                                    expanded_value = tex;
                                }
                            }

                            // 7.4.10
                            "@set" => {
                                expanded_value =
                                    Context::_expand(active_context, active_property, value)?;
                            }

                            // 7.4.11
                            "@reverse" => {
                                if let Value::Object(obj) = value {
                                    // 7.4.11.1
                                    expanded_value = Context::_expand(
                                        active_context,
                                        Some(&prop),
                                        Value::Object(obj.clone()),
                                    )?;

                                    if let Value::Object(mut expv) = expanded_value {
                                        // 7.4.11.2
                                        if let Some(reverse) = expv.remove("@reverse") {
                                            if let Value::Object(ob) = reverse {
                                                for (property, mut item) in ob {
                                                    // 7.4.11.2.1
                                                    if !result.contains_key(&property) {
                                                        result.insert(
                                                            property.to_string(),
                                                            Value::Array(Vec::new()),
                                                        );
                                                    }

                                                    // 7.4.11.2.2
                                                    if let Value::Array(ref mut val) =
                                                        result[&property]
                                                    {
                                                        if item.is_array() {
                                                            // XXX no idea if this is right
                                                            val.append(item.as_array_mut().unwrap())
                                                        } else {
                                                            val.push(item)
                                                        }
                                                    }
                                                }
                                            } else {
                                                unreachable!()
                                            }
                                        }

                                        // 7.4.11.3
                                        if !expv.is_empty() {
                                            // 7.4.11.3.1
                                            if !result.contains_key("@reverse") {
                                                result.insert(
                                                    "@reverse".to_owned(),
                                                    Value::Object(Map::new()),
                                                );
                                            }

                                            // 7.4.11.3.2
                                            let mut reverse_map =
                                                result["@reverse"].as_object_mut().unwrap();

                                            // 7.4.11.3.3
                                            for (property, items) in expv {
                                                // 7.4.11.3.3.1
                                                for item in items.as_array().unwrap() {
                                                    if let Value::Object(ref vobj) = *item {
                                                        // 7.4.11.3.3.1.1
                                                        if vobj.contains_key("@value")
                                                            || vobj.contains_key("@list")
                                                        {
                                                            return Err(ExpansionError::InvalidReversePropertyValue);
                                                        }
                                                    }

                                                    // 7.4.11.3.3.1.2
                                                    if !reverse_map.contains_key(&property) {
                                                        reverse_map.insert(
                                                            property.to_owned(),
                                                            Value::Array(Vec::new()),
                                                        );
                                                    }

                                                    let map = reverse_map[&property]
                                                        .as_array_mut()
                                                        .unwrap();

                                                    // 7.4.11.3.3.1.3
                                                    map.push(item.clone())
                                                }
                                            }
                                        }

                                        // 7.4.11.4
                                        continue;
                                    } else {
                                        unreachable!()
                                    }
                                } else {
                                    return Err(ExpansionError::InvalidReverseValue);
                                }
                            }

                            _ => continue,
                        }

                        // 7.4.12, provably non-null! :D
                        result.insert(prop, expanded_value);
                    } else {
                        let mut expanded_value: Option<Value> = None;

                        if let Some(item) = active_context.terms.get(&key) {
                            if let Some(ref map) = item.container_mapping {
                                if map == "@language" && value.is_object() {
                                    // 7.5
                                    let obj = value.as_object().unwrap();
                                    let mut new_arr = Vec::new();

                                    for (language, language_value) in obj {
                                        let language = language.to_lowercase();

                                        let language_values = match *language_value {
                                            Value::String(ref string) => {
                                                vec![Value::String(string.clone())]
                                            }
                                            Value::Array(ref arr) => arr.clone(),
                                            Value::Null => continue,
                                            _ => {
                                                return Err(ExpansionError::InvalidLanguageMapValue)
                                            }
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

                                    expanded_value = Some(Value::Array(new_arr));
                                } else if map == "@index" && value.is_object() {
                                    // 7.6
                                    if let Value::Object(obj) = value {
                                        let mut ar = Vec::new();
                                        for (index, mut index_value) in obj {
                                            if !index_value.is_array() {
                                                index_value =
                                                    Value::Array(vec![index_value].into());
                                            }

                                            index_value = Context::_expand(
                                                active_context,
                                                Some(&key),
                                                index_value,
                                            )?;
                                            if let Value::Array(var) = index_value {
                                                for mut item in var {
                                                    if !item.as_object()
                                                        .unwrap()
                                                        .contains_key("@index")
                                                    {
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

                                        expanded_value = Some(Value::Array(ar));
                                        value = Value::Null;
                                    } else {
                                        unreachable!();
                                    }
                                }
                            }
                        }

                        // 7.7
                        if expanded_value == None {
                            expanded_value =
                                Some(Context::_expand(active_context, Some(&key), value)?);
                        }
                        let mut expanded_value = expanded_value.unwrap();

                        // 7.8
                        if expanded_value.is_null() {
                            continue;
                        }

                        if let Some(item) = active_context.terms.get(&key) {
                            if let Some(ref map) = item.container_mapping {
                                // 7.9
                                if map == "@list" {
                                    match expanded_value {
                                        Value::Object(obj) => {
                                            if !obj.contains_key("@list") {
                                                expanded_value =
                                                    Value::Array(vec![Value::Object(obj)].into());
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
                            }

                            // 7.10
                            if item.reverse {
                                // 7.10.1
                                if !result.contains_key("@reverse") {
                                    result
                                        .insert("@reverse".to_string(), Value::Object(Map::new()));
                                }

                                // 7.10.2
                                if let Value::Object(ref mut reverse_map) = result["@reverse"] {
                                    // 7.10.3
                                    let mut ar = if let Value::Array(array) = expanded_value {
                                        array
                                    } else {
                                        vec![expanded_value].into()
                                    };

                                    // 7.10.4
                                    for item in &ar {
                                        // 7.10.4.1
                                        if let Value::Object(ref rm) = *item {
                                            if rm.contains_key("@value") || rm.contains_key("@list")
                                            {
                                                return Err(
                                                    ExpansionError::InvalidReversePropertyValue,
                                                );
                                            }
                                        }
                                    }

                                    if !reverse_map.contains_key(&prop) {
                                        // 7.10.4.2
                                        reverse_map.insert(prop, Value::Array(ar));
                                    } else {
                                        // 7.10.4.3
                                        if let Value::Array(ref mut arr) = reverse_map[&prop] {
                                            arr.append(&mut ar)
                                        } else {
                                            unreachable!();
                                        }
                                    }
                                }
                                continue;
                            }
                        }

                        if !expanded_value.is_array() {
                            expanded_value = Value::Array(vec![expanded_value].into());
                        }

                        // 7.11
                        if !result.contains_key(&prop) {
                            // 7.11.1
                            result.insert(prop, expanded_value);
                        } else {
                            // 7.11.2
                            if let Value::Array(ref mut arr) = result[&prop] {
                                arr.append(expanded_value.as_array_mut().unwrap());
                            }
                        }
                    }
                }

                if result.contains_key("@value") {
                    let val = &result["@value"];

                    for key in result.keys() {
                        if key == "@value" || key == "@language" || key == "@type"
                            || key == "@index"
                        {
                            continue;
                        }

                        return Err(ExpansionError::InvalidValueObject);
                    }

                    if result.contains_key("@type") && result.contains_key("@language") {
                        return Err(ExpansionError::InvalidValueObject);
                    }

                    match *val {
                        Value::Null => return Ok(Value::Null),
                        Value::String(_) => {}
                        _ => if result.contains_key("@language") {
                            return Err(ExpansionError::InvalidTypedValue);
                        },
                    }

                    if let Some(typeval) = result.get("@type") {
                        if !typeval.is_string() {
                            return Err(ExpansionError::InvalidTypedValue);
                        }
                    }
                } else if result.contains_key("@type") {
                    let typeval = result.remove("@type").unwrap();
                    result.insert(
                        "@type".to_owned(),
                        if typeval.is_string() {
                            Value::Array(vec![typeval].into())
                        } else {
                            typeval
                        },
                    );
                } else if result.contains_key("@set") {
                    if result.len() > 2 || (result.len() == 2 && !result.contains_key("@index")) {
                        return Err(ExpansionError::InvalidSetObject);
                    }

                    return Ok(result.remove("@set").unwrap());
                } else if result.contains_key("@list") {
                    if result.len() > 2 || (result.len() == 2 && !result.contains_key("@index")) {
                        return Err(ExpansionError::InvalidListObject);
                    }
                }

                if result.len() == 1 && result.contains_key("@language") {
                    Ok(Value::Null)
                } else if active_property == None || active_property == Some("@graph") {
                    if result.len() == 0 || result.contains_key("@value")
                        || result.contains_key("@list")
                    {
                        Ok(Value::Null)
                    } else if result.len() == 1 && result.contains_key("@id") {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Object(result))
                    }
                } else {
                    Ok(Value::Object(result))
                }
            }

            // 2
            _ => {
                if let Some(activeprop) = active_property {
                    if activeprop == "@graph" {
                        // 2.1
                        Ok(Value::Null)
                    } else {
                        // 2.2
                        Ok(active_context._expand_value(activeprop, elem))
                    }
                } else {
                    // 2.1
                    Ok(Value::Null)
                }
            }
        }
    }

    pub fn expand(&mut self, elem: Value) -> Result<Value, ExpansionError> {
        let mut val = Context::_expand(self, None, elem)?;

        if val.as_object()
            .and_then(|f| Some(f.len() == 1 && f.contains_key("@graph"))) == Some(true)
        {
            if let Value::Object(mut objd) = val {
                val = objd.remove("@graph").unwrap();
            }
        }

        if val.is_null() {
            Ok(Value::Array(Vec::new()))
        } else if !val.is_array() {
            Ok(Value::Array(vec![val].into()))
        } else {
            Ok(val)
        }
    }
}
