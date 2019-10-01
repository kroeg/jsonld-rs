//! Stuff that has to do with JSON-LD to RDF handling.
//!
//! This library defines its own structs for RDF quads,
//! which it will serialize the RDF into.

use serde_json::Map;
use serde_json::Value as JValue;
use std::collections::HashMap;
use std::hash::BuildHasher;

use crate::nodemap::{generate_node_map, BlankNodeGenerator, NodeMapError, Pointer};

#[derive(Debug, Clone)]
/// The contents of a single quad, which is either an ID reference or an Object.
pub enum QuadContents {
    /// An ID
    Id(String),

    /// An object, which consists of respectively a type, content, and optionally a language.
    Object(String, String, Option<String>),
}

#[derive(Debug, Clone)]
/// A single quad, consisting of a subject, predicate, and contents.
pub struct StringQuad {
    pub subject_id: String,
    pub predicate_id: String,

    pub contents: QuadContents,
}

/// Predicate for the first item in a list.
pub const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";

/// Predicate for the rest of the items in a list.
pub const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";

/// Object for the end of a list.
pub const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

/// ID for the type of a list.
pub const RDF_LIST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#List";

fn object_to_rdf(typeval: Option<String>, value: JValue) -> QuadContents {
    match value {
        JValue::String(strval) => {
            let typeval =
                typeval.unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#string".to_owned());

            QuadContents::Object(typeval, strval, None)
        }

        JValue::Bool(boolval) => {
            let typeval =
                typeval.unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#boolean".to_owned());

            QuadContents::Object(typeval, boolval.to_string(), None)
        }

        JValue::Number(numval) => {
            let typeval = if numval.is_f64() {
                "http://www.w3.org/2001/XMLSchema#double"
            } else {
                "http://www.w3.org/2001/XMLSchema#integer"
            }
            .to_owned();

            QuadContents::Object(typeval, numval.to_string(), None)
        }

        _ => unreachable!(),
    }
}

fn serialize_list<T>(
    reference: Vec<Pointer>,
    triples: &mut Vec<StringQuad>,
    generator: &mut T,
) -> QuadContents
where
    T: BlankNodeGenerator,
{
    if reference.is_empty() {
        QuadContents::Id("rdf:nil".to_owned())
    } else {
        let bnodes: Vec<_> = reference
            .into_iter()
            .map(|val| (generator.generate_blank_node(None), val))
            .collect();
        let mut bnodes = bnodes.into_iter().peekable();

        let first_node = match bnodes.peek() {
            Some(&(ref val, _)) => val.to_owned(),
            _ => unreachable!(),
        };

        while let Some((subject, item)) = bnodes.next() {
            let object = translate_reference(item, triples, generator);

            triples.push(StringQuad {
                subject_id: subject.to_owned(),
                predicate_id: RDF_FIRST.to_owned(),
                contents: object,
            });

            let next_id = match bnodes.peek() {
                Some(&(ref val, _)) => val.to_owned(),
                None => RDF_NIL.to_owned(),
            };

            triples.push(StringQuad {
                subject_id: subject,
                predicate_id: RDF_REST.to_owned(),
                contents: QuadContents::Id(next_id),
            });
        }

        QuadContents::Id(first_node)
    }
}

fn translate_reference<T>(
    reference: Pointer,
    triples: &mut Vec<StringQuad>,
    generator: &mut T,
) -> QuadContents
where
    T: BlankNodeGenerator,
{
    match reference {
        Pointer::List(list) => serialize_list(list, triples, generator),
        Pointer::Id(id) => QuadContents::Id(id),
        Pointer::Value(val) => {
            // Cannot collapse these two ifs because putting val.value in the tuple moves it there.
            if val.language.is_some() && val.value.is_string() {
                if let (Some(language), JValue::String(strval)) = (val.language, val.value) {
                    return QuadContents::Object(
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_owned(),
                        strval,
                        Some(language),
                    );
                }

                unreachable!();
            }

            object_to_rdf(val.type_id, val.value)
        }
    }
}

/// Translates an expanded JSON-LD object into RDF quads.
///
/// This method needs a blank node generator, and returns a
/// map keyed on graph. When no `@graph` is used, objects will
/// be stored under the `@default` graph.
pub fn jsonld_to_rdf<T>(
    element: &JValue,
    generator: &mut T,
) -> Result<HashMap<String, Vec<StringQuad>>, NodeMapError>
where
    T: BlankNodeGenerator,
{
    let node_map = generate_node_map(element, generator)?;
    let mut dataset = HashMap::new();

    for (graph_name, graph) in node_map {
        let mut triples = Vec::new();

        for (_, node) in graph {
            for typ in node.types {
                triples.push(StringQuad {
                    subject_id: node.id.to_owned(),
                    predicate_id: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_owned(),

                    contents: QuadContents::Id(typ),
                });
            }
            for (property, values) in node.data {
                if property == "@type" {
                    for value in values {
                        if let Pointer::Id(value) = value {
                            triples.push(StringQuad {
                                subject_id: node.id.to_owned(),
                                predicate_id: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                                    .to_owned(),

                                contents: QuadContents::Id(value),
                            });
                        } else {
                            unreachable!();
                        }
                    }
                } else if property.starts_with('@') {
                    continue;
                } else {
                    for item in values {
                        let contents = translate_reference(item, &mut triples, generator);
                        triples.push(StringQuad {
                            subject_id: node.id.to_owned(),
                            predicate_id: property.to_owned(),
                            contents,
                        })
                    }
                }
            }
        }

        dataset.insert(graph_name, triples);
    }

    Ok(dataset)
}

fn literal_to_json(contents: &QuadContents, use_native_types: bool) -> JValue {
    let mut obj = Map::new();

    match contents.clone() {
        QuadContents::Id(id) => {
            obj.insert("@id".to_owned(), JValue::String(id));
        }

        QuadContents::Object(typeval, value, lang) => {
            let (typeval, value) = if use_native_types
                && typeval == "http://www.w3.org/2001/XMLSchema#string"
            {
                (None, JValue::String(value))
            } else if use_native_types && typeval == "http://www.w3.org/2001/XMLSchema#boolean" {
                if value == "true" {
                    (None, JValue::Bool(true))
                } else if value == "false" {
                    (None, JValue::Bool(false))
                } else {
                    (Some(typeval), JValue::String(value))
                }
            } else if use_native_types
                && (typeval == "http://www.w3.org/2001/XMLSchema#integer"
                    || typeval == "http://www.w3.org/2001/XMLSchema#double")
            {
                // Parsing numbers should work like in JSON, so just call into serde_json.
                if let Ok(JValue::Number(numval)) = serde_json::from_str(&value) {
                    (None, JValue::Number(numval))
                } else {
                    (Some(typeval), JValue::String(value))
                }
            } else if typeval == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" {
                if let Some(lang) = lang {
                    obj.insert("@language".to_owned(), JValue::String(lang));
                }

                (None, JValue::String(value))
            } else if typeval == "http://www.w3.org/2001/XMLSchema#string" {
                (None, JValue::String(value))
            } else {
                (Some(typeval), JValue::String(value))
            };

            obj.insert("@value".to_owned(), value);

            if let Some(typeval) = typeval {
                obj.insert("@type".to_owned(), JValue::String(typeval));
            }
        }
    }

    JValue::Object(obj)
}

/// Translates RDF into equivalent JSON-LD.
///
/// Like its counterpart, this method takes a map keyed on
/// graph. If none are used, the key should be `@default`.
///
/// This method cannot fail. All RDF is properly translatable into
/// JSON-LD.
#[allow(clippy::cognitive_complexity)]
pub fn rdf_to_jsonld<S: BuildHasher>(
    graphs: &HashMap<String, Vec<StringQuad>, S>,
    use_native_types: bool,
    use_rdf_type: bool,
) -> JValue {
    let mut graph_map = Map::new();
    let mut usages = HashMap::new();

    for (graph_name, triples) in graphs {
        let mut node_map = Map::new();

        for triple in triples {
            if !node_map.contains_key(&triple.subject_id) {
                let mut input = Map::new();
                input.insert(
                    "@id".to_owned(),
                    JValue::String(triple.subject_id.to_owned()),
                );

                node_map.insert(triple.subject_id.to_owned(), JValue::Object(input));
            }

            if let QuadContents::Id(ref idstr) = triple.contents {
                if !node_map.contains_key(idstr) {
                    let mut input = Map::new();
                    input.insert("@id".to_owned(), JValue::String(idstr.to_owned()));

                    node_map.insert(idstr.to_owned(), JValue::Object(input));
                }

                if triple.predicate_id == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                    && !use_rdf_type
                {
                    let node = node_map
                        .get_mut(&triple.subject_id)
                        .unwrap()
                        .as_object_mut()
                        .unwrap();
                    if !node.contains_key("@type") {
                        node.insert(
                            "@type".to_owned(),
                            JValue::Array(vec![JValue::String(idstr.to_owned())]),
                        );
                    } else {
                        let arr = node.get_mut("@type").unwrap().as_array_mut().unwrap();

                        let val = JValue::String(idstr.clone());
                        if !arr.contains(&val) {
                            arr.push(val);
                        }
                    }

                    continue;
                }

                if !usages.contains_key(graph_name) {
                    usages.insert(graph_name.to_owned(), HashMap::new());
                }

                let usage = usages.get_mut(graph_name).unwrap();

                if !usage.contains_key(idstr) {
                    usage.insert(idstr.to_owned(), Vec::new());
                }

                let usagelist = usage.get_mut(idstr).unwrap();

                usagelist.push((
                    triple.subject_id.to_owned(),
                    triple.predicate_id.to_owned(),
                    idstr.to_owned(),
                ));
            }

            let node = node_map
                .get_mut(&triple.subject_id)
                .unwrap()
                .as_object_mut()
                .unwrap();

            let value = literal_to_json(&triple.contents, use_native_types);

            if !node.contains_key(&triple.predicate_id) {
                node.insert(triple.predicate_id.to_owned(), JValue::Array(Vec::new()));
            }

            let arr = node
                .get_mut(&triple.predicate_id)
                .unwrap()
                .as_array_mut()
                .unwrap();

            if !arr.contains(&value) {
                arr.push(value);
            }
        }

        graph_map.insert(graph_name.to_owned(), JValue::Object(node_map));
    }

    for (name, mut graph_object) in usages {
        if !graph_object.contains_key(RDF_NIL) {
            continue;
        }

        let node_map = graph_map.get_mut(&name).unwrap().as_object_mut().unwrap();

        let nil = graph_object.remove(RDF_NIL).unwrap();
        for (mut node_id, mut property, mut head) in nil {
            let mut list = Vec::new();
            let mut list_nodes = Vec::new();

            loop {
                if property != RDF_REST
                    || !graph_object.contains_key(&node_id)
                    || graph_object[&node_id].len() != 1
                {
                    break;
                }

                // The nodes that a list consist of can have only three predicates:
                //  rdf:first, rdf:rest, and an optional @type which is equal to rdf:List.

                let node = node_map.get(&node_id).unwrap().as_object().unwrap();
                if !node.contains_key(RDF_REST) || !node.contains_key(RDF_FIRST) {
                    break;
                }

                // XXX: yes this breaks if set to `use rdf:type`. Blame the spec.
                if node.len() == 3 && node.contains_key("@type") {
                    if let JValue::Array(ref arr) = node["@type"] {
                        if arr.len() != 1 || arr[0].as_str() != Some(RDF_LIST) {
                            break;
                        }
                    } else {
                        break;
                    }
                } else if node.len() > 2 {
                    break;
                }

                if let JValue::Object(mut node) = node_map.remove(&node_id).unwrap() {
                    if let JValue::Array(mut first) = node.remove(RDF_FIRST).unwrap() {
                        list.push(first.pop().unwrap());
                    } else {
                        unreachable!();
                    }

                    let node_usage = &graph_object[&node_id][0];

                    list_nodes.push(node_id);

                    node_id = node_usage.0.to_owned();
                    property = node_usage.1.to_owned();
                    head = node_usage.2.to_owned();

                    if !node_id.starts_with("_:") {
                        break;
                    }
                } else {
                    unreachable!();
                }
            }

            if property == RDF_FIRST {
                if node_id == RDF_NIL {
                    continue;
                }

                // This list is contained in another list's rdf:first field, and as
                // such has to be translated into a list object which will be
                // pointed at in the outer list.
                let head_obj = node_map.get_mut(&head).unwrap();
                let rest_obj = head_obj.as_array_mut().unwrap().remove(0);
                *head_obj = rest_obj;

                list.pop();
                list_nodes.pop();
            }

            if let JValue::Object(mut head_object) = node_map.remove(&node_id).unwrap() {
                for value in head_object
                    .get_mut(&property)
                    .unwrap()
                    .as_array_mut()
                    .unwrap()
                {
                    let value = value.as_object_mut().unwrap();
                    if value.get("@id").and_then(JValue::as_str) != Some(&head) {
                        continue;
                    }

                    value.remove("@id");
                    list.reverse();
                    value.insert("@list".to_owned(), JValue::Array(list));
                    break;
                }

                node_map.insert(node_id.to_owned(), JValue::Object(head_object));
            } else {
                unreachable!();
            }

            for node in list_nodes {
                node_map.remove(&node);
            }
        }
    }

    let mut result = Map::new();
    if let Some(JValue::Object(val)) = graph_map.remove("@default") {
        for (subject, mut node) in val {
            if graph_map.contains_key(&subject) {
                let mut arr = Vec::new();
                if let Some(JValue::Object(val)) = graph_map.remove(&subject) {
                    for (_, n) in val {
                        let obj = n.as_object().unwrap();
                        if obj.len() > 1 || !obj.contains_key("@id") {
                            arr.push(n);
                        }
                    }
                }

                node.as_object_mut()
                    .unwrap()
                    .insert("@graph".to_owned(), JValue::Array(arr));
            }

            let obj = node.as_object().unwrap();
            if obj.len() > 1 || !obj.contains_key("@id") {
                result.insert(subject, node);
            }
        }
    } else {
        unreachable!();
    }

    JValue::Object(result)
}
