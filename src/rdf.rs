//! Stuff that has to do with JSON-LD to RDF handling.
//!
//! This library defines its own structs for RDF quads,
//! which it will serialize the RDF into.

use serde_json::{Map, Value};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
/// The contents of a single quad, which is either an ID reference or an Object.
pub enum QuadContents {
    /// An ID
    Id(String),

    /// An object, which consists of respectively a type, language, and optionally a language.
    Object(String, String, Option<String>),
}

#[derive(Debug, Clone)]
/// A single quad, consisting of a subject, predicate, and contents.
pub struct StringQuad {
    pub subject_id: String,
    pub predicate_id: String,

    pub contents: QuadContents,
}

#[derive(Debug)]
/// An error that occurs when generating node maps.
pub enum RDFError {
    /// Value that should have been a string is not a string.
    ExpectedString,

    /// `@type` value is not a string, array of string, or invalid
    InvalidTypeValue,

    /// `@index` value is not a string.
    InvalidIndexValue,

    /// `@reverse` value is not an object.
    InvalidReverseValue,

    /// Two objects with the same ID have different `@index` values.
    ConflictingIndexValues,
}

impl fmt::Display for RDFError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl Error for RDFError {
    fn description(&self) -> &str {
        match *self {
            RDFError::ExpectedString => "Expected string",
            RDFError::InvalidTypeValue => "invalid @type value",
            RDFError::InvalidIndexValue => "invalid @index value",
            RDFError::InvalidReverseValue => "invalid @reverse value",
            RDFError::ConflictingIndexValues => "conflicting @index values",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

#[derive(Debug)]
/// A reference contained in the node map.
enum Reference {
    /// A reference to an ID.
    Id(String),

    /// A type/value reference.
    TypeValue(Option<String>, Value),

    /// A reference to a language string.
    LanguageValue(String, String),

    /// A list of references.
    List(Vec<Reference>),
}

#[derive(Debug)]
/// A node map node.
struct Node {
    pub id: String,
    pub index: Option<String>,
    pub types: Vec<String>,

    pub data: HashMap<String, Vec<Reference>>,
}

/// Alias to the structure that the node map generation
/// expects.
type NodeMap = HashMap<String, HashMap<String, Node>>;

/// Trait used to generate blank nodes in the node map generation.
pub trait BlankNodeGenerator {
    /// Generate a new blank node. If `id` is Some, this method
    /// should return the same `String`, based on the `id`.
    fn generate_blank_node(&mut self, id: Option<&str>) -> String;
}

/// Predicate for the first item in a list.
pub const RDF_FIRST: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";

/// Predicate for the rest of the items in a list.
pub const RDF_REST: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";

/// Object for the end of a list.
pub const RDF_NIL: &'static str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

#[derive(Debug)]
/// Parameter passed into the node map generation.
enum SubjectType {
    None,
    Normal(String, String),
    Reverse(String, String),
}

/// "Consumes" a JSON-LD value, assumes that it's a String, and
/// returns an error if this is not true.
fn nom_string(val: Value) -> Result<String, RDFError> {
    match val {
        Value::String(strval) => Ok(strval),
        _ => Err(RDFError::ExpectedString),
    }
}

/// Translates `Option<Result>` to `Result<Option>`.
fn transpose<T, E>(item: Option<Result<T, E>>) -> Result<Option<T>, E> {
    match item {
        Some(Ok(x)) => Ok(Some(x)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Makes a reference out of a JSON object.
fn make_reference(mut element: Map<String, Value>) -> Result<Reference, RDFError> {
    let val = element.remove("@value").unwrap();
    let typeval = element.remove("@type");
    let language = element.remove("@language");

    if let Some(language) = language {
        Ok(Reference::LanguageValue(
            nom_string(language)?,
            nom_string(val)?,
        ))
    } else {
        Ok(Reference::TypeValue(
            transpose(typeval.map(|f| nom_string(f)))?,
            val,
        ))
    }
}

fn generate_node_map<T>(
    element: Value,
    node_map: &mut NodeMap,
    active_graph: &str,
    active_subject: &SubjectType,
    mut list: Option<&mut Vec<Reference>>,
    generator: &mut T,
) -> Result<(), RDFError>
where
    T: BlankNodeGenerator,
{
    match element {
        // 1
        Value::Array(arr) => {
            if let Some(val) = list {
                for item in arr {
                    generate_node_map(
                        item,
                        node_map,
                        active_graph,
                        active_subject,
                        Some(val),
                        generator,
                    )?;
                }
            } else {
                for item in arr {
                    generate_node_map(
                        item,
                        node_map,
                        active_graph,
                        active_subject,
                        None,
                        generator,
                    )?;
                }
            }
        }

        // 2
        Value::Object(mut element) => {
            if !node_map.contains_key(active_graph) {
                node_map.insert(active_graph.to_owned(), HashMap::new());
            }

            // 3
            let removed_type = element.remove("@type");
            if let Some(Value::Array(mut elems)) = removed_type {
                // If element has an @type member, perform for each item the following steps:
                //    If item is a blank node identifier, replace it with a newly generated blank node identifier passing item for identifier.
                let elems = elems
                    .into_iter()
                    .map(|item| {
                        if let Value::String(item) = item {
                            Value::String(if item.starts_with("_:") {
                                generator.generate_blank_node(Some(&item))
                            } else {
                                item
                            })
                        } else {
                            unreachable!()
                        }
                    })
                    .collect();

                element.insert("@type".to_owned(), Value::Array(elems));
                // todo
            }

            // 4
            if element.contains_key("@value") {
                let reference = make_reference(element)?;

                if let Some(list) = list {
                    // 4.2
                    list.push(reference);
                } else {
                    // 4.1
                    match active_subject {
                        SubjectType::Normal(ref active_subject, ref active_property)
                        | SubjectType::Reverse(ref active_subject, ref active_property) => {
                            let graph = node_map.get_mut(active_graph).unwrap();
                            let node = graph.get_mut(active_subject).unwrap();

                            if node.data.contains_key(active_property) {
                                node.data.get_mut(active_property).unwrap().push(reference);
                            } else {
                                node.data
                                    .insert(active_property.to_owned(), vec![reference]);
                            }
                        }

                        _ => unreachable!(),
                    }
                }
            } else if element.contains_key("@list") {
                let mut result = Vec::new();

                generate_node_map(
                    element.remove("@list").unwrap(),
                    node_map,
                    active_graph,
                    active_subject,
                    Some(&mut result),
                    generator,
                )?;

                match active_subject {
                    SubjectType::Normal(ref active_subject, ref active_property)
                    | SubjectType::Reverse(ref active_subject, ref active_property) => {
                        let graph = node_map.get_mut(active_graph).unwrap();
                        let node = graph.get_mut(active_subject).unwrap();

                        let reference = Reference::List(result);

                        if node.data.contains_key(active_property) {
                            node.data.get_mut(active_property).unwrap().push(reference);
                        } else {
                            node.data
                                .insert(active_property.to_owned(), vec![reference]);
                        }
                    }

                    _ => unreachable!(),
                }
            } else {
                // 6:

                let id = element
                    .remove("@id")
                    .and_then(|f| match f {
                        Value::String(id) => Some(id),
                        _ => None,
                    })
                    .unwrap_or_else(|| generator.generate_blank_node(None));

                // 6.3, 6.4
                let mut node = node_map
                    .get_mut(active_graph)
                    .and_then(|f| f.remove(&id))
                    .unwrap_or_else(|| Node {
                        id: id.to_owned(),
                        index: None,
                        types: Vec::new(),
                        data: HashMap::new(),
                    });

                match active_subject {
                    SubjectType::Reverse(ref id, ref active_property) => {
                        // 6.5
                        let reference = Reference::Id(id.to_owned());

                        if node.data.contains_key(active_property) {
                            node.data.get_mut(active_property).unwrap().push(reference);
                        } else {
                            node.data
                                .insert(active_property.to_owned(), vec![reference]);
                        }
                    }

                    SubjectType::Normal(_, ref active_property) => {
                        // 6.6
                        let reference = Reference::Id(id.to_owned());

                        if let Some(ref mut val) = list {
                            val.push(reference)
                        } else {
                            if node.data.contains_key(active_property) {
                                // XXX dedupe
                                node.data.get_mut(active_property).unwrap().push(reference);
                            } else {
                                node.data
                                    .insert(active_property.to_owned(), vec![reference]);
                            }
                        }
                    }

                    _ => {}
                }

                if let Some(types) = element.remove("@type") {
                    if let Value::Array(types) = types {
                        for item in types {
                            if let Value::String(strval) = item {
                                node.types.push(strval) // XXX dedupe
                            } else {
                                return Err(RDFError::InvalidTypeValue);
                            }
                        }
                    } else {
                        return Err(RDFError::InvalidTypeValue);
                    }
                }

                // 6.8
                if let Some(index) = element.remove("@index") {
                    if let Value::String(index) = index {
                        let index = Some(index);

                        if node.index != None && node.index != index {
                            return Err(RDFError::ConflictingIndexValues);
                        }

                        node.index = index
                    } else {
                        return Err(RDFError::InvalidIndexValue);
                    }
                }

                // 6.9
                if let Some(reverse) = element.remove("@reverse") {
                    if let Value::Object(reverse) = reverse {
                        for (property, value) in reverse {
                            let refsubj = SubjectType::Reverse(id.to_owned(), property);
                            if let Value::Array(values) = value {
                                node_map
                                    .get_mut(active_graph)
                                    .unwrap()
                                    .insert(id.to_owned(), node);
                                for value in values {
                                    generate_node_map(
                                        value,
                                        node_map,
                                        active_graph,
                                        &refsubj,
                                        None,
                                        generator,
                                    )?;
                                }
                                node = node_map.get_mut(active_graph).unwrap().remove(&id).unwrap();
                            }
                        }
                    } else {
                        return Err(RDFError::InvalidReverseValue);
                    }
                }

                // 6.10
                if let Some(graph) = element.remove("@graph") {
                    node_map
                        .get_mut(active_graph)
                        .unwrap()
                        .insert(id.to_owned(), node);
                    generate_node_map(graph, node_map, &id, &SubjectType::None, None, generator)?;
                    node = node_map.get_mut(active_graph).unwrap().remove(&id).unwrap();
                }

                // 6.11
                for (property, value) in element {
                    let property = if property.starts_with("_:") {
                        generator.generate_blank_node(Some(&property))
                    } else {
                        property
                    };

                    if !node.data.contains_key(&property) {
                        node.data.insert(property.to_owned(), Vec::new());
                    }
                    let reference = SubjectType::Normal(id.to_owned(), property);
                    node_map
                        .get_mut(active_graph)
                        .unwrap()
                        .insert(id.to_owned(), node);
                    generate_node_map(value, node_map, active_graph, &reference, None, generator)?;
                    node = node_map.get_mut(active_graph).unwrap().remove(&id).unwrap();
                }

                node_map.get_mut(active_graph).unwrap().insert(id, node);
            }
        }

        _ => {}
    }

    Ok(())
}

fn object_to_rdf(typeval: Option<String>, value: Value) -> QuadContents {
    match value {
        Value::String(strval) => {
            let typeval =
                typeval.unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#string".to_owned());

            QuadContents::Object(typeval, strval, None)
        }

        Value::Bool(boolval) => {
            let typeval =
                typeval.unwrap_or_else(|| "http://www.w3.org/2001/XMLSchema#boolean".to_owned());

            QuadContents::Object(typeval, boolval.to_string(), None)
        }

        Value::Number(numval) => {
            let typeval = if numval.is_f64() {
                "http://www.w3.org/2001/XMLSchema#double"
            } else {
                "http://www.w3.org/2001/XMLSchema#integer"
            }.to_owned();

            QuadContents::Object(typeval, numval.to_string(), None)
        }

        _ => unreachable!(),
    }
}

fn serialize_list<T>(
    reference: Vec<Reference>,
    triples: &mut Vec<StringQuad>,
    generator: &mut T,
) -> QuadContents
where
    T: BlankNodeGenerator,
{
    if reference.len() == 0 {
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

        loop {
            match bnodes.next() {
                Some((subject, item)) => {
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

                _ => break,
            }
        }

        QuadContents::Id(first_node)
    }
}

fn translate_reference<T>(
    reference: Reference,
    triples: &mut Vec<StringQuad>,
    generator: &mut T,
) -> QuadContents
where
    T: BlankNodeGenerator,
{
    match reference {
        Reference::List(list) => serialize_list(list, triples, generator),
        Reference::Id(id) => QuadContents::Id(id),
        Reference::LanguageValue(lang, string) => QuadContents::Object(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString".to_owned(),
            string,
            Some(lang),
        ),

        Reference::TypeValue(typeval, value) => object_to_rdf(typeval, value),
    }
}

/// Translates an expanded JSON-LD object into RDF quads.
///
/// This method needs a blank node generator, and returns a
/// map keyed on graph. By default, all items go into
/// `@default`, but if `@graph` is used this may differ.
pub fn jsonld_to_rdf<T>(
    element: Value,
    generator: &mut T,
) -> Result<HashMap<String, Vec<StringQuad>>, RDFError>
where
    T: BlankNodeGenerator,
{
    let mut node_map = HashMap::new();

    generate_node_map(
        element,
        &mut node_map,
        "@default",
        &SubjectType::None,
        None,
        generator,
    )?;

    let mut dataset = HashMap::new();

    for (graph_name, graph) in node_map {
        let mut triples = Vec::new();

        for (_, node) in graph {
            for (property, values) in node.data {
                if property == "@type" {
                    for value in values {
                        if let Reference::Id(value) = value {
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
                } else if property.starts_with("@") {
                    continue;
                } else {
                    for item in values {
                        let contents = translate_reference(item, &mut triples, generator);
                        triples.push(StringQuad {
                            subject_id: node.id.to_owned(),
                            predicate_id: property.to_owned(),
                            contents: contents,
                        })
                    }
                }
            }
        }

        dataset.insert(graph_name, triples);
    }

    Ok(dataset)
}

fn literal_to_json(contents: QuadContents, use_native_types: bool) -> Value {
    let mut obj = Map::new();
    use serde_json::from_str;

    match contents {
        QuadContents::Id(id) => {
            obj.insert("@id".to_owned(), Value::String(id));
        }

        QuadContents::Object(typeval, value, lang) => {
            let (typeval, value) = if use_native_types
                && typeval == "http://www.w3.org/2001/XMLSchema#string"
            {
                (None, Value::String(value))
            } else if use_native_types && typeval == "http://www.w3.org/2001/XMLSchema#boolean" {
                if value == "true" {
                    (None, Value::Bool(true))
                } else if value == "False" {
                    (None, Value::Bool(false))
                } else {
                    (Some(typeval), Value::String(value))
                }
            } else if use_native_types
                && (typeval == "http://www.w3.org/2001/XMLSchema#integer"
                    || typeval == "http://www.w3.org/2001/XMLSchema#double")
            {
                if let Ok(Value::Number(numval)) = from_str(&value) {
                    (None, Value::Number(numval))
                } else {
                    (Some(typeval), Value::String(value))
                }
            } else if typeval == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" {
                if let Some(lang) = lang {
                    obj.insert("@language".to_owned(), Value::String(lang));
                }

                (None, Value::String(value))
            } else if typeval == "http://www.w3.org/2001/XMLSchema#string" {
                (None, Value::String(value))
            } else {
                (Some(typeval), Value::String(value))
            };

            obj.insert("@value".to_owned(), value);

            if let Some(typeval) = typeval {
                obj.insert("@type".to_owned(), Value::String(typeval));
            }
        }
    }

    Value::Object(obj)
}

/// Translates RDF into equivalent JSON-LD.
///
/// Like its counterpart, this method takes a map keyed on
/// graph. If none are used, the key should be `@default`.
///
/// This method cannot fail. All RDF is properly translatable into
/// JSON-LD.
pub fn rdf_to_jsonld(
    graphs: HashMap<String, Vec<StringQuad>>,
    use_native_types: bool,
    use_rdf_type: bool,
) -> Value {
    let mut graph_map = Map::new();
    let mut usages: HashMap<String, HashMap<String, Vec<(String, String, String)>>> =
        HashMap::new();

    for (graph, triples) in graphs {
        if !graph_map.contains_key(&graph) {}

        let mut node_map = Map::new();

        for triple in triples {
            // 3.6.1
            if !node_map.contains_key(&triple.subject_id) {
                let mut input = Map::new();
                input.insert(
                    "@id".to_owned(),
                    Value::String(triple.subject_id.to_owned()),
                );

                node_map.insert(triple.subject_id.to_owned(), Value::Object(input));
            }

            if let QuadContents::Id(ref idstr) = triple.contents {
                if !node_map.contains_key(idstr) {
                    let mut input = Map::new();
                    input.insert("@id".to_owned(), Value::String(idstr.to_owned()));

                    node_map.insert(idstr.to_owned(), Value::Object(input));
                }

                if &triple.predicate_id == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
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
                            Value::Array(vec![Value::String(idstr.to_owned())]),
                        );
                    } else {
                        // todo: deduplicate, 3.5.4
                        node.get_mut("@type")
                            .unwrap()
                            .as_array_mut()
                            .unwrap()
                            .push(Value::String(idstr.to_owned()));
                    }

                    continue;
                }

                if !usages.contains_key(&graph) {
                    usages.insert(graph.to_owned(), HashMap::new());
                }

                let usage = usages.get_mut(&graph).unwrap();

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
            let value = literal_to_json(triple.contents, use_native_types);
            if !node.contains_key(&triple.predicate_id) {
                node.insert(triple.predicate_id.to_owned(), Value::Array(Vec::new()));
            }

            // deduplicate???
            node.get_mut(&triple.predicate_id)
                .unwrap()
                .as_array_mut()
                .unwrap()
                .push(value);
        }

        graph_map.insert(graph, Value::Object(node_map));
    }

    for (name, mut graph_object) in usages {
        if !graph_object.contains_key(RDF_NIL) {
            continue;
        }

        let mut node_map = graph_map.get_mut(&name).unwrap().as_object_mut().unwrap();

        let mut nil = graph_object.remove(RDF_NIL).unwrap();
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
                {
                    let node = node_map.get(&node_id).unwrap().as_object().unwrap();
                    if !node.contains_key(RDF_REST) || !node.contains_key(RDF_FIRST) {
                        break;
                    }

                    // todo: @type
                    if node.len() > 3 {
                        break;
                    }
                }

                if let Value::Object(mut node) = node_map.remove(&node_id).unwrap() {
                    if let Value::Array(mut first) = node.remove(RDF_FIRST).unwrap() {
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

            if &property == RDF_FIRST {
                if &node_id == RDF_NIL {
                    continue;
                }

                panic!("todo")
            }

            {
                if let Value::Object(mut head_object) = node_map.remove(&node_id).unwrap() {
                    for value in head_object
                        .get_mut(&property)
                        .unwrap()
                        .as_array_mut()
                        .unwrap()
                    {
                        let value = value.as_object_mut().unwrap();
                        if !value.contains_key("@id") || value["@id"].as_str().unwrap() != &head {
                            continue;
                        }

                        value.remove("@id");
                        list.reverse();
                        value.insert("@list".to_owned(), Value::Array(list));
                        break;
                    }
                }
            }

            for node in list_nodes {
                node_map.remove(&node);
            }
        }
    }

    let mut result = Map::new();
    if let Some(Value::Object(val)) = graph_map.remove("@default") {
        for (subject, mut node) in val {
            if graph_map.contains_key(&subject) {
                let mut arr = Vec::new();
                if let Some(Value::Object(val)) = graph_map.remove(&subject) {
                    for (_, n) in val {
                        if {
                            let obj_n = n.as_object().unwrap();
                            obj_n.len() > 1 || !obj_n.contains_key("@id")
                        } {
                            arr.push(n);
                        }
                    }
                }

                node.as_object_mut()
                    .unwrap()
                    .insert("@graph".to_owned(), Value::Array(arr));
            }

            if {
                let nobj = node.as_object().unwrap();
                nobj.len() > 1 || !nobj.contains_key("@id")
            } {
                result.insert(subject, node);
            }
        }
    } else {
        unreachable!();
    }

    Value::Object(result)
}
