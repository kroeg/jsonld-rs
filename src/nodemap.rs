use serde_json::Map;
use serde_json::Value as JValue;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::ops::{Index, IndexMut};

#[derive(Debug)]
/// An error that occurs when generating node maps.
pub enum NodeMapError {
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

impl fmt::Display for NodeMapError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl Error for NodeMapError {
    fn description(&self) -> &str {
        match *self {
            NodeMapError::ExpectedString => "Expected string",
            NodeMapError::InvalidTypeValue => "invalid @type value",
            NodeMapError::InvalidIndexValue => "invalid @index value",
            NodeMapError::InvalidReverseValue => "invalid @reverse value",
            NodeMapError::ConflictingIndexValues => "conflicting @index values",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A reference contained in the node map.
pub enum Pointer {
    /// A reference to an ID.
    Id(String),

    /// A type/language/value reference.
    Value(Value),

    /// A list of references.
    List(Vec<Pointer>),
}

impl Pointer {
    /// Translates this `Pointer` to the JSON-LD this was generated from.
    pub fn to_json(self) -> JValue {
        let mut map = Map::new();
        match self {
            Pointer::Id(id) => {
                map.insert("@id".to_owned(), JValue::String(id));
            }
            Pointer::List(list) => {
                map.insert(
                    "@list".to_owned(),
                    JValue::Array(list.into_iter().map(|f| f.to_json()).collect()),
                );
            }
            Pointer::Value(val) => {
                map.insert("@value".to_owned(), val.value);
                if let Some(tid) = val.type_id {
                    map.insert("@type".to_owned(), JValue::String(tid));
                }

                if let Some(lang) = val.language {
                    map.insert("@language".to_owned(), JValue::String(lang));
                }
            }
        };

        JValue::Object(map)
    }
}

#[derive(PartialEq, Debug, Clone)]
/// The equivalent to a JSON-LD `@value` object
pub struct Value {
    /// The value contained within this JSON-LD value object. If `type_id` is
    /// `None`, the interpretations are trivial, else the contents are String
    /// and `type_id` explains how to interpret it.
    pub value: JValue,

    /// The (optional) type ID of this value object. `type_id` and `language`
    /// cannot be `Some` at the same time. If this is None, the `value` field
    /// may be any other JSON primitive.
    pub type_id: Option<String>,

    /// The (optional) language of the value object. If `Some`, the value is
    /// always a language string, and should be interpreted as such.
    pub language: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
/// A node map node.
pub struct Entity {
    pub id: String,
    pub index: Option<String>,
    pub types: Vec<String>,

    pub(crate) data: HashMap<String, Vec<Pointer>>,
}

static EMPTY: Vec<Pointer> = Vec::new();

impl Entity {
    pub fn new(id: String) -> Entity {
        Entity {
            id: id,
            index: None,
            types: Vec::new(),
            data: HashMap::new()
        }
    }

    /// Gets a list of values contained in this entity based on predicate.
    ///
    /// This function never fails, if an unknown value is passed in it will
    /// return an empty array.
    pub fn get(&self, val: &str) -> &Vec<Pointer> {
        self.data.get(val).unwrap_or(&EMPTY)
    }

    /// Gets a mutable list of values in this element based on the predicate.
    ///
    /// If an unknown value is passed in, it will return a new empty array.
    pub fn get_mut(&mut self, val: &str) -> &mut Vec<Pointer> {
        self.data
            .entry(val.to_owned())
            .or_insert_with(|| Vec::new())
    }

    /// Translates this `Entity` to the JSON-LD this was generated from.
    pub fn to_json(self) -> JValue {
        let mut map = Map::new();

        map.insert("@id".to_owned(), JValue::String(self.id));
        if let Some(index) = self.index {
            map.insert("@index".to_owned(), JValue::String(index));
        }

        map.insert("@type".to_owned(), JValue::Array(self.types.into_iter().map(JValue::String).collect()));

        for (k, v) in self.data {
            map.insert(
                k,
                JValue::Array(v.into_iter().map(|f| f.to_json()).collect()),
            );
        }

        JValue::Object(map)
    }

    pub fn into_data(self) -> HashMap<String, Vec<Pointer>> {
        self.data
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<Pointer>)> {
        self.data.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Vec<Pointer>)> {
        self.data.iter_mut()
    }
}

impl<'a> Index<&'a str> for Entity {
    type Output = Vec<Pointer>;

    fn index<'b>(&'b self, index: &'a str) -> &'b Vec<Pointer> {
        self.get(index)
    }
}

impl<'a> IndexMut<&'a str> for Entity {
    fn index_mut<'b>(&'b mut self, index: &'a str) -> &'b mut Vec<Pointer> {
        self.get_mut(index)
    }
}

/// Alias to the structure that the node map generation
/// expects.
pub type NodeMap = HashMap<String, HashMap<String, Entity>>;

/// Trait used to generate blank nodes in the node map generation.
pub trait BlankNodeGenerator {
    /// Generate a new blank node. If `id` is Some, this method
    /// should return the same `String`, based on the `id`.
    fn generate_blank_node(&mut self, id: Option<&str>) -> String;
}

pub struct DefaultNodeGenerator {
    i: u32,
    data: HashMap<String, String>
}

impl DefaultNodeGenerator {
    pub fn new() -> DefaultNodeGenerator {
        DefaultNodeGenerator { i: 0, data: HashMap::new() }
    }
}

impl BlankNodeGenerator for DefaultNodeGenerator {
    fn generate_blank_node(&mut self, id: Option<&str>) -> String {
        if let Some(id) = id {
            if !self.data.contains_key(id) {
                let new_id = format!("_:b{}", self.i);
                self.i += 1;
                self.data.insert(id.to_owned(), new_id);
            }

            self.data[id].to_owned()
        } else {
            self.i += 1;
            format!("_:b{}", self.i - 1)
        }
    }
}

#[derive(Debug)]
/// Parameter passed into the node map generation.
enum SubjectType {
    None,
    Normal(String, String),
    Reverse(String, String),
}

/// "Consumes" a JSON-LD value, assumes that it's a String, and
/// returns an error if this is not true.
fn nom_string(val: JValue) -> Result<String, NodeMapError> {
    match val {
        JValue::String(strval) => Ok(strval),
        _ => Err(NodeMapError::ExpectedString),
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
fn make_reference(mut element: Map<String, JValue>) -> Result<Pointer, NodeMapError> {
    let val = element.remove("@value").unwrap();
    let typeval = element.remove("@type");
    let language = element.remove("@language");

    Ok(Pointer::Value(Value {
        value: val,
        type_id: transpose(typeval.map(|f| nom_string(f)))?,
        language: transpose(language.map(|f| nom_string(f)))?,
    }))
}

/// Generates a node map!
pub fn generate_node_map<T: BlankNodeGenerator>(
    element: JValue,
    generator: &mut T,
) -> Result<NodeMap, NodeMapError> {
    let mut node_map = HashMap::new();

    _generate_node_map(
        element,
        &mut node_map,
        "@default",
        &SubjectType::None,
        None,
        generator,
    )?;

    Ok(node_map)
}

fn _generate_node_map<T>(
    element: JValue,
    node_map: &mut NodeMap,
    active_graph: &str,
    active_subject: &SubjectType,
    mut list: Option<&mut Vec<Pointer>>,
    generator: &mut T,
) -> Result<(), NodeMapError>
where
    T: BlankNodeGenerator,
{
    match element {
        // 1
        JValue::Array(arr) => {
            if let Some(val) = list {
                for item in arr {
                    _generate_node_map(
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
                    _generate_node_map(
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
        JValue::Object(mut element) => {
            if !node_map.contains_key(active_graph) {
                node_map.insert(active_graph.to_owned(), HashMap::new());
            }

            // 3
            let removed_type = element.remove("@type");
            if let Some(JValue::Array(mut elems)) = removed_type {
                // If element has an @type member, perform for each item the following steps:
                //    If item is a blank node identifier, replace it with a newly generated blank node identifier passing item for identifier.
                let elems = elems
                    .into_iter()
                    .map(|item| {
                        if let JValue::String(item) = item {
                            JValue::String(if item.starts_with("_:") {
                                generator.generate_blank_node(Some(&item))
                            } else {
                                item
                            })
                        } else {
                            unreachable!()
                        }
                    })
                    .collect();

                element.insert("@type".to_owned(), JValue::Array(elems));
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

                _generate_node_map(
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

                        let reference = Pointer::List(result);

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
                        JValue::String(id) => if id.starts_with("_:") { Some(generator.generate_blank_node(Some(&id))) } else { Some(id) },
                        _ => None,
                    })
                    .unwrap_or_else(|| generator.generate_blank_node(None));

                if let SubjectType::Normal(ref active_id, ref active_property) = active_subject {
                        // 6.6
                        let reference = Pointer::Id(id.to_owned());

                        if let Some(ref mut val) = list {
                            val.push(reference)
                        } else {
                            let mut node = node_map.get_mut(active_graph).and_then(|f| f.remove(active_id)).unwrap();
                            if node.data.contains_key(active_property) {
                                // XXX dedupe
                                node.data.get_mut(active_property).unwrap().push(reference);
                            } else {
                                node.data
                                    .insert(active_property.to_owned(), vec![reference]);
                            }

                            node_map.get_mut(active_graph).unwrap().insert(active_id.to_owned(), node);
                        }
                    }

                // 6.3, 6.4
                let mut node = node_map
                    .get_mut(active_graph)
                    .and_then(|f| f.remove(&id))
                    .unwrap_or_else(|| Entity {
                        id: id.to_owned(),
                        index: None,
                        types: Vec::new(),
                        data: HashMap::new()
                    });

                match active_subject {
                    SubjectType::Reverse(ref id, ref active_property) => {
                        // 6.5
                        let reference = Pointer::Id(id.to_owned());

                        if node.data.contains_key(active_property) {
                            node.data.get_mut(active_property).unwrap().push(reference);
                        } else {
                            node.data
                                .insert(active_property.to_owned(), vec![reference]);
                        }
                    },

                    _ => {}
                }

                if let Some(types) = element.remove("@type") {
                    if let JValue::Array(types) = types {
                        for item in types {
                            if let JValue::String(strval) = item {
                                node.types.push(strval) // XXX dedupe
                            } else {
                                return Err(NodeMapError::InvalidTypeValue);
                            }
                        }
                    } else {
                        return Err(NodeMapError::InvalidTypeValue);
                    }
                }

                // 6.8
                if let Some(index) = element.remove("@index") {
                    if let JValue::String(index) = index {
                        let index = Some(index);

                        if node.index != None && node.index != index {
                            return Err(NodeMapError::ConflictingIndexValues);
                        }

                        node.index = index
                    } else {
                        return Err(NodeMapError::InvalidIndexValue);
                    }
                }

                // 6.9
                if let Some(reverse) = element.remove("@reverse") {
                    if let JValue::Object(reverse) = reverse {
                        for (property, value) in reverse {
                            let refsubj = SubjectType::Reverse(id.to_owned(), property);
                            if let JValue::Array(values) = value {
                                node_map
                                    .get_mut(active_graph)
                                    .unwrap()
                                    .insert(id.to_owned(), node);
                                for value in values {
                                    _generate_node_map(
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
                        return Err(NodeMapError::InvalidReverseValue);
                    }
                }

                // 6.10
                if let Some(graph) = element.remove("@graph") {
                    node_map
                        .get_mut(active_graph)
                        .unwrap()
                        .insert(id.to_owned(), node);
                    _generate_node_map(graph, node_map, &id, &SubjectType::None, None, generator)?;
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
                    _generate_node_map(value, node_map, active_graph, &reference, None, generator)?;
                    node = node_map.get_mut(active_graph).unwrap().remove(&id).unwrap();
                }

                node_map.get_mut(active_graph).unwrap().insert(id, node);
            }
        }

        _ => {}
    }


    Ok(())
}
