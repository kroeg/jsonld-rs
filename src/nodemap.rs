//! Code to translate JSON-LD into a statically-typed struct and back again.

use serde_json::Map;
use serde_json::Value as JValue;
use std::borrow::Cow;
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

    /// `@id` value is not a string.
    InvalidIdValue,

    /// Value passed into generate_node_map was neither an array nor an object.
    InvalidObject,
}

impl fmt::Display for NodeMapError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NodeMapError::ExpectedString => write!(f, "Expected string"),
            NodeMapError::InvalidTypeValue => write!(f, "invalid @type value"),
            NodeMapError::InvalidIdValue => write!(f, "invalid @id value"),
            NodeMapError::InvalidIndexValue => write!(f, "invalid @index value"),
            NodeMapError::InvalidReverseValue => write!(f, "invalid @reverse value"),
            NodeMapError::ConflictingIndexValues => write!(f, "conflicting @index values"),
            NodeMapError::InvalidObject => write!(f, "invalid object passed to generate_node_map"),
        }
    }
}

impl Error for NodeMapError {}

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
    pub fn into_json(self) -> JValue {
        let mut map = Map::new();
        match self {
            Pointer::Id(id) => {
                map.insert("@id".to_owned(), JValue::String(id));
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

            Pointer::List(list) => {
                map.insert(
                    "@list".to_owned(),
                    JValue::Array(list.into_iter().map(Pointer::into_json).collect()),
                );
            }
        };

        JValue::Object(map)
    }
}

#[derive(PartialEq, Debug, Clone)]
/// The equivalent to a JSON-LD `@value` object.
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

    pub data: HashMap<String, Vec<Pointer>>,
}

lazy_static::lazy_static! {
    static ref EMPTY: Vec<Pointer> = Vec::new();
}

impl Entity {
    pub fn new(id: String) -> Entity {
        Entity {
            id,
            index: None,
            types: Vec::new(),
            data: HashMap::new(),
        }
    }

    /// Gets a list of values contained in this entity based on predicate.
    ///
    /// This function never fails: if an unknown value is passed in it will
    /// return an empty array.
    pub fn get(&self, val: &str) -> &Vec<Pointer> {
        self.data.get(val).unwrap_or(&EMPTY)
    }

    /// Gets a mutable list of values in this element based on the predicate.
    ///
    /// If an unknown value is passed in, it will return a new empty array.
    pub fn get_mut(&mut self, val: &str) -> &mut Vec<Pointer> {
        self.data.entry(val.to_owned()).or_insert_with(Vec::new)
    }

    /// Translates this `Entity` to the JSON-LD this was generated from.
    pub fn into_json(self) -> JValue {
        let mut map = Map::new();

        map.insert("@id".to_owned(), JValue::String(self.id));
        if let Some(index) = self.index {
            map.insert("@index".to_owned(), JValue::String(index));
        }

        map.insert(
            "@type".to_owned(),
            JValue::Array(self.types.into_iter().map(JValue::String).collect()),
        );

        for (k, v) in self.data {
            map.insert(
                k,
                JValue::Array(v.into_iter().map(|f| f.into_json()).collect()),
            );
        }

        JValue::Object(map)
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
    /// should return the same value every time the same `id` is passed.
    fn generate_blank_node(&mut self, id: Option<&str>) -> String;
}

/// The spec-defined node generator, returning node IDs of form `_:bN` where N is assigned sequentially.
#[derive(Default)]
pub struct DefaultNodeGenerator {
    i: u32,
    data: HashMap<String, String>,
}

impl DefaultNodeGenerator {
    pub fn new() -> DefaultNodeGenerator {
        DefaultNodeGenerator {
            i: 0,
            data: HashMap::new(),
        }
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
enum SubjectType<'a> {
    None,
    Normal(&'a str, &'a str),
    Reverse(&'a str, &'a str),
}

/// "Consumes" a JSON-LD value, assumes that it's a String, and
/// returns an error if this is not true.
fn expect_string(val: &JValue) -> Result<String, NodeMapError> {
    match val {
        JValue::String(strval) => Ok(strval.clone()),
        _ => Err(NodeMapError::ExpectedString),
    }
}

/// Makes a reference out of a JSON-LD value.
fn make_reference(
    element: &Map<String, JValue>,
    typeval: Option<JValue>,
) -> Result<Pointer, NodeMapError> {
    let val = element.get("@value").unwrap();
    let language = element.get("@language");

    Ok(Pointer::Value(Value {
        value: val.clone(),
        type_id: typeval.as_ref().map(expect_string).transpose()?,
        language: language.map(expect_string).transpose()?,
    }))
}

/// Generates a node map from a fully expanded JSON-LD value,
/// and a reference to a blank node generator.
///
/// As opposed to the JSON-LD API, this does not return a specially-formed
///  JSON object, but instead returns an equivalent set of Rust structs.
/// This allows for easier usage from Rust code, as everything is maximally
///  statically typed. The node map can later be transformed back into the
///  expected JSON losslessly.
pub fn generate_node_map<T: BlankNodeGenerator>(
    element: &JValue,
    generator: &mut T,
) -> Result<NodeMap, NodeMapError> {
    let mut node_map = HashMap::new();

    _generate_node_map(
        element,
        &mut node_map,
        "@default",
        &SubjectType::None,
        &mut None,
        generator,
    )?;

    Ok(node_map)
}

/// Internal generator for the node map generator.
#[allow(clippy::cognitive_complexity)]
fn _generate_node_map<T>(
    element: &JValue,
    node_map: &mut NodeMap,
    active_graph: &str,
    active_subject: &SubjectType,
    list: &mut Option<&mut Vec<Pointer>>,
    generator: &mut T,
) -> Result<(), NodeMapError>
where
    T: BlankNodeGenerator,
{
    match element {
        JValue::Array(arr) => {
            for item in arr {
                _generate_node_map(
                    item,
                    node_map,
                    active_graph,
                    active_subject,
                    list,
                    generator,
                )?;
            }
        }

        JValue::Object(element) => {
            if !node_map.contains_key(active_graph) {
                node_map.insert(active_graph.to_owned(), HashMap::new());
            }

            // Replace any blank node IDs in the `@type` property with newly
            //  generated blank node IDs.
            // Simultaneously check if the `@type` property is well-formed.
            let type_data = match element.get("@type") {
                Some(JValue::Array(elems)) => {
                    let elems = elems
                        .iter()
                        .map(|item| {
                            if let JValue::String(item) = item {
                                Ok(JValue::String(if item.starts_with("_:") {
                                    generator.generate_blank_node(Some(&item))
                                } else {
                                    item.clone()
                                }))
                            } else {
                                Err(NodeMapError::InvalidTypeValue)
                            }
                        })
                        .collect::<Result<_, NodeMapError>>()?;

                    Some(JValue::Array(elems))
                }

                Some(JValue::String(item)) => Some(JValue::String(if item.starts_with("_:") {
                    generator.generate_blank_node(Some(&item))
                } else {
                    item.clone()
                })),

                Some(_) => return Err(NodeMapError::InvalidTypeValue),
                None => None,
            };

            if element.contains_key("@value") {
                // This is a "primitive" JSON-LD value. Translate it into a native
                //  `Pointer` object.
                let reference = make_reference(element, type_data)?;

                if let Some(ref mut list) = list {
                    // If we are processing the items inside a list, add the pointer
                    //  to the list.
                    list.push(reference);
                } else {
                    match *active_subject {
                        SubjectType::Normal(active_subject, active_property)
                        | SubjectType::Reverse(active_subject, active_property) => {
                            let graph = node_map.get_mut(active_graph).unwrap();
                            let node = graph.get_mut(active_subject).unwrap();

                            if let Some(item) = node.data.get_mut(active_property) {
                                if !item.contains(&reference) {
                                    item.push(reference);
                                }
                            } else {
                                node.data
                                    .insert(active_property.to_owned(), vec![reference]);
                            }
                        }

                        // `@value` cannot be in the root of an expanded JSON-LD object,
                        //  so `SubjectType` will never be None.
                        _ => unreachable!(),
                    }
                }
            } else if let Some(list) = element.get("@list") {
                let mut result = Vec::new();

                _generate_node_map(
                    list,
                    node_map,
                    active_graph,
                    active_subject,
                    &mut Some(&mut result),
                    generator,
                )?;

                match *active_subject {
                    SubjectType::Normal(active_subject, active_property)
                    | SubjectType::Reverse(active_subject, active_property) => {
                        let graph = node_map.get_mut(active_graph).unwrap();
                        let node = graph.get_mut(active_subject).unwrap();

                        let reference = Pointer::List(result);

                        if let Some(item) = node.data.get_mut(active_property) {
                            item.push(reference);
                        } else {
                            node.data
                                .insert(active_property.to_owned(), vec![reference]);
                        }
                    }

                    _ => unreachable!(),
                }
            } else {
                // Replace blank node IDs with newly minted ones,
                //  and assign an ID if this object has none.
                let id = match element.get("@id") {
                    Some(JValue::String(id)) => {
                        if id.starts_with("_:") {
                            generator.generate_blank_node(Some(&id))
                        } else {
                            id.clone()
                        }
                    }

                    Some(_) => return Err(NodeMapError::InvalidIdValue),
                    None => generator.generate_blank_node(None),
                };

                if let SubjectType::Normal(active_id, active_property) = *active_subject {
                    // We are not in the root of the object tree, so add a pointer to the node
                    //  we are contained in.
                    let reference = Pointer::Id(id.to_owned());

                    if let Some(ref mut val) = list {
                        val.push(reference)
                    } else {
                        let node = node_map
                            .get_mut(active_graph)
                            .unwrap()
                            .get_mut(active_id)
                            .unwrap();

                        if let Some(item) = node.data.get_mut(active_property) {
                            // XXX dedupe
                            item.push(reference);
                        } else {
                            node.data
                                .insert(active_property.to_owned(), vec![reference]);
                        }
                    }
                }

                // Find (or create) the node we are working on right now.
                let mut node = node_map
                    .get_mut(active_graph)
                    .unwrap()
                    .entry(id.to_owned())
                    .or_insert_with(|| Entity {
                        id: id.to_owned(),
                        index: None,
                        types: Vec::new(),
                        data: HashMap::new(),
                    });

                if let SubjectType::Reverse(id, active_property) = *active_subject {
                    let reference = Pointer::Id(id.to_owned());

                    if let Some(item) = node.data.get_mut(active_property) {
                        if !item.contains(&reference) {
                            item.push(reference);
                        }
                    } else {
                        node.data
                            .insert(active_property.to_owned(), vec![reference]);
                    }
                }

                if let Some(types) = element
                    .get("@type")
                    .map(|f| f.as_array().ok_or(NodeMapError::InvalidTypeValue))
                    .transpose()?
                {
                    for item in types {
                        let item = item.as_str().ok_or(NodeMapError::InvalidTypeValue)?;

                        let item = if item.starts_with("_:") {
                            generator.generate_blank_node(Some(item))
                        } else {
                            item.to_owned()
                        };

                        if !node.types.contains(&item) {
                            node.types.push(item);
                        }
                    }
                }

                if let Some(index) = element.get("@index") {
                    if let JValue::String(index) = index {
                        if node.index.is_some() && node.index.as_ref().unwrap() != index {
                            return Err(NodeMapError::ConflictingIndexValues);
                        }

                        node.index = Some(index.clone())
                    } else {
                        return Err(NodeMapError::InvalidIndexValue);
                    }
                }

                if let Some(reverse) = element.get("@reverse") {
                    if let JValue::Object(reverse) = reverse {
                        for (property, value) in reverse {
                            let refsubj = SubjectType::Reverse(&id, property);
                            if let JValue::Array(values) = value {
                                for value in values {
                                    _generate_node_map(
                                        value,
                                        node_map,
                                        active_graph,
                                        &refsubj,
                                        &mut None,
                                        generator,
                                    )?;
                                }

                                node = node_map
                                    .get_mut(active_graph)
                                    .unwrap()
                                    .get_mut(&id)
                                    .unwrap();
                            }
                        }
                    } else {
                        return Err(NodeMapError::InvalidReverseValue);
                    }
                }

                if let Some(graph) = element.get("@graph") {
                    _generate_node_map(
                        graph,
                        node_map,
                        &id,
                        &SubjectType::None,
                        &mut None,
                        generator,
                    )?;

                    node = node_map
                        .get_mut(active_graph)
                        .unwrap()
                        .get_mut(&id)
                        .unwrap();
                }

                for (property, value) in element {
                    if property.starts_with('@') {
                        continue;
                    }

                    let property = if property.starts_with("_:") {
                        Cow::Owned(generator.generate_blank_node(Some(&property)))
                    } else {
                        Cow::Borrowed(property)
                    };

                    if !node.data.contains_key(property.as_ref()) {
                        node.data.insert(property.clone().into_owned(), Vec::new());
                    }

                    let reference = SubjectType::Normal(&id, property.as_ref());
                    _generate_node_map(
                        value,
                        node_map,
                        active_graph,
                        &reference,
                        &mut None,
                        generator,
                    )?;

                    node = node_map
                        .get_mut(active_graph)
                        .unwrap()
                        .get_mut(&id)
                        .unwrap();
                }
            }
        }

        _ => return Err(NodeMapError::InvalidObject),
    }

    Ok(())
}
