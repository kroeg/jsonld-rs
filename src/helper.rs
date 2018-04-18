use serde::{Serialize, Serializer};
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};

use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Clone)]
pub enum ValueObjectValue {
    String(String),
    Number(Number),
    Boolean(bool),
    Null,
}

impl Serialize for ValueObjectValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ValueObjectValue::String(ref strval) => serializer.serialize_str(strval),
            ValueObjectValue::Number(ref num) => serializer.serialize_some(num),
            ValueObjectValue::Boolean(ref boolean) => serializer.serialize_bool(boolean.clone()),
            ValueObjectValue::Null => serializer.serialize_none(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ValueObjectType {
    TypeIri(String),
    Language(String),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ValueObject {
    pub value: ValueObjectValue,
    pub value_type: ValueObjectType,
    pub index: Option<String>,
}

impl Serialize for ValueObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let count = 1 + if self.index.is_none() { 0 } else { 1 }
            + if self.value_type == ValueObjectType::Null {
                0
            } else {
                1
            };
        let mut state = serializer.serialize_struct("ValueObject", count)?;
        state.serialize_field("@value", &self.value)?;

        match self.value_type {
            ValueObjectType::TypeIri(ref strval) => state.serialize_field("@type", strval)?,
            ValueObjectType::Language(ref strval) => state.serialize_field("@language", strval)?,
            ValueObjectType::Null => {}
        };

        if let Some(ref indexval) = self.index {
            state.serialize_field("@index", indexval)?;
        }

        state.end()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct NodeObject {
    pub id: Option<String>,
    pub types: Vec<String>,
    pub index: Option<String>,

    pub items: BTreeMap<String, LdList>,
    pub reverse: BTreeMap<String, LdList>,
}

impl Serialize for NodeObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let count =
            if self.reverse.len() > 0 { 1 } else { 0 } + if self.types.len() > 0 { 1 } else { 0 }
                + self.items.len() + if self.id.is_none() { 0 } else { 1 }
                + if self.index.is_none() { 0 } else { 1 };
        let mut state = serializer.serialize_map(Some(count))?;

        if self.types.len() > 0 {
            state.serialize_entry("@type", &self.types)?;
        }

        if let Some(ref indexval) = self.index {
            state.serialize_entry("@index", indexval)?;
        }

        if self.reverse.len() > 0 {
            state.serialize_entry("@reverse", &self.reverse)?;
        }

        if let Some(ref idval) = self.id {
            state.serialize_entry("@id", idval)?;
        }

        for (ref key, ref val) in &self.items {
            state.serialize_entry(key, val)?;
        }

        state.end()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ValueOrNode {
    Value(ValueObject),
    Node(NodeObject),
}

impl Serialize for ValueOrNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            ValueOrNode::Value(ref vo) => vo.serialize(serializer),
            ValueOrNode::Node(ref vo) => vo.serialize(serializer),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum LdList {
    List(Vec<ValueOrNode>),
    Set(Vec<ValueOrNode>),
}

impl Serialize for LdList {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            LdList::List(ref lst) => {
                let mut state_arr = serializer.serialize_seq(Some(1))?;
                let mut map = BTreeMap::new();
                map.insert("@list".to_owned(), lst);
                state_arr.serialize_element(&map)?;
                state_arr.end()
            }
            LdList::Set(ref set) => {
                let mut state_arr = serializer.serialize_seq(Some(set.len()))?;
                for ref item in set {
                    state_arr.serialize_element(item)?;
                }

                state_arr.end()
            }
        }
    }
}

fn parse_value_object(mut ob: Map<String, Value>) -> ValueObject {
    let typ = if let Some(val) = ob.remove("@type") {
        if let Value::String(strval) = val {
            ValueObjectType::TypeIri(strval)
        } else {
            ValueObjectType::Null
        }
    } else if let Some(val) = ob.remove("@language") {
        if let Value::String(strval) = val {
            ValueObjectType::Language(strval)
        } else {
            ValueObjectType::Null
        }
    } else {
        ValueObjectType::Null
    };

    let index = ob.remove("@index").and_then(|f| {
        if let Value::String(strval) = f {
            Some(strval)
        } else {
            None
        }
    });

    let value = ob.remove("@value").unwrap();
    let result = match value {
        Value::String(strval) => ValueObjectValue::String(strval),
        Value::Number(num) => ValueObjectValue::Number(num),
        Value::Bool(boolean) => ValueObjectValue::Boolean(boolean),
        Value::Null => ValueObjectValue::Null,
        _ => ValueObjectValue::Null,
    };

    ValueObject {
        value: result,
        index: index,
        value_type: typ,
    }
}

fn list_into_stuff(mut ob: Map<String, Value>) -> BTreeMap<String, LdList> {
    let mut map = BTreeMap::new();

    for (key, value) in ob {
        let is_empty = value.as_array().and_then(|f| Some(f.len() == 0));
        if is_empty != Some(false) {
            continue;
        }

        let is_list = value
            .as_array()
            .and_then(|f| if f.len() == 1 { Some(f) } else { None })
            .and_then(|f| f[0].as_object())
            .and_then(|f| Some(f.contains_key("@list")));

        if is_list == Some(true) {
            if let Value::Array(mut arr) = value {
                let first = arr.remove(0);
                if let Value::Object(mut ob) = first {
                    if let Value::Array(mut cont) = ob.remove("@list").unwrap() {
                        map.insert(
                            key,
                            LdList::List(cont.into_iter().map(|f| parse(f).unwrap()).collect()),
                        );
                    } else {
                        unreachable!();
                    }
                } else {
                    unreachable!();
                }
            } else {
                unreachable!();
            }
        } else {
            if let Value::Array(mut arr) = value {
                map.insert(
                    key,
                    LdList::Set(arr.into_iter().map(|f| parse(f).unwrap()).collect()),
                );
            } else {
                unreachable!();
            }
        }
    }

    map
}

pub fn parse_node_object(mut ob: Map<String, Value>) -> NodeObject {
    let mut types = Vec::new();

    if let Some(typeval) = ob.remove("@type") {
        if let Value::Array(ar) = typeval {
            for item in ar {
                if let Value::String(strval) = item {
                    types.push(strval)
                }
            }
        }
    }

    let index = ob.remove("@index").and_then(|f| {
        if let Value::String(strval) = f {
            Some(strval)
        } else {
            None
        }
    });
    let id = ob.remove("@id").and_then(|f| {
        if let Value::String(strval) = f {
            Some(strval)
        } else {
            None
        }
    });

    let mut reverse_map = BTreeMap::new();

    if let Some(mut revmap) = ob.remove("@reverse") {
        if let Value::Object(mut obj) = revmap {
            reverse_map = list_into_stuff(obj);
        }

        panic!("todo");
    }

    let map = list_into_stuff(ob);

    NodeObject {
        id: id,
        index: index,
        types: types,
        items: map,
        reverse: reverse_map,
    }
}

pub fn parse(expanded: Value) -> Result<ValueOrNode, ()> {
    if let Value::Object(mut ob) = expanded {
        if ob.contains_key("@value") {
            Ok(ValueOrNode::Value(parse_value_object(ob)))
        } else {
            Ok(ValueOrNode::Node(parse_node_object(ob)))
        }
    } else {
        Err(())
    }
}
