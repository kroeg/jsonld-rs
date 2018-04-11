extern crate jsonld;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;

#[derive(Deserialize)]
struct SequenceOptions {
    #[serde(rename = "compactArrays")]
    compact_arrays: Option<bool>,

    #[serde(rename = "specVersion")]
    spec_version: Option<String>,
}

#[derive(Deserialize)]
struct FakeSequence {
    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "@type")]
    types: Vec<String>,

    name: String,
    purpose: Option<String>,
    input: String,
    context: String,
    expect: String,
    option: Option<SequenceOptions>,
}

#[derive(Deserialize)]
struct FakeManifest {
    #[serde(rename = "baseIri")]
    base_iri: String,
    sequence: Vec<FakeSequence>,
}

use jsonld::{compact, JsonLdOptions, RemoteContextLoader};

use std::fs::File;

use serde_json::Value;
use std::error::Error;
use std::rc::Rc;

use jsonld::helper::parse;

#[derive(Debug)]
struct TestContextLoader {}

impl RemoteContextLoader for TestContextLoader {
    fn load_context(&self, _url: &str) -> Result<Value, Box<Error>> {
        Ok(Value::Null)
    }
}

fn get_data(name: &str) -> Value {
    let f = File::open(&("tests/".to_owned() + name)).expect("file fail");

    serde_json::from_reader(f).expect("json fail")
}

fn run_single_seq(seq: FakeSequence, base_iri: &str) {
    if !seq.types.iter().any(|f| f == "jld:PositiveEvaluationTest") {
        return;
    }

    if let Some(spec_version) = seq.option.as_ref().and_then(|f| f.spec_version.as_ref()) {
        if spec_version == "json-ld-1.1" {
            return;
        }
    }

    let input = get_data(&seq.input);
    let context = get_data(&seq.context);
    let expect = get_data(&seq.expect);

    println!("{} {}\n: {:?}", seq.id, seq.name, seq.purpose);

    let res = compact(
        &input,
        &context,
        JsonLdOptions {
            base: Some(base_iri.to_owned()),
            compact_arrays: seq.option.and_then(|f| f.compact_arrays),
            document_loader: Rc::new(TestContextLoader {}),
            expand_context: None,
            processing_mode: None,
        },
    ).unwrap();

    if expect != res {
        println!(
            "Diff: {}\n{}\n------",
            serde_json::to_string_pretty(&expect).unwrap(),
            serde_json::to_string_pretty(&res).unwrap()
        );
    } else {
        println!("Ok!\n------");
    }
}

fn main() {
    let data: FakeManifest = serde_json::from_value(get_data("compact-manifest.jsonld")).unwrap();
    for seq in data.sequence {
        run_single_seq(seq, &data.base_iri);
    }
}
