#![feature(never_type)]

use jsonld::{expand, JsonLdOptions, RemoteContextLoader};
use serde::Deserialize;
use serde_json::Value;
use std::fs::File;
use std::future::Future;
use std::pin::Pin;

#[derive(Deserialize)]
struct SequenceOpts {
    base: Option<String>,

    #[serde(rename = "expandContext")]
    expand_context: Option<String>,

    #[serde(rename = "processingMode")]
    processing_mode: Option<String>,
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
    expect: String,
    option: Option<SequenceOpts>,
}

#[derive(Deserialize)]
struct FakeManifest {
    #[serde(rename = "baseIri")]
    base_iri: String,
    sequence: Vec<FakeSequence>,
}

#[derive(Debug)]
struct TestContextLoader {}

impl RemoteContextLoader for TestContextLoader {
    type Error = !;
    type Future = Pin<Box<dyn Future<Output = Result<Value, Self::Error>> + Send>>;

    fn load_context(_url: String) -> Self::Future {
        Box::pin(async { Ok(Value::Null) })
    }
}

fn get_data(name: &str) -> Value {
    let f = File::open(&("tests/".to_owned() + name)).expect("file fail");

    serde_json::from_reader(f).expect("json fail")
}

async fn run_single_seq(seq: FakeSequence, iri: &str) {
    if let Some(processing_mode) = seq.option.as_ref().and_then(|f| f.processing_mode.as_ref()) {
        if processing_mode == "json-ld-1.1" {
            return;
        }
    }

    if !seq.types.iter().any(|f| f == "jld:PositiveEvaluationTest") {
        return;
    }

    let input = get_data(&seq.input);
    let expect = get_data(&seq.expect);

    let base_iri = seq
        .option
        .as_ref()
        .and_then(|f| f.base.to_owned())
        .or_else(|| Some(iri.to_owned() + &seq.input));

    let ctx = seq
        .option
        .as_ref()
        .and_then(|f| f.expand_context.as_ref())
        .map(|f| get_data(f));

    let res = expand::<TestContextLoader>(
        &input,
        &JsonLdOptions {
            base: base_iri,
            compact_arrays: None,
            expand_context: ctx,
            processing_mode: None,
        },
    )
    .await;

    println!("{} {}\n: {:?}", seq.id, seq.name, seq.purpose);

    match res {
        Ok(res) => {
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

        Err(e) => {
            println!("Fail: {}", e);
        }
    }
}

fn main() {
    let data: FakeManifest = serde_json::from_value(get_data("expand-manifest.jsonld")).unwrap();
    for seq in data.sequence {
        async_std::task::block_on(run_single_seq(seq, &data.base_iri));
    }
}
