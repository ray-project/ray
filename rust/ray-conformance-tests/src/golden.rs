// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Golden file loading infrastructure.

use serde::de::DeserializeOwned;
use std::path::PathBuf;

/// Returns the path to the `golden-data/` directory.
pub fn golden_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden-data")
}

/// Load and parse a JSON golden file.
pub fn load_golden<T: DeserializeOwned>(relative_path: &str) -> T {
    let path = golden_data_dir().join(relative_path);
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read golden file {}: {e}", path.display()));
    serde_json::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse golden file {}: {e}", path.display()))
}

/// A golden file containing conformance test cases.
#[derive(Debug, serde::Deserialize)]
pub struct GoldenFile<I, O> {
    pub version: u32,
    pub generator: String,
    pub cases: Vec<ConformanceCase<I, O>>,
}

/// A single conformance test case.
#[derive(Debug, serde::Deserialize)]
pub struct ConformanceCase<I, O> {
    pub description: String,
    pub input: I,
    pub expected: O,
}
