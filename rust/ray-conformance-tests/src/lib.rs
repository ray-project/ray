// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Golden-file conformance tests ensuring Rust matches C++ behavior.
//!
//! Tests load expected values from JSON golden files and assert the Rust
//! implementation produces identical results.

pub mod golden;

#[cfg(test)]
mod categories;
