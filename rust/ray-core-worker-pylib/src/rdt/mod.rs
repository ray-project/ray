// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Rust implementation of the RDT (Ray Direct Transport) core data structures.
//!
//! Ported from `python/ray/experimental/rdt/` for improved performance
//! (GIL-free blocking waits, faster locking).
//!
//! - `metadata`: TensorTransportMetadata type
//! - `store`: RDTStore (thread-safe tensor storage with condition variables)
//! - `registry`: Transport manager registry (singleton)

pub mod metadata;
pub mod registry;
pub mod store;
