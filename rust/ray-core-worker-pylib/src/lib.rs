// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! PyO3 Python bindings for Ray (_raylet.so replacement).
//!
//! Replaces `python/ray/_raylet.pyx` (Cython).
//! Will be built with maturin to produce `_raylet.so`.

// Phase 9: PyO3 bindings for IDs, GcsClient, CoreWorker, etc.

/// Placeholder module — will be populated incrementally.
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pyfunction]
fn get_ray_version() -> &'static str {
    ray_common::constants::RAY_VERSION
}

#[cfg(feature = "python")]
#[pymodule]
fn _raylet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_ray_version, m)?)?;
    Ok(())
}
