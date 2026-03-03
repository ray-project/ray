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

pub mod common;
pub mod ids;
pub mod object_ref;
pub mod core_worker;
pub mod gcs_client;
pub mod serialization;

// Re-export primary types for Rust consumers.
pub use common::{PyLanguage, PyWorkerType};
pub use core_worker::PyCoreWorker;
pub use gcs_client::PyGcsClient;
pub use ids::*;
pub use object_ref::PyObjectRef;

// ─── PyO3 module (only when the "python" feature is enabled) ─────────

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pyfunction]
fn get_ray_version() -> &'static str {
    ray_common::constants::RAY_VERSION
}

#[cfg(feature = "python")]
#[pyfunction]
fn get_ray_commit() -> &'static str {
    // Will be replaced by build script in production.
    "unknown"
}

/// Check if Ray has been initialized (stub — always false until ray_init called).
#[cfg(feature = "python")]
#[pyfunction]
fn is_initialized() -> bool {
    INITIALIZED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(feature = "python")]
static INITIALIZED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Mark Ray as initialized. Called internally after successful init.
#[cfg(feature = "python")]
#[pyfunction]
fn mark_initialized() {
    INITIALIZED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Mark Ray as shut down.
#[cfg(feature = "python")]
#[pyfunction]
fn mark_shutdown() {
    INITIALIZED.store(false, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(feature = "python")]
#[pymodule]
fn _raylet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ─── Module-level functions ──────────────────────────────────
    m.add_function(wrap_pyfunction!(get_ray_version, m)?)?;
    m.add_function(wrap_pyfunction!(get_ray_commit, m)?)?;
    m.add_function(wrap_pyfunction!(is_initialized, m)?)?;
    m.add_function(wrap_pyfunction!(mark_initialized, m)?)?;
    m.add_function(wrap_pyfunction!(mark_shutdown, m)?)?;

    // ─── ID types ────────────────────────────────────────────────
    m.add_class::<ids::PyObjectID>()?;
    m.add_class::<ids::PyTaskID>()?;
    m.add_class::<ids::PyActorID>()?;
    m.add_class::<ids::PyJobID>()?;
    m.add_class::<ids::PyWorkerID>()?;
    m.add_class::<ids::PyNodeID>()?;
    m.add_class::<ids::PyPlacementGroupID>()?;

    // ─── Enums ───────────────────────────────────────────────────
    m.add_class::<common::PyLanguage>()?;
    m.add_class::<common::PyWorkerType>()?;

    // ─── Core types ──────────────────────────────────────────────
    m.add_class::<object_ref::PyObjectRef>()?;
    m.add_class::<core_worker::PyCoreWorker>()?;
    m.add_class::<gcs_client::PyGcsClient>()?;

    // ─── Constants ───────────────────────────────────────────────
    m.add("RAY_VERSION", ray_common::constants::RAY_VERSION)?;

    Ok(())
}
