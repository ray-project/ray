// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Thread-safe per-actor tensor storage with condition-variable waiting.
//!
//! Ported from `python/ray/experimental/rdt/rdt_store.py`.
//! The key advantage over the Python version is that blocking waits
//! (`wait_and_get_object`, `wait_and_pop_object`, `wait_tensor_freed`)
//! release the GIL during the condvar wait, avoiding deadlocks.

#[cfg(feature = "python")]
use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(feature = "python")]
use std::sync::{Condvar, Mutex};
#[cfg(feature = "python")]
use std::time::{Duration, Instant};

#[cfg(feature = "python")]
use pyo3::exceptions::{PyAssertionError, PyTimeoutError};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyList;

/// Internal representation of an RDT object in the store.
#[cfg(feature = "python")]
struct RDTObject {
    /// Python list of tensors (or empty list if error).
    data: Py<PyAny>,
    /// Whether this is the primary (owner's) copy.
    is_primary: bool,
    /// If a recv failed, the Python exception is stored here.
    error: Option<Py<PyAny>>,
}

/// Interior state protected by the mutex.
#[cfg(feature = "python")]
struct RDTStoreInner {
    /// Maps object ID → queue of RDT objects.
    store: HashMap<String, VecDeque<RDTObject>>,
    /// Maps Python `id(tensor)` → set of object IDs containing that tensor.
    tensor_to_object_ids: HashMap<isize, HashSet<String>>,
}

/// Thread-safe tensor storage for RDT (Ray Direct Transport).
///
/// This store is read/written by multiple threads:
/// 1. Main thread (user code): get, put, pop
/// 2. `_ray_system` thread (data transfers): get, put
/// 3. CoreWorker GC thread: pop (garbage collection)
///
/// Blocking waits release the Python GIL to prevent deadlocks.
#[cfg(feature = "python")]
#[pyo3::pyclass(module = "_raylet")]
pub struct PyRDTStore {
    inner: Mutex<RDTStoreInner>,
    /// Signaled when an object is added to the store.
    object_present_cv: Condvar,
    /// Signaled when an object is popped from the store.
    object_freed_cv: Condvar,
}

/// Get the Python `id()` of an object as an `isize`.
/// In CPython, `id(obj)` returns the memory address of the object.
/// `Bound::as_ptr()` returns the same ffi::PyObject pointer.
#[cfg(feature = "python")]
fn py_id(obj: &Bound<'_, PyAny>) -> isize {
    obj.as_ptr() as isize
}

#[cfg(feature = "python")]
#[pymethods]
impl PyRDTStore {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: Mutex::new(RDTStoreInner {
                store: HashMap::new(),
                tensor_to_object_ids: HashMap::new(),
            }),
            object_present_cv: Condvar::new(),
            object_freed_cv: Condvar::new(),
        }
    }

    /// Check if an object with the given ID exists in the store.
    fn has_object(&self, obj_id: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        guard
            .store
            .get(obj_id)
            .map_or(false, |q| !q.is_empty())
    }

    /// Check if a tensor (by Python `id()`) is tracked in the store.
    /// Used for testing only.
    fn has_tensor(&self, tensor: &Bound<'_, PyAny>) -> bool {
        let tensor_id = py_id(tensor);
        let guard = self.inner.lock().unwrap();
        guard.tensor_to_object_ids.contains_key(&tensor_id)
    }

    /// Get the first object in the queue for the given ID.
    /// Raises the stored error if the object recorded an error.
    fn get_object(&self, py: Python<'_>, obj_id: &str) -> PyResult<PyObject> {
        let guard = self.inner.lock().unwrap();
        let queue = guard.store.get(obj_id).ok_or_else(|| {
            PyAssertionError::new_err(format!("obj_id={obj_id} not found in RDT store"))
        })?;
        let obj = queue.front().ok_or_else(|| {
            PyAssertionError::new_err(format!("obj_id={obj_id} queue is empty in RDT store"))
        })?;
        if let Some(ref err) = obj.error {
            return Err(PyErr::from_value_bound(err.bind(py).clone()));
        }
        Ok(obj.data.clone_ref(py))
    }

    /// Add an RDT object to the store.
    ///
    /// Args:
    ///   obj_id: The object ID.
    ///   rdt_object: Either a list of tensors or a Python exception.
    ///   is_primary: Whether this is the primary (owner's) copy.
    #[pyo3(signature = (obj_id, rdt_object, is_primary=false))]
    fn add_object(
        &self,
        py: Python<'_>,
        obj_id: &str,
        rdt_object: &Bound<'_, PyAny>,
        is_primary: bool,
    ) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        let is_exception =
            rdt_object.is_instance_of::<pyo3::exceptions::PyBaseException>();

        if is_exception {
            let empty_list: Py<PyAny> = PyList::empty_bound(py).unbind().into();
            guard
                .store
                .entry(obj_id.to_string())
                .or_default()
                .push_back(RDTObject {
                    data: empty_list,
                    is_primary,
                    error: Some(rdt_object.clone().unbind()),
                });
        } else {
            // Track tensor IDs for wait_tensor_freed.
            let list: &Bound<'_, PyList> = rdt_object.downcast()?;
            for item in list.iter() {
                let tensor_id = py_id(&item);
                guard
                    .tensor_to_object_ids
                    .entry(tensor_id)
                    .or_default()
                    .insert(obj_id.to_string());
            }
            guard
                .store
                .entry(obj_id.to_string())
                .or_default()
                .push_back(RDTObject {
                    data: rdt_object.clone().unbind(),
                    is_primary,
                    error: None,
                });
        }
        self.object_present_cv.notify_all();
        Ok(())
    }

    /// Add a primary-copy object and extract transport metadata.
    fn add_object_primary(
        &self,
        py: Python<'_>,
        obj_id: &str,
        tensors: &Bound<'_, PyAny>,
        tensor_transport: &str,
    ) -> PyResult<PyObject> {
        self.add_object(py, obj_id, tensors, true)?;
        let util = py.import_bound("ray.experimental.rdt.util")?;
        let mgr = util.call_method1("get_tensor_transport_manager", (tensor_transport,))?;
        let meta = mgr.call_method1("extract_tensor_transport_metadata", (obj_id, tensors))?;
        Ok(meta.unbind())
    }

    /// Check if the front object for the given ID is a primary copy.
    fn is_primary_copy(&self, obj_id: &str) -> bool {
        let guard = self.inner.lock().unwrap();
        guard
            .store
            .get(obj_id)
            .and_then(|q| q.front())
            .map_or(false, |obj| obj.is_primary)
    }

    /// Wait for an object to appear, then return it (without removing).
    /// Releases the GIL during the wait.
    #[pyo3(signature = (obj_id, timeout=None))]
    fn wait_and_get_object(
        &self,
        py: Python<'_>,
        obj_id: String,
        timeout: Option<f64>,
    ) -> PyResult<PyObject> {
        self.wait_for_object(py, &obj_id, timeout)?;
        self.get_object(py, &obj_id)
    }

    /// Wait for an object to appear, then pop it (removing from store).
    /// Releases the GIL during the wait.
    #[pyo3(signature = (obj_id, timeout=None))]
    fn wait_and_pop_object(
        &self,
        py: Python<'_>,
        obj_id: String,
        timeout: Option<f64>,
    ) -> PyResult<PyObject> {
        self.wait_for_object(py, &obj_id, timeout)?;
        self.pop_object(py, &obj_id)
    }

    /// Pop the front object from the queue for the given ID.
    /// Updates tensor tracking and notifies freed CV.
    fn pop_object(&self, py: Python<'_>, obj_id: &str) -> PyResult<PyObject> {
        let mut guard = self.inner.lock().unwrap();
        let queue = guard.store.get_mut(obj_id).ok_or_else(|| {
            PyAssertionError::new_err(format!("obj_id={obj_id} not found in RDT store"))
        })?;
        let obj = queue.pop_front().ok_or_else(|| {
            PyAssertionError::new_err(format!("obj_id={obj_id} queue is empty in RDT store"))
        })?;
        if queue.is_empty() {
            guard.store.remove(obj_id);
        }
        if let Some(ref err) = obj.error {
            self.object_freed_cv.notify_all();
            return Err(PyErr::from_value_bound(err.bind(py).clone()));
        }
        // Remove tensor tracking.
        let list: &Bound<'_, PyList> = obj.data.bind(py).downcast()?;
        for item in list.iter() {
            let tensor_id = py_id(&item);
            if let Some(ids) = guard.tensor_to_object_ids.get_mut(&tensor_id) {
                ids.remove(obj_id);
                if ids.is_empty() {
                    guard.tensor_to_object_ids.remove(&tensor_id);
                }
            }
        }
        self.object_freed_cv.notify_all();
        Ok(obj.data.clone_ref(py))
    }

    /// Wait until a tensor is freed from all objects in the store.
    /// Releases the GIL during the wait.
    #[pyo3(signature = (tensor, timeout=None))]
    fn wait_tensor_freed(
        &self,
        py: Python<'_>,
        tensor: &Bound<'_, PyAny>,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let tensor_id = py_id(tensor);
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t));

        let timed_out = py.allow_threads(|| {
            let mut guard = self.inner.lock().unwrap();
            loop {
                if !guard.tensor_to_object_ids.contains_key(&tensor_id) {
                    return false;
                }
                if let Some(dl) = deadline {
                    let remaining = dl.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return true;
                    }
                    let result = self.object_freed_cv.wait_timeout(guard, remaining).unwrap();
                    guard = result.0;
                    if result.1.timed_out() {
                        return guard.tensor_to_object_ids.contains_key(&tensor_id);
                    }
                } else {
                    guard = self.object_freed_cv.wait(guard).unwrap();
                }
            }
        });

        if timed_out {
            Err(PyTimeoutError::new_err(format!(
                "Tensor not freed from RDT object store after {}s. \
                 The tensor will not be freed until all ObjectRefs containing \
                 the tensor have gone out of scope.",
                timeout.unwrap_or(0.0)
            )))
        } else {
            Ok(())
        }
    }

    /// Return total number of objects across all queues.
    fn get_num_objects(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.store.values().map(|q| q.len()).sum()
    }
}

#[cfg(feature = "python")]
impl PyRDTStore {
    /// Internal helper: wait for an object to appear in the store.
    /// Releases the GIL during the condvar wait.
    fn wait_for_object(
        &self,
        py: Python<'_>,
        obj_id: &str,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let obj_id_owned = obj_id.to_string();
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t));

        let timed_out = py.allow_threads(|| {
            let mut guard = self.inner.lock().unwrap();
            loop {
                if guard
                    .store
                    .get(&obj_id_owned)
                    .map_or(false, |q| !q.is_empty())
                {
                    return false;
                }
                if let Some(dl) = deadline {
                    let remaining = dl.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return true;
                    }
                    let result = self
                        .object_present_cv
                        .wait_timeout(guard, remaining)
                        .unwrap();
                    guard = result.0;
                    if result.1.timed_out() {
                        return !guard
                            .store
                            .get(&obj_id_owned)
                            .map_or(false, |q| !q.is_empty());
                    }
                } else {
                    guard = self.object_present_cv.wait(guard).unwrap();
                }
            }
        });

        if timed_out {
            Err(PyTimeoutError::new_err(format!(
                "ObjectRef({obj_id}) not found in RDT object store after {}s, \
                 transfer may have failed. Please report this issue on GitHub: \
                 https://github.com/ray-project/ray/issues/new/choose",
                timeout.unwrap_or(0.0)
            )))
        } else {
            Ok(())
        }
    }
}
