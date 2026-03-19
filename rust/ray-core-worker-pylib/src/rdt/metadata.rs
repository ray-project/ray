// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Metadata types for tensor transport.
//!
//! Ported from `python/ray/experimental/rdt/tensor_transport_manager.py`.

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyDict, PyList, PyTuple};

/// Metadata describing tensors for transport between actors.
///
/// Fields:
/// - `tensor_meta`: List of `(shape, dtype)` tuples. Each shape is a `torch.Size`
///    and each dtype is a `torch.dtype`, stored as opaque Python objects to
///    preserve type fidelity for transport backends.
/// - `tensor_device`: Device string (e.g. `"cuda"`, `"cpu"`), or `None`.
#[cfg(feature = "python")]
#[pyo3::pyclass(module = "_raylet")]
pub struct PyTensorTransportMetadata {
    /// List of (shape, dtype) tuples — stored as Python objects.
    pub tensor_meta: Vec<(Py<PyAny>, Py<PyAny>)>,
    /// Device type string.
    pub tensor_device: Option<String>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyTensorTransportMetadata {
    #[new]
    #[pyo3(signature = (tensor_meta, tensor_device=None))]
    fn py_new(tensor_meta: Vec<(Py<PyAny>, Py<PyAny>)>, tensor_device: Option<String>) -> Self {
        Self {
            tensor_meta,
            tensor_device,
        }
    }

    /// Return tensor_meta as a Python list of tuples.
    #[getter]
    fn tensor_meta<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for (shape, dtype) in &self.tensor_meta {
            let tup = PyTuple::new_bound(py, &[shape.bind(py), dtype.bind(py)]);
            list.append(tup)?;
        }
        Ok(list)
    }

    /// Set tensor_meta from a Python list of tuples.
    #[setter]
    fn set_tensor_meta(&mut self, value: Vec<(Py<PyAny>, Py<PyAny>)>) {
        self.tensor_meta = value;
    }

    #[getter]
    fn tensor_device(&self) -> Option<&str> {
        self.tensor_device.as_deref()
    }

    #[setter]
    fn set_tensor_device(&mut self, value: Option<String>) {
        self.tensor_device = value;
    }

    /// Pickle support: serialize to a Python dict.
    fn __getstate__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new_bound(py);
        let meta_list = PyList::empty_bound(py);
        for (shape, dtype) in &self.tensor_meta {
            let tup = PyTuple::new_bound(py, &[shape.bind(py), dtype.bind(py)]);
            meta_list.append(tup)?;
        }
        dict.set_item("tensor_meta", meta_list)?;
        dict.set_item("tensor_device", &self.tensor_device)?;
        Ok(dict)
    }

    /// Pickle support: restore from a Python dict.
    fn __setstate__(&mut self, state: &Bound<'_, PyDict>) -> PyResult<()> {
        let meta_any = state.get_item("tensor_meta")?.unwrap();
        let meta_list: &Bound<'_, PyList> = meta_any.downcast()?;
        let mut tensor_meta = Vec::new();
        for item in meta_list.iter() {
            let tup: &Bound<'_, PyTuple> = item.downcast()?;
            let shape = tup.get_item(0)?.unbind();
            let dtype = tup.get_item(1)?.unbind();
            tensor_meta.push((shape, dtype));
        }
        self.tensor_meta = tensor_meta;
        let device_any = state.get_item("tensor_device")?;
        self.tensor_device = device_any
            .map(|v| v.extract::<Option<String>>())
            .transpose()?
            .flatten();
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "TensorTransportMetadata(tensor_meta=[{} items], tensor_device={:?})",
            self.tensor_meta.len(),
            self.tensor_device,
        )
    }

    fn __eq__(&self, py: Python<'_>, other: &Self) -> PyResult<bool> {
        if self.tensor_device != other.tensor_device {
            return Ok(false);
        }
        if self.tensor_meta.len() != other.tensor_meta.len() {
            return Ok(false);
        }
        for ((s1, d1), (s2, d2)) in self.tensor_meta.iter().zip(other.tensor_meta.iter()) {
            let s_eq: bool = s1.bind(py).eq(s2.bind(py))?;
            let d_eq: bool = d1.bind(py).eq(d2.bind(py))?;
            if !s_eq || !d_eq {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
