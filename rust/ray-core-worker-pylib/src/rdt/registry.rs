// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Transport manager registry (singleton).
//!
//! Ported from `python/ray/experimental/rdt/util.py`.
//! Manages registration and lazy instantiation of tensor transport backends.

#[cfg(feature = "python")]
use std::collections::HashMap;
#[cfg(feature = "python")]
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "python")]
use pyo3::exceptions::PyValueError;
#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Info about a registered transport manager.
#[cfg(feature = "python")]
struct TransportManagerInfo {
    transport_manager_class: Py<PyAny>,
    devices: Vec<String>,
    data_type: Py<PyAny>,
}

/// Interior state of the transport registry.
#[cfg(feature = "python")]
struct RegistryInner {
    info: HashMap<String, TransportManagerInfo>,
    managers: HashMap<String, Py<PyAny>>,
    has_custom: bool,
    defaults_registered: bool,
}

#[cfg(feature = "python")]
fn get_registry() -> &'static Mutex<RegistryInner> {
    static REGISTRY: OnceLock<Mutex<RegistryInner>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(RegistryInner {
            info: HashMap::new(),
            managers: HashMap::new(),
            has_custom: false,
            defaults_registered: false,
        })
    })
}

#[cfg(feature = "python")]
const DEFAULT_TRANSPORTS: &[&str] = &["NIXL", "GLOO", "NCCL", "CUDA_IPC"];

/// Register a new tensor transport backend.
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (transport_name, devices, transport_manager_class, data_type))]
pub fn register_tensor_transport(
    py: Python<'_>,
    transport_name: &str,
    devices: Vec<String>,
    transport_manager_class: Py<PyAny>,
    data_type: Py<PyAny>,
) -> PyResult<()> {
    let transport_name = transport_name.to_uppercase();
    let mut reg = get_registry().lock().unwrap();

    if reg.info.contains_key(&transport_name) {
        return Err(PyValueError::new_err(format!(
            "Transport {transport_name} already registered."
        )));
    }

    // Validate it's a subclass of TensorTransportManager.
    let ttm_mod = py.import_bound("ray.experimental.rdt.tensor_transport_manager")?;
    let ttm_class = ttm_mod.getattr("TensorTransportManager")?;
    let builtins = py.import_bound("builtins")?;
    let is_subclass: bool = builtins
        .call_method1("issubclass", (transport_manager_class.bind(py), &ttm_class))?
        .extract()?;
    if !is_subclass {
        let name: String = transport_manager_class
            .bind(py)
            .getattr("__name__")?
            .extract()?;
        return Err(PyValueError::new_err(format!(
            "transport_manager_class {name} must be a subclass of TensorTransportManager."
        )));
    }

    if !DEFAULT_TRANSPORTS.contains(&transport_name.as_str()) {
        reg.has_custom = true;
    }

    reg.info.insert(
        transport_name,
        TransportManagerInfo {
            transport_manager_class,
            devices,
            data_type,
        },
    );
    Ok(())
}

/// Get (or create) the singleton transport manager for the given transport name.
#[cfg(feature = "python")]
#[pyfunction]
pub fn get_tensor_transport_manager(py: Python<'_>, transport_name: &str) -> PyResult<PyObject> {
    ensure_default_transports_registered(py)?;
    let mut reg = get_registry().lock().unwrap();

    if let Some(mgr) = reg.managers.get(transport_name) {
        return Ok(mgr.clone_ref(py));
    }

    let info = reg.info.get(transport_name).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Unsupported tensor transport protocol: {transport_name}"
        ))
    })?;

    let instance = info.transport_manager_class.call0(py)?;
    reg.managers
        .insert(transport_name.to_string(), instance.clone_ref(py));
    Ok(instance)
}

/// Get the data type for the given transport.
#[cfg(feature = "python")]
#[pyfunction]
pub fn get_transport_data_type(py: Python<'_>, tensor_transport: &str) -> PyResult<PyObject> {
    ensure_default_transports_registered(py)?;
    let reg = get_registry().lock().unwrap();
    let info = reg.info.get(tensor_transport).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Unsupported tensor transport protocol: {tensor_transport}"
        ))
    })?;
    Ok(info.data_type.clone_ref(py))
}

/// Check if a device type is supported by the given transport.
#[cfg(feature = "python")]
#[pyfunction]
pub fn device_match_transport(
    py: Python<'_>,
    device: &str,
    tensor_transport: &str,
) -> PyResult<bool> {
    ensure_default_transports_registered(py)?;
    let reg = get_registry().lock().unwrap();
    let info = reg.info.get(tensor_transport).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Unsupported tensor transport protocol: {tensor_transport}"
        ))
    })?;
    Ok(info.devices.iter().any(|d| d == device))
}

/// Normalize transport name to uppercase and validate it exists.
#[cfg(feature = "python")]
#[pyfunction]
pub fn normalize_and_validate_tensor_transport(
    py: Python<'_>,
    tensor_transport: &str,
) -> PyResult<String> {
    ensure_default_transports_registered(py)?;
    let upper = tensor_transport.to_uppercase();
    let reg = get_registry().lock().unwrap();
    if !reg.info.contains_key(&upper) {
        return Err(PyValueError::new_err(format!(
            "Invalid tensor transport: {upper}"
        )));
    }
    Ok(upper)
}

/// Validate that the transport is one-sided (e.g. NIXL).
/// Raises ValueError for two-sided transports like NCCL.
#[cfg(feature = "python")]
#[pyfunction]
pub fn validate_one_sided(
    py: Python<'_>,
    tensor_transport: &str,
    ray_usage_func: &str,
) -> PyResult<()> {
    ensure_default_transports_registered(py)?;
    let reg = get_registry().lock().unwrap();
    let info = reg.info.get(tensor_transport).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Unsupported tensor transport protocol: {tensor_transport}"
        ))
    })?;
    let is_one_sided: bool = info
        .transport_manager_class
        .call_method0(py, "is_one_sided")?
        .extract(py)?;
    if !is_one_sided {
        return Err(PyValueError::new_err(format!(
            "Trying to use two-sided tensor transport: {tensor_transport} for {ray_usage_func}. \
             This is only supported for one-sided transports such as NIXL or the OBJECT_STORE."
        )));
    }
    Ok(())
}

/// Ensure default transports (NIXL, GLOO, NCCL, CUDA_IPC) are registered.
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "_ensure_default_transports_registered")]
pub fn py_ensure_default_transports_registered(py: Python<'_>) -> PyResult<()> {
    ensure_default_transports_registered(py)
}

#[cfg(feature = "python")]
fn ensure_default_transports_registered(py: Python<'_>) -> PyResult<()> {
    {
        let reg = get_registry().lock().unwrap();
        if reg.defaults_registered {
            return Ok(());
        }
    }

    // Mark as registered before importing (prevents re-entry).
    {
        let mut reg = get_registry().lock().unwrap();
        if reg.defaults_registered {
            return Ok(());
        }
        reg.defaults_registered = true;
    }

    // Import torch. If unavailable, skip default registration.
    let torch = match py.import_bound("torch") {
        Ok(t) => t,
        Err(_) => return Ok(()),
    };
    let tensor_type = torch.getattr("Tensor")?;

    // Import transport classes.
    let nixl_mod = py.import_bound("ray.experimental.rdt.nixl_tensor_transport")?;
    let nixl_class = nixl_mod.getattr("NixlTensorTransport")?;

    let coll_mod = py.import_bound("ray.experimental.rdt.collective_tensor_transport")?;
    let gloo_class = coll_mod.getattr("GLOOTensorTransport")?;
    let nccl_class = coll_mod.getattr("NCCLTensorTransport")?;

    let ipc_mod = py.import_bound("ray.experimental.rdt.cuda_ipc_transport")?;
    let ipc_class = ipc_mod.getattr("CudaIpcTransport")?;

    register_tensor_transport(
        py,
        "NIXL",
        vec!["cuda".into(), "cpu".into()],
        nixl_class.unbind(),
        tensor_type.clone().unbind(),
    )?;
    register_tensor_transport(
        py,
        "GLOO",
        vec!["cpu".into()],
        gloo_class.unbind(),
        tensor_type.clone().unbind(),
    )?;
    register_tensor_transport(
        py,
        "NCCL",
        vec!["cuda".into()],
        nccl_class.unbind(),
        tensor_type.clone().unbind(),
    )?;
    register_tensor_transport(
        py,
        "CUDA_IPC",
        vec!["cuda".into()],
        ipc_class.unbind(),
        tensor_type.unbind(),
    )?;

    Ok(())
}

/// Reset all transport managers and registry state.
/// Called on `ray.shutdown()`.
#[cfg(feature = "python")]
#[pyfunction]
pub fn _reset_transport_registry(_py: Python<'_>) -> PyResult<()> {
    let mut reg = get_registry().lock().unwrap();
    reg.info.clear();
    reg.managers.clear();
    reg.has_custom = false;
    reg.defaults_registered = false;
    Ok(())
}

/// Check if custom (non-default) transports have been registered.
#[cfg(feature = "python")]
#[pyfunction]
pub fn _has_custom_transports(_py: Python<'_>) -> bool {
    let reg = get_registry().lock().unwrap();
    reg.has_custom
}

/// Get the transport_manager_info dict as a Python dict.
/// Used by `register_custom_tensor_transports_on_actor`.
#[cfg(feature = "python")]
#[pyfunction]
pub fn _get_transport_manager_info(py: Python<'_>) -> PyResult<PyObject> {
    ensure_default_transports_registered(py)?;
    let reg = get_registry().lock().unwrap();
    let dict = pyo3::types::PyDict::new_bound(py);
    let util_mod = py.import_bound("ray.experimental.rdt.util")?;
    let info_class = util_mod.getattr("TransportManagerInfo")?;
    for (name, info) in &reg.info {
        let devices_list = pyo3::types::PyList::new_bound(py, &info.devices);
        let py_info = info_class.call1((
            info.transport_manager_class.bind(py),
            devices_list,
            info.data_type.bind(py),
        ))?;
        dict.set_item(name, py_info)?;
    }
    Ok(dict.unbind().into())
}
