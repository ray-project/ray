"""Parity-gap tests for the Rust Python/native surface.

These tests are intentionally marked xfail because they encode behavior expected
from the Cython/C++ backend contract that the current Rust port does not yet
fully preserve.
"""

from __future__ import annotations

import importlib.util
import inspect
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
RUST_RAY_DIR = ROOT / "rust" / "ray"


def _clear_ray_modules():
    for name in list(sys.modules):
        if name == "ray" or name.startswith("ray.") or name == "_raylet":
            sys.modules.pop(name, None)


def _install_stub_raylet():
    mod = types.ModuleType("_raylet")
    mod.start_cluster = lambda *args, **kwargs: None
    for name in [
        "PyCoreWorker",
        "PyGcsClient",
        "PyWorkerID",
        "PyObjectID",
        "PyActorID",
        "PyTaskID",
        "PyNodeID",
        "PyJobID",
        "PyObjectRef",
        "PyPlacementGroupID",
    ]:
        setattr(mod, name, type(name, (), {}))
    sys.modules["_raylet"] = mod


def _load_rust_ray_package():
    _clear_ray_modules()
    _install_stub_raylet()
    spec = importlib.util.spec_from_file_location(
        "ray",
        RUST_RAY_DIR / "__init__.py",
        submodule_search_locations=[str(RUST_RAY_DIR)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["ray"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_object_ref_async_contract_parity():
    ray_mod = _load_rust_ray_package()
    assert hasattr(ray_mod.ObjectRef, "future")
    assert hasattr(ray_mod.ObjectRef, "as_future")
    assert hasattr(ray_mod.ObjectRef, "_on_completed")


def test_object_ref_constructor_contract_parity():
    ray_mod = _load_rust_ray_package()
    sig = inspect.signature(ray_mod.ObjectRef)
    params = list(sig.parameters)
    assert "owner_addr" in params
    assert "call_site_data" in params
    assert "skip_adding_local_ref" in params


def test_raylet_symbol_parity_smoke():
    shim_path = RUST_RAY_DIR / "_raylet.py"
    source = shim_path.read_text()
    assert "CoreWorker" in source
    assert "GcsClient" in source
    assert "ObjectRef" in source


def test_object_ref_has_owner_address_method():
    ray_mod = _load_rust_ray_package()
    assert hasattr(ray_mod.ObjectRef, "owner_address")


def test_object_ref_has_on_completed_hook():
    ray_mod = _load_rust_ray_package()
    assert hasattr(ray_mod.ObjectRef, "_on_completed")


def test_raylet_shim_mentions_core_worker_or_gcs_client():
    shim_path = RUST_RAY_DIR / "_raylet.py"
    source = shim_path.read_text()
    assert "PyCoreWorker" in source or "CoreWorker" in source
    assert "PyGcsClient" in source or "GcsClient" in source


def test_gcs_client_async_and_multi_get_contract_present():
    source = (ROOT / "rust" / "ray-core-worker-pylib" / "src" / "gcs_client.rs").read_text()
    assert "async_internal_kv_get" in source
    assert "async_internal_kv_multi_get" in source
    assert "internal_kv_multi_get" in source


def test_ray_util_placement_group_helpers_present():
    util_init = (RUST_RAY_DIR / "util" / "__init__.py").read_text()
    assert "placement_group" in util_init
    assert "remove_placement_group" in util_init
    assert "get_placement_group" in util_init


def test_raylet_shim_exports_cluster_and_client_entrypoints():
    shim_path = RUST_RAY_DIR / "_raylet.py"
    source = shim_path.read_text()
    assert "start_cluster" in source
    assert "PyCoreWorker" in source
    assert "PyGcsClient" in source


def test_rdt_store_has_no_python_fallback_path():
    source = (RUST_RAY_DIR / "experimental" / "rdt" / "rdt_store.py").read_text()
    assert "except ImportError" not in source
    assert "fallback pure-Python implementation" not in source
