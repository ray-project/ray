"""Transport manager registry and utilities for the Rust Ray backend.

Adapted from python/ray/experimental/gpu_object_manager/util.py
for standalone use.
"""

import threading
from typing import Dict, List, Optional

from ray.experimental.rdt.tensor_transport_manager import (
    TensorTransportManager,
    TensorTransportMetadata,
)

# Singleton instances of transport managers
_transport_managers: Dict[str, TensorTransportManager] = {}
_transport_managers_lock = threading.Lock()
_default_registered = False


def _ensure_default_transports_registered():
    global _default_registered
    with _transport_managers_lock:
        if _default_registered:
            return
        _default_registered = True
        try:
            from ray.experimental.rdt.nixl_tensor_transport import NixlTensorTransport
            _transport_managers["NIXL"] = NixlTensorTransport()
        except ImportError:
            pass


def get_tensor_transport_manager(
    transport_name: str,
) -> TensorTransportManager:
    """Get the singleton tensor transport manager for the given protocol."""
    _ensure_default_transports_registered()
    with _transport_managers_lock:
        transport_name = transport_name.upper()
        if transport_name in _transport_managers:
            return _transport_managers[transport_name]
        raise ValueError(f"Unsupported tensor transport protocol: {transport_name}")


def create_empty_tensors_from_metadata(
    tensor_transport_meta: TensorTransportMetadata,
) -> list:
    """Allocate empty tensors matching the metadata shapes/dtypes."""
    import torch

    tensors = []
    device = tensor_transport_meta.tensor_device
    for meta in tensor_transport_meta.tensor_meta:
        shape, dtype = meta
        tensor = torch.empty(shape, dtype=dtype, device=device)
        tensors.append(tensor)
    return tensors


def reset_transport_managers():
    """Reset all transport managers (called on ray.shutdown)."""
    global _default_registered
    with _transport_managers_lock:
        _transport_managers.clear()
        _default_registered = False
