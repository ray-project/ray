"""
This module is intended for implementing internal serializers for some
site packages.
"""

import sys

from ray.util.annotations import DeveloperAPI

import warnings

_TORCH_WARNING_FILTER_ACTIVATE = True


class _TorchTensorReducingHelper:
    def __init__(self, tensor: "torch.Tensor"):
        self.tensor = tensor

    @classmethod
    def rebuild_tensor(cls, rebuild_func, device, ndarray, params):
        import torch

        global _TORCH_WARNING_FILTER_ACTIVATE
        # filtering warning messages would be the bottleneck for
        # deserializing torch tensors. Since the warning only prompts once,
        # we would only deal with it for the first time.
        if _TORCH_WARNING_FILTER_ACTIVATE:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=UserWarning,
                    message="The given NumPy array is not writeable",
                )
                _tensor = torch.from_numpy(ndarray)
            _TORCH_WARNING_FILTER_ACTIVATE = False
        else:
            _tensor = torch.from_numpy(ndarray)
        if device != torch.device("cpu"):
            _tensor = _tensor.to(device)
        tensor = rebuild_func(_tensor.storage(), *params)
        return cls(tensor)

    @classmethod
    def rebuild_sparse_tensor(cls, rebuild_func, content):
        tensor = rebuild_func(*content)
        return cls(tensor)

    def __reduce_ex__(self, protocol):
        _rebuild_func, content = self.tensor.__reduce_ex__(protocol)
        if self.tensor.is_sparse:
            # Torch will help us reduce the sparse tensor into
            # several continuous tensors.
            return self.rebuild_sparse_tensor, (_rebuild_func, content)
        # By only replacing the storage with a numpy array, we can reuse
        # zero-copy serialization while keeping all other params of the
        # torch tensor.
        return self.rebuild_tensor, (
            _rebuild_func,
            self.tensor.device,
            self.tensor.detach().cpu().numpy(),
            content[1:],
        )


def _unwrap_tensor(s: "torch.Tensor"):
    return s.tensor


def torch_tensor_reducer(tensor: "torch.Tensor"):
    return _unwrap_tensor, (_TorchTensorReducingHelper(tensor),)


@DeveloperAPI
def register_torch_zero_copy_serializer(serialization_context):
    """Register zero copy torch serializer to Ray."""
    try:
        import torch
    except ImportError:
        return
    serialization_context._register_cloudpickle_reducer(
        torch.Tensor, torch_tensor_reducer
    )


@DeveloperAPI
def register_starlette_serializer(serialization_context):
    try:
        import starlette.datastructures
    except ImportError:
        return

    # Starlette's app.state object is not serializable
    # because it overrides __getattr__
    serialization_context._register_cloudpickle_serializer(
        starlette.datastructures.State,
        custom_serializer=lambda s: s._state,
        custom_deserializer=lambda s: starlette.datastructures.State(s),
    )


@DeveloperAPI
def apply(serialization_context):
    from ray._private.pydantic_compat import register_pydantic_serializers

    register_pydantic_serializers(serialization_context)
    register_starlette_serializer(serialization_context)
    register_torch_zero_copy_serializer(serialization_context)

    if sys.platform != "win32":
        from ray._private.arrow_serialization import (
            _register_custom_datasets_serializers,
        )

        _register_custom_datasets_serializers(serialization_context)
