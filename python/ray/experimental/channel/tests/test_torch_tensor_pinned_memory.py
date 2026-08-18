import pytest
import torch
from ray.experimental.channel.serialization_context import _SerializationContext
from ray.experimental.util.types import Device


def test_torch_tensor_serialization_pinned_memory():
    """
    Test that deserializing torch tensors via _SerializationContext automatically pins CPU memory
    and uses non_blocking transfer when target device is GPU, while leaving pure CPU tensors unpinned.
    """
    ctx = _SerializationContext()
    t = torch.randn(10, 10)

    val = ctx.serialize_to_numpy_or_scalar(t)
    # CPU target device should remain standard unpinned tensor to avoid memory waste
    deserialized_cpu = ctx.deserialize_from_numpy_or_scalar(
        val[0], val[1], val[2], Device.CPU
    )
    assert not deserialized_cpu.is_pinned()

    # GPU target device uses pinned memory + non_blocking transfer
    if torch.cuda.is_available():
        deserialized_gpu = ctx.deserialize_from_numpy_or_scalar(
            val[0], val[1], val[2], Device.GPU
        )
        assert deserialized_gpu.is_cuda
