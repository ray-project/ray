# coding: utf-8
import logging
import os
import sys

import pytest
import torch

from ray.experimental.channel.serialization_context import _SerializationContext
from ray.experimental.util.types import Device

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "scalar_and_dtype",
    [
        # Basic tests
        (1.23456, torch.float16),
        (1.23456, torch.bfloat16),
        (1.23456, torch.float32),
        (1.23456, torch.float64),
        (123, torch.int8),
        (123, torch.int16),
        (123456, torch.int32),
        (123456, torch.int64),
        (123, torch.uint8),
        (123, torch.uint16),
        (123456, torch.uint32),
        (123456, torch.uint64),
        (True, torch.bool),
        # Boundary values tests - integers
        (127, torch.int8),  # INT8_MAX
        (-128, torch.int8),  # INT8_MIN
        (32767, torch.int16),  # INT16_MAX
        (-32768, torch.int16),  # INT16_MIN
        (2147483647, torch.int32),  # INT32_MAX
        (-2147483648, torch.int32),  # INT32_MIN
        (9223372036854775807, torch.int64),  # INT64_MAX
        (-9223372036854775808, torch.int64),  # INT64_MIN
        # Boundary values tests - unsigned integers
        (255, torch.uint8),  # UINT8_MAX
        (0, torch.uint8),  # UINT8_MIN
        (65535, torch.uint16),  # UINT16_MAX
        (0, torch.uint16),  # UINT16_MIN
        (4294967295, torch.uint32),  # UINT32_MAX
        (0, torch.uint32),  # UINT32_MIN
        (18446744073709551615, torch.uint64),  # UINT64_MAX
        (0, torch.uint64),  # UINT64_MIN
        # Floating point special values
        (float("inf"), torch.float32),
        (float("-inf"), torch.float32),
        (float("nan"), torch.float32),
        (float("inf"), torch.float64),
        (float("-inf"), torch.float64),
        (float("nan"), torch.float64),
        # Float precision tests
        (1.2345678901234567, torch.float32),  # Beyond float32 precision
        (1.2345678901234567, torch.float64),  # Within float64 precision
        (1e-45, torch.float32),  # Near float32 smallest positive normal
        (
            2.2250738585072014e-308,
            torch.float64,
        ),  # Near float64 smallest positive normal
    ],
)
def test_scalar_tensor(scalar_and_dtype):
    scalar, dtype = scalar_and_dtype
    context = _SerializationContext()
    scalar_tensor = torch.tensor(scalar, dtype=dtype)
    np_array, tensor_dtype, tensor_device_type = context.serialize_to_numpy_or_scalar(
        scalar_tensor
    )
    assert tensor_dtype == dtype
    deserialized_tensor = context.deserialize_from_numpy_or_scalar(
        np_array, dtype, tensor_device_type, Device.CPU
    )

    # Special handling for NaN values
    if torch.is_floating_point(scalar_tensor) and torch.isnan(scalar_tensor):
        assert torch.isnan(deserialized_tensor)
    else:
        assert (deserialized_tensor == scalar_tensor).all()


@pytest.mark.parametrize(
    "tensor_shape_and_dtype",
    [
        ((10, 10), torch.float16),
        ((10, 10), torch.bfloat16),
        ((10, 10, 10), torch.float32),
        ((10, 10, 10, 10), torch.float64),
        ((10, 10), torch.int8),
        ((10, 10), torch.int16),
        ((10, 10), torch.int32),
        ((10, 10), torch.int64),
        ((10, 10), torch.uint8),
        ((10, 10), torch.uint16),
        ((10, 10), torch.uint32),
        ((10, 10), torch.uint64),
    ],
)
def test_non_scalar_tensor(tensor_shape_and_dtype):
    tensor_shape, dtype = tensor_shape_and_dtype
    context = _SerializationContext()

    # Create tensor based on dtype with varying values
    if dtype in [torch.float16, torch.bfloat16, torch.float32, torch.float64]:
        # For floating point types, use randn
        tensor = torch.randn(*tensor_shape).to(dtype)
    else:
        # For integer types, create varying values within appropriate ranges
        total_elements = torch.prod(torch.tensor(tensor_shape)).item()

        if dtype == torch.uint8:
            # Range: 0 to 255
            values = torch.arange(0, min(total_elements, 256), dtype=torch.int32) % 256
        elif dtype == torch.uint16:
            # Range: 0 to 65535
            values = (
                torch.arange(0, min(total_elements, 65536), dtype=torch.int32) % 65536
            )
        elif dtype == torch.int8:
            # Range: -128 to 127
            values = (
                torch.arange(0, min(total_elements, 256), dtype=torch.int32) % 256
            ) - 128
        elif dtype == torch.int16:
            # Range: -32768 to 32767
            values = (
                torch.arange(0, min(total_elements, 65536), dtype=torch.int32) % 65536
            ) - 32768
        elif dtype == torch.int32:
            # Use a smaller range to avoid overflow
            values = torch.arange(0, total_elements, dtype=torch.int32) % 10000 - 5000
        else:  # int64
            # Use a smaller range to avoid overflow
            values = torch.arange(0, total_elements, dtype=torch.int64) % 10000 - 5000

        # Reshape the values to match the target shape
        tensor = values.reshape(tensor_shape).to(dtype)

    np_array, tensor_dtype, tensor_device_type = context.serialize_to_numpy_or_scalar(
        tensor
    )
    deserialized_tensor = context.deserialize_from_numpy_or_scalar(
        np_array, tensor_dtype, tensor_device_type, Device.CPU
    )

    assert (tensor == deserialized_tensor).all()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
