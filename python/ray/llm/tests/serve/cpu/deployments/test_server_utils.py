import base64
import struct
import sys

import pytest

from ray.llm._internal.serve.deployments.utils.server_utils import floats_to_base64


def test_floats_to_base64_empty_list():
    """Test encoding an empty list of floats."""
    assert floats_to_base64([]) == ""


def test_floats_to_base64_single_float():
    """Test encoding a single float."""
    float_list = [3.14159]
    binary = struct.pack("f", float_list[0])
    expected = base64.b64encode(binary).decode("utf-8")
    assert floats_to_base64(float_list) == expected


def test_floats_to_base64_multiple_floats():
    """Test encoding multiple floats."""
    float_list = [1.0, 2.0, 3.0, -4.5, 0.0]
    binary = struct.pack(f"{len(float_list)}f", *float_list)
    expected = base64.b64encode(binary).decode("utf-8")
    assert floats_to_base64(float_list) == expected


def test_floats_to_base64_round_trip():
    """Test that encoded floats can be decoded back to the original values."""
    float_list = [1.5, -2.75, 3.333, 0.0, -0.0, 1e-10]
    encoded = floats_to_base64(float_list)
    # Decode the base64 string back to binary
    decoded_binary = base64.b64decode(encoded)
    # Unpack the binary back to floats
    decoded_floats = struct.unpack(f"{len(float_list)}f", decoded_binary)
    # Check that the values are close (not exactly equal due to floating point precision)
    for original, decoded in zip(float_list, decoded_floats):
        assert abs(original - decoded) < 1e-6


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
