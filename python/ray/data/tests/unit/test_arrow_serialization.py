import sys
from unittest import mock

import numpy as np
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

from ray._private.arrow_serialization import (
    PicklableArrayPayload,
    _align_bit_offset,
    _bytes_for_bits,
    _copy_bitpacked_buffer_if_needed,
    _copy_buffer_if_needed,
    _copy_normal_buffer_if_needed,
    _copy_offsets_buffer_if_needed,
)


@pytest.mark.parametrize(
    "n,expected",
    [(0, 0)] + [(i, 8) for i in range(1, 9)] + [(i, 16) for i in range(9, 17)],
)
def test_bytes_for_bits_manual(n, expected):
    assert _bytes_for_bits(n) == expected


def test_bytes_for_bits_auto():
    M = 128
    expected = [((n - 1) // 8 + 1) * 8 for n in range(M)]
    for n, e in enumerate(expected):
        assert _bytes_for_bits(n) == e, n


def test_align_bit_offset_auto():
    M = 10
    n = M * (2**8 - 1)
    # Represent an integer as a Pyarrow buffer of bytes.
    bytes_ = n.to_bytes(M, sys.byteorder)
    buf = pa.py_buffer(bytes_)
    for slice_len in range(1, M):
        for bit_offset in range(1, n - slice_len * 8):
            byte_length = _bytes_for_bits(bit_offset + slice_len * 8) // 8
            # Shift the buffer to eliminate the offset.
            out_buf = _align_bit_offset(buf, bit_offset, byte_length)
            # Check that shifted buffer is equivalent to our base int shifted by the
            # same number of bits.
            assert int.from_bytes(out_buf.to_pybytes(), sys.byteorder) == (
                n >> bit_offset
            )


@mock.patch("ray._private.arrow_serialization._copy_normal_buffer_if_needed")
@mock.patch("ray._private.arrow_serialization._copy_bitpacked_buffer_if_needed")
def test_copy_buffer_if_needed(mock_bitpacked, mock_normal):
    # Test that type-based buffer copy dispatch works as expected.
    bytes_ = b"abcd"
    buf = pa.py_buffer(bytes_)
    offset = 1
    length = 2

    # Normal (non-boolean) buffer copy path.
    type_ = pa.int32()
    _copy_buffer_if_needed(buf, type_, offset, length)
    expected_byte_width = 4
    mock_normal.assert_called_once_with(buf, expected_byte_width, offset, length)
    mock_normal.reset_mock()

    type_ = pa.int64()
    _copy_buffer_if_needed(buf, type_, offset, length)
    expected_byte_width = 8
    mock_normal.assert_called_once_with(buf, expected_byte_width, offset, length)
    mock_normal.reset_mock()

    # Boolean buffer copy path.
    type_ = pa.bool_()
    _copy_buffer_if_needed(buf, type_, offset, length)
    mock_bitpacked.assert_called_once_with(buf, offset, length)
    mock_bitpacked.reset_mock()


def test_copy_normal_buffer_if_needed():
    bytes_ = b"abcd"
    buf = pa.py_buffer(bytes_)
    byte_width = 1
    uncopied_buf = _copy_normal_buffer_if_needed(buf, byte_width, 0, len(bytes_))
    assert uncopied_buf.address == buf.address
    assert uncopied_buf.size == len(bytes_)
    for offset in range(1, len(bytes_) - 1):
        for length in range(1, len(bytes_) - offset):
            copied_buf = _copy_normal_buffer_if_needed(buf, byte_width, offset, length)
            assert copied_buf.address != buf.address
            assert copied_buf.size == length


def test_copy_bitpacked_buffer_if_needed():
    M = 20
    n = M * 8
    # Represent an integer as a pyarrow buffer of bytes.
    bytes_ = (n * 8).to_bytes(M, sys.byteorder)
    buf = pa.py_buffer(bytes_)
    for offset in range(0, n - 1):
        for length in range(1, n - offset):
            copied_buf = _copy_bitpacked_buffer_if_needed(buf, offset, length)
            if offset > 0:
                assert copied_buf.address != buf.address
            else:
                assert copied_buf.address == buf.address
            # Buffer needs to include bits remaining in byte after adjusting for bit
            # offset..
            assert copied_buf.size == ((length + (offset % 8) - 1) // 8) + 1


@pytest.mark.parametrize(
    "arr_type,expected_offset_type",
    [
        (pa.list_(pa.int64()), pa.int32()),
        (pa.string(), pa.int32()),
        (pa.binary(), pa.int32()),
        (pa.large_list(pa.int64()), pa.int64()),
        (pa.large_string(), pa.int64()),
        (pa.large_binary(), pa.int64()),
    ],
)
def test_copy_offsets_buffer_if_needed(arr_type, expected_offset_type):
    offset_arr = pa.array([0, 1, 3, 6, 10, 15, 21], type=expected_offset_type)
    buf = offset_arr.buffers()[1]
    offset = 2
    length = 3
    offset_buf, data_offset, data_length = _copy_offsets_buffer_if_needed(
        buf, arr_type, offset, length
    )
    assert data_offset == 3
    assert data_length == 12
    truncated_offset_arr = pa.Array.from_buffers(
        expected_offset_type, length, [None, offset_buf]
    )
    expected_offset_arr = pa.array([0, 3, 7], type=expected_offset_type)
    assert truncated_offset_arr.equals(expected_offset_arr)


@pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("10.0.0"),
    reason="FixedShapeTensorArray is not supported in PyArrow < 10.0.0",
)
def test_fixed_shape_tensor_array_serialization():
    a = pa.FixedShapeTensorArray.from_numpy_ndarray(
        np.arange(4 * 2 * 3).reshape(4, 2, 3)
    )
    payload = PicklableArrayPayload.from_array(a)
    a2 = payload.to_array()
    assert a == a2


class _VariableShapeTensorType(pa.ExtensionType):
    def __init__(
        self,
        value_type: pa.DataType,
        ndim: int,
    ) -> None:
        self.value_type = value_type
        self.ndim = ndim
        super().__init__(
            pa.struct(
                [
                    pa.field("data", pa.list_(value_type)),
                    pa.field("shape", pa.list_(pa.int32(), ndim)),
                ]
            ),
            "variable_shape_tensor",
        )

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes):
        ndim = storage_type[1].type.list_size
        value_type = storage_type[0].type.value_type
        return cls(value_type, ndim)


def test_variable_shape_tensor_serialization():
    t = _VariableShapeTensorType(pa.float32(), 2)
    values = [
        {
            "data": np.arange(2 * 3, dtype=np.float32).tolist(),
            "shape": [2, 3],
        },
        {
            "data": np.arange(4 * 5, dtype=np.float32).tolist(),
            "shape": [4, 5],
        },
    ]
    storage = pa.array(values, type=t.storage_type)
    ar = pa.ExtensionArray.from_storage(t, storage)
    payload = PicklableArrayPayload.from_array(ar)
    ar2 = payload.to_array()
    assert ar == ar2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
