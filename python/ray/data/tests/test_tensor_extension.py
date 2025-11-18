import itertools
import threading
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import (
    ArrowConversionError,
    ArrowTensorArray,
    ArrowTensorType,
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
    _are_contiguous_1d_views,
    _concat_ndarrays,
    _extension_array_concat_supported,
    concat_tensor_arrays,
    unify_tensor_arrays,
)
from ray.air.util.tensor_extensions.pandas import TensorArray, TensorDtype
from ray.air.util.tensor_extensions.utils import (
    create_ragged_ndarray,
)
from ray.data import DataContext


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize(
    "values",
    [
        [np.zeros((3, 1)), np.zeros((3, 2))],
        [np.zeros((3,))],
    ],
)
def test_create_ragged_ndarray(values, restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    ragged_array = create_ragged_ndarray(values)
    assert len(ragged_array) == len(values)
    for actual_array, expected_array in zip(ragged_array, values):
        np.testing.assert_array_equal(actual_array, expected_array)


def test_tensor_array_validation():
    # Test unknown input type raises TypeError.
    with pytest.raises(TypeError):
        TensorArray(object())

    # Test non-primitive element raises TypeError.
    with pytest.raises(TypeError):
        TensorArray(np.array([object(), object()]))

    with pytest.raises(TypeError):
        TensorArray([object(), object()])


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_scalar_tensor_array_roundtrip(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.arange(10)
    ata = ArrowTensorArray.from_numpy(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out, arr)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_scalar_tensor_array_roundtrip_boolean(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.array([True, False, False, True])
    ata = ArrowTensorArray.from_numpy(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    # Zero-copy is not possible since Arrow bitpacks boolean arrays while NumPy does
    # not.
    out = ata.to_numpy(zero_copy_only=False)
    np.testing.assert_array_equal(out, arr)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_scalar_tensor_array_roundtrip(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.arange(10)
    ta = TensorArray(arr)
    assert isinstance(ta.dtype, TensorDtype)
    assert len(ta) == len(arr)
    out = ta.to_numpy()
    np.testing.assert_array_equal(out, arr)

    # Check Arrow conversion.
    ata = ta.__arrow_array__()
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out, arr)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_tensor_array_validation(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    # Test tensor elements with differing dimensions raises ValueError.
    with pytest.raises(ValueError):
        ArrowVariableShapedTensorArray.from_numpy([np.ones((2, 2)), np.ones((3, 3, 3))])

    # Test arbitrary object raises ValueError.
    with pytest.raises(ValueError):
        ArrowVariableShapedTensorArray.from_numpy(object())

    # Test empty array raises ValueError.
    with pytest.raises(ValueError):
        ArrowVariableShapedTensorArray.from_numpy(np.array([]))

    # Test deeply ragged tensor raises ValueError.
    with pytest.raises(ValueError):
        ArrowVariableShapedTensorArray.from_numpy(
            np.array(
                [
                    np.array(
                        [
                            np.array([1, 2]),
                            np.array([3, 4, 5]),
                        ],
                        dtype=object,
                    ),
                    np.array(
                        [
                            np.array([5, 6, 7, 8]),
                        ],
                        dtype=object,
                    ),
                    np.array(
                        [
                            np.array([5, 6, 7, 8]),
                            np.array([5, 6, 7, 8]),
                            np.array([5, 6, 7, 8]),
                        ],
                        dtype=object,
                    ),
                ],
                dtype=object,
            )
        )


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_tensor_array_roundtrip(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    arr = np.array(arrs, dtype=object)
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    for o, a in zip(out, arr):
        np.testing.assert_array_equal(o, a)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_tensor_array_roundtrip_boolean(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.array(
        [[True, False], [False, False, True], [False], [True, True, False, True]],
        dtype=object,
    )
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    for o, a in zip(out, arr):
        np.testing.assert_array_equal(o, a)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_tensor_array_roundtrip_contiguous_optimization(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    # Test that a roundtrip on slices of an already-contiguous 1D base array does not
    # create any unnecessary copies.
    base = np.arange(6)
    base_address = base.__array_interface__["data"][0]
    arr = np.array([base[:2], base[2:]], dtype=object)
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    assert ata.storage.field("data").buffers()[3].address == base_address
    out = ata.to_numpy()
    for o, a in zip(out, arr):
        assert o.base.address == base_address
        np.testing.assert_array_equal(o, a)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_tensor_array_slice(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    arr = np.array(arrs, dtype=object)
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    indices = [0, 1, 2]
    for i in indices:
        np.testing.assert_array_equal(ata[i], arr[i])
    slices = [
        slice(0, 1),
        slice(1, 2),
        slice(2, 3),
        slice(0, 2),
        slice(1, 3),
        slice(0, 3),
    ]
    for slice_ in slices:
        ata_slice = ata[slice_]
        ata_slice_np = ata_slice.to_numpy()
        arr_slice = arr[slice_]
        # Check for equivalent dtypes and shapes.
        assert ata_slice_np.dtype == arr_slice.dtype
        assert ata_slice_np.shape == arr_slice.shape
        # Iteration over tensor array slices triggers NumPy conversion.
        for o, e in zip(ata_slice, arr_slice):
            np.testing.assert_array_equal(o, e)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_bool_tensor_array_slice(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.array(
        [
            [True],
            [True, False],
            [False, True, False],
        ],
        dtype=object,
    )
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    indices = [0, 1, 2]
    for i in indices:
        np.testing.assert_array_equal(ata[i], arr[i])

    slices = [
        slice(0, 1),
        slice(1, 2),
        slice(2, 3),
        slice(0, 2),
        slice(1, 3),
        slice(0, 3),
    ]
    for slice_ in slices:
        ata_slice = ata[slice_]
        ata_slice_np = ata_slice.to_numpy()
        arr_slice = arr[slice_]
        # Check for equivalent dtypes and shapes.
        assert ata_slice_np.dtype == arr_slice.dtype
        assert ata_slice_np.shape == arr_slice.shape
        # Iteration over tensor array slices triggers NumPy conversion.
        for o, e in zip(ata_slice, arr_slice):
            np.testing.assert_array_equal(o, e)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_arrow_variable_shaped_string_tensor_array_slice(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.array(
        [
            ["Philip", "J", "Fry"],
            ["Leela", "Turanga"],
            ["Professor", "Hubert", "J", "Farnsworth"],
            ["Lrrr"],
        ],
        dtype=object,
    )
    ata = ArrowVariableShapedTensorArray.from_numpy(arr)
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    indices = [0, 1, 2, 3]
    for i in indices:
        np.testing.assert_array_equal(ata[i], arr[i])
    slices = [
        slice(0, 1),
        slice(1, 2),
        slice(2, 3),
        slice(3, 4),
        slice(0, 2),
        slice(1, 3),
        slice(2, 4),
        slice(0, 3),
        slice(1, 4),
        slice(0, 4),
    ]
    for slice_ in slices:
        ata_slice = ata[slice_]
        ata_slice_np = ata_slice.to_numpy()
        arr_slice = arr[slice_]
        # Check for equivalent dtypes and shapes.
        assert ata_slice_np.dtype == arr_slice.dtype
        assert ata_slice_np.shape == arr_slice.shape
        # Iteration over tensor array slices triggers NumPy conversion.
        for o, e in zip(ata_slice, arr_slice):
            np.testing.assert_array_equal(o, e)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_variable_shaped_tensor_array_roundtrip(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    arr = np.array(arrs, dtype=object)
    ta = TensorArray(arr)
    assert isinstance(ta.dtype, TensorDtype)
    assert len(ta) == len(arr)
    out = ta.to_numpy()
    for o, a in zip(out, arr):
        np.testing.assert_array_equal(o, a)

    # Check Arrow conversion.
    ata = ta.__arrow_array__()
    assert isinstance(ata.type, ArrowVariableShapedTensorType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    for o, a in zip(out, arr):
        np.testing.assert_array_equal(o, a)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_variable_shaped_tensor_array_slice(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    arr = np.array(arrs, dtype=object)
    ta = TensorArray(arr)
    assert isinstance(ta.dtype, TensorDtype)
    assert len(ta) == len(arr)
    indices = [0, 1, 2]
    for i in indices:
        np.testing.assert_array_equal(ta[i], arr[i])
    slices = [
        slice(0, 1),
        slice(1, 2),
        slice(2, 3),
        slice(0, 2),
        slice(1, 3),
        slice(0, 3),
    ]
    for slice_ in slices:
        for o, e in zip(ta[slice_], arr[slice_]):
            np.testing.assert_array_equal(o, e)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_ops(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    df = pd.DataFrame({"one": [1, 2, 3], "two": TensorArray(arr)})

    def apply_arithmetic_ops(arr):
        return 2 * (arr + 1) / 3

    def apply_comparison_ops(arr):
        return arr % 2 == 0

    def apply_logical_ops(arr):
        return arr & (3 * arr) | (5 * arr)

    # Op tests, using NumPy as the groundtruth.
    np.testing.assert_equal(apply_arithmetic_ops(arr), apply_arithmetic_ops(df["two"]))

    np.testing.assert_equal(apply_comparison_ops(arr), apply_comparison_ops(df["two"]))

    np.testing.assert_equal(apply_logical_ops(arr), apply_logical_ops(df["two"]))


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_array_protocol(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = TensorArray(arr)

    np.testing.assert_array_equal(
        np.asarray(t_arr, dtype=np.float32), arr.astype(np.float32)
    )

    t_arr_elem = t_arr[0]

    np.testing.assert_array_equal(
        np.asarray(t_arr_elem, dtype=np.float32), arr[0].astype(np.float32)
    )


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_dataframe_repr(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = TensorArray(arr)
    df = pd.DataFrame({"a": t_arr})

    expected_repr = """                      a
0  [[ 0,  1], [ 2,  3]]
1  [[ 4,  5], [ 6,  7]]
2  [[ 8,  9], [10, 11]]"""
    assert repr(df) == expected_repr


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_scalar_cast(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (1,)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = TensorArray(arr)

    for t_arr_elem, arr_elem in zip(t_arr, arr):
        assert float(t_arr_elem) == float(arr_elem)

    arr = np.arange(1).reshape((1, 1, 1))
    t_arr = TensorArray(arr)
    assert float(t_arr) == float(arr)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_reductions(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})

    # Reduction tests, using NumPy as the groundtruth.
    for name, reducer in TensorArray.SUPPORTED_REDUCERS.items():
        np_kwargs = {}
        if name in ("std", "var"):
            # Pandas uses a ddof default of 1 while NumPy uses 0.
            # Give NumPy a ddof kwarg of 1 in order to ensure equivalent
            # standard deviation calculations.
            np_kwargs["ddof"] = 1
        np.testing.assert_equal(df["two"].agg(name), reducer(arr, axis=0, **np_kwargs))


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize("shape", [(2, 0), (2, 5, 0), (0, 5), (0, 0)])
def test_zero_length_arrow_tensor_array_roundtrip(
    restore_data_context, tensor_format, shape
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    arr = np.empty(shape, dtype=np.int8)
    t_arr = ArrowTensorArray.from_numpy(arr)
    assert len(t_arr) == len(arr)
    out = t_arr.to_numpy()
    np.testing.assert_array_equal(out, arr)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize("chunked", [False, True])
def test_arrow_tensor_array_getitem(chunked, restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = ArrowTensorArray.from_numpy(arr)
    if chunked:
        t_arr = pa.chunked_array(t_arr)

    pyarrow_version = get_pyarrow_version()
    if (
        chunked
        and pyarrow_version >= parse_version("8.0.0")
        and pyarrow_version < parse_version("9.0.0")
    ):
        for idx in range(outer_dim):
            item = t_arr[idx]
            assert isinstance(item, pa.ExtensionScalar)
            item = item.type._extension_scalar_to_ndarray(item)
            np.testing.assert_array_equal(item, arr[idx])
    else:
        for idx in range(outer_dim):
            np.testing.assert_array_equal(t_arr[idx], arr[idx])

    # Test __iter__.
    for t_subarr, subarr in zip(t_arr, arr):
        np.testing.assert_array_equal(t_subarr, subarr)

    # Test to_pylist.
    np.testing.assert_array_equal(t_arr.to_pylist(), list(arr))

    # Test slicing and indexing.
    t_arr2 = t_arr[1:]
    if chunked:
        # For extension arrays, ChunkedArray.to_numpy() concatenates chunk storage
        # arrays and calls to_numpy() on the resulting array, which returns the wrong
        # ndarray.
        # TODO(Clark): Fix this in Arrow by (1) providing an ExtensionArray hook for
        # concatenation, and (2) using that + a to_numpy() call on the resulting
        # ExtensionArray.
        t_arr2_npy = t_arr2.chunk(0).to_numpy()
    else:
        t_arr2_npy = t_arr2.to_numpy()

    np.testing.assert_array_equal(t_arr2_npy, arr[1:])

    if (
        chunked
        and pyarrow_version >= parse_version("8.0.0")
        and pyarrow_version < parse_version("9.0.0")
    ):
        for idx in range(1, outer_dim):
            item = t_arr2[idx - 1]
            assert isinstance(item, pa.ExtensionScalar)
            item = item.type._extension_scalar_to_ndarray(item)
            np.testing.assert_array_equal(item, arr[idx])
    else:
        for idx in range(1, outer_dim):
            np.testing.assert_array_equal(t_arr2[idx - 1], arr[idx])


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize("chunked", [False, True])
def test_arrow_variable_shaped_tensor_array_getitem(
    chunked, restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shapes = [(2, 2), (3, 3), (4, 4)]
    outer_dim = len(shapes)
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    arr = np.array(arrs, dtype=object)
    t_arr = ArrowVariableShapedTensorArray.from_numpy(arr)

    if chunked:
        t_arr = pa.chunked_array(t_arr)

    pyarrow_version = get_pyarrow_version()
    if (
        chunked
        and pyarrow_version >= parse_version("8.0.0")
        and pyarrow_version < parse_version("9.0.0")
    ):
        for idx in range(outer_dim):
            item = t_arr[idx]
            assert isinstance(item, pa.ExtensionScalar)
            item = item.type._extension_scalar_to_ndarray(item)
            np.testing.assert_array_equal(item, arr[idx])
    else:
        for idx in range(outer_dim):
            np.testing.assert_array_equal(t_arr[idx], arr[idx])

    # Test __iter__.
    for t_subarr, subarr in zip(t_arr, arr):
        np.testing.assert_array_equal(t_subarr, subarr)

    # Test to_pylist.
    for t_subarr, subarr in zip(t_arr.to_pylist(), list(arr)):
        np.testing.assert_array_equal(t_subarr, subarr)

    # Test slicing and indexing.
    t_arr2 = t_arr[1:]
    if chunked:
        # For extension arrays, ChunkedArray.to_numpy() concatenates chunk storage
        # arrays and calls to_numpy() on the resulting array, which returns the wrong
        # ndarray.
        # TODO(Clark): Fix this in Arrow by (1) providing an ExtensionArray hook for
        # concatenation, and (2) using that + a to_numpy() call on the resulting
        # ExtensionArray.
        t_arr2_npy = t_arr2.chunk(0).to_numpy()
    else:
        t_arr2_npy = t_arr2.to_numpy()

    for t_subarr, subarr in zip(t_arr2_npy, arr[1:]):
        np.testing.assert_array_equal(t_subarr, subarr)

    if (
        chunked
        and pyarrow_version >= parse_version("8.0.0")
        and pyarrow_version < parse_version("9.0.0")
    ):
        for idx in range(1, outer_dim):
            item = t_arr2[idx - 1]
            assert isinstance(item, pa.ExtensionScalar)
            item = item.type._extension_scalar_to_ndarray(item)
            np.testing.assert_array_equal(item, arr[idx])
    else:
        for idx in range(1, outer_dim):
            np.testing.assert_array_equal(t_arr2[idx - 1], arr[idx])


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize(
    "test_arr,dtype",
    [
        ([[1, 2], [3, 4], [5, 6], [7, 8]], None),
        ([[1, 2], [3, 4], [5, 6], [7, 8]], np.int32),
        ([[1, 2], [3, 4], [5, 6], [7, 8]], np.int16),
        ([[1, 2], [3, 4], [5, 6], [7, 8]], np.longlong),
        ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], None),
        ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], np.float32),
        ([[1.5, 2.5], [3.3, 4.2], [5.2, 6.9], [7.6, 8.1]], np.float16),
        ([[False, True], [True, False], [True, True], [False, False]], None),
    ],
)
def test_arrow_tensor_array_slice(test_arr, dtype, restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    # Test that ArrowTensorArray slicing works as expected.
    arr = np.array(test_arr, dtype=dtype)
    ata = ArrowTensorArray.from_numpy(arr)
    np.testing.assert_array_equal(ata.to_numpy(), arr)
    slice1 = ata.slice(0, 2)
    np.testing.assert_array_equal(slice1.to_numpy(), arr[0:2])
    np.testing.assert_array_equal(slice1[1], arr[1])
    slice2 = ata.slice(2, 2)
    np.testing.assert_array_equal(slice2.to_numpy(), arr[2:4])
    np.testing.assert_array_equal(slice2[1], arr[3])


pytest_tensor_array_concat_shapes = [(1, 2, 2), (3, 2, 2), (2, 3, 3)]
pytest_tensor_array_concat_arrs = [
    np.arange(np.prod(shape)).reshape(shape)
    for shape in pytest_tensor_array_concat_shapes
]
pytest_tensor_array_concat_arrs += [
    create_ragged_ndarray(
        [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))]
    )
]
pytest_tensor_array_concat_arr_combinations = list(
    itertools.combinations(pytest_tensor_array_concat_arrs, 2)
)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize("a1,a2", pytest_tensor_array_concat_arr_combinations)
def test_tensor_array_concat(a1, a2, restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    ta1 = TensorArray(a1)
    ta2 = TensorArray(a2)
    ta = TensorArray._concat_same_type([ta1, ta2])
    assert len(ta) == a1.shape[0] + a2.shape[0]
    assert ta.dtype.element_dtype == ta1.dtype.element_dtype
    if a1.shape[1:] == a2.shape[1:]:
        assert ta.dtype.element_shape == a1.shape[1:]
        np.testing.assert_array_equal(ta.to_numpy(), np.concatenate([a1, a2]))
    else:
        assert ta.dtype.element_shape == (None,) * (len(a1.shape) - 1)
        for arr, expected in zip(
            ta.to_numpy(), np.array([e for a in [a1, a2] for e in a], dtype=object)
        ):
            np.testing.assert_array_equal(arr, expected)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
@pytest.mark.parametrize("a1,a2", pytest_tensor_array_concat_arr_combinations)
def test_arrow_tensor_array_concat(a1, a2, restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    ta1 = ArrowTensorArray.from_numpy(a1)
    ta2 = ArrowTensorArray.from_numpy(a2)
    ta = concat_tensor_arrays([ta1, ta2])
    assert len(ta) == a1.shape[0] + a2.shape[0]
    if a1.shape[1:] == a2.shape[1:]:
        if tensor_format == "v1":
            tensor_type_class = ArrowTensorType
        elif tensor_format == "v2":
            tensor_type_class = ArrowTensorTypeV2
        else:
            raise ValueError(f"unexpected format: {tensor_format}")

        assert isinstance(ta.type, tensor_type_class)
        assert ta.type.storage_type == ta1.type.storage_type
        assert ta.type.storage_type == ta2.type.storage_type
        assert ta.type.shape == a1.shape[1:]
        np.testing.assert_array_equal(ta.to_numpy(), np.concatenate([a1, a2]))
    else:
        assert isinstance(ta.type, ArrowVariableShapedTensorType)
        assert pa.types.is_struct(ta.type.storage_type)
        for arr, expected in zip(
            ta.to_numpy(), np.array([e for a in [a1, a2] for e in a], dtype=object)
        ):
            np.testing.assert_array_equal(arr, expected)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_variable_shaped_tensor_array_chunked_concat(
    restore_data_context, tensor_format
):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    # Test that chunking a tensor column and concatenating its chunks preserves typing
    # and underlying data.
    shape1 = (2, 2, 2)
    shape2 = (3, 4, 4)
    a1 = np.arange(np.prod(shape1)).reshape(shape1)
    a2 = np.arange(np.prod(shape2)).reshape(shape2)
    ta1 = ArrowTensorArray.from_numpy(a1)
    ta2 = ArrowTensorArray.from_numpy(a2)
    unified_arrs = unify_tensor_arrays([ta1, ta2])
    ta = concat_tensor_arrays(unified_arrs)
    assert len(ta) == shape1[0] + shape2[0]
    assert isinstance(ta.type, ArrowVariableShapedTensorType)
    assert pa.types.is_struct(ta.type.storage_type)
    for arr, expected in zip(
        ta.to_numpy(), np.array([e for a in [a1, a2] for e in a], dtype=object)
    ):
        np.testing.assert_array_equal(arr, expected)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_variable_shaped_tensor_array_uniform_dim(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    shape1 = (3, 2, 2)
    shape2 = (3, 4, 4)
    a1 = np.arange(np.prod(shape1)).reshape(shape1)
    a2 = np.arange(np.prod(shape2)).reshape(shape2)
    ta = TensorArray([a1, a2])
    assert len(ta) == 2
    assert ta.is_variable_shaped
    for a, expected in zip(ta.to_numpy(), [a1, a2]):
        np.testing.assert_array_equal(a, expected)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_large_arrow_tensor_array(restore_data_context, tensor_format):
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    test_arr = np.ones((1000, 550), dtype=np.uint8)

    if tensor_format == "v1":
        with pytest.raises(ArrowConversionError) as exc_info:
            ta = ArrowTensorArray.from_numpy([test_arr] * 4000)

        assert (
            repr(exc_info.value.__cause__)
            == "ArrowInvalid('Negative offsets in list array')"
        )
    else:
        ta = ArrowTensorArray.from_numpy([test_arr] * 4000)
        assert len(ta) == 4000
        for arr in ta:
            assert np.asarray(arr).shape == (1000, 550)


@pytest.mark.parametrize("tensor_format", ["v1", "v2"])
def test_tensor_array_string_tensors_simple(restore_data_context, tensor_format):
    """Simple test for fixed-shape string tensor arrays with pandas/arrow roundtrip."""
    DataContext.get_current().use_arrow_tensor_v2 = tensor_format == "v2"

    # Create fixed-shape string tensor
    string_tensors = np.array(
        [["hello", "world"], ["arrow", "pandas"], ["tensor", "string"]]
    )

    # Create pandas DataFrame with TensorArray
    df_pandas = pd.DataFrame({"id": [1, 2, 3], "strings": TensorArray(string_tensors)})
    # Convert to Arrow table
    arrow_table = pa.Table.from_pandas(df_pandas)

    # Convert back to pandas. Beginning v19+ pyarrow will handle
    # extension types correctly
    ignore_metadata = get_pyarrow_version() < parse_version("19.0.0")
    df_roundtrip = arrow_table.to_pandas(ignore_metadata=ignore_metadata)

    # Verify the roundtrip preserves the data
    original_strings = df_pandas["strings"].to_numpy()
    roundtrip_strings = df_roundtrip["strings"].to_numpy()

    np.testing.assert_array_equal(original_strings, roundtrip_strings)
    np.testing.assert_array_equal(roundtrip_strings, string_tensors)


def test_tensor_type_equality_checks():
    # Test that different types are not equal
    fs_tensor_type_v1 = ArrowTensorType((2, 3), pa.int64())
    fs_tensor_type_v2 = ArrowTensorTypeV2((2, 3), pa.int64())

    assert fs_tensor_type_v1 != fs_tensor_type_v2

    # Test different shapes/dtypes aren't equal
    assert fs_tensor_type_v1 != ArrowTensorType((3, 3), pa.int64())
    assert fs_tensor_type_v1 != ArrowTensorType((2, 3), pa.float64())
    assert fs_tensor_type_v2 != ArrowTensorTypeV2((3, 3), pa.int64())
    assert fs_tensor_type_v2 != ArrowTensorTypeV2((2, 3), pa.float64())

    # Test var-shaped tensor type
    vs_tensor_type = ArrowVariableShapedTensorType(pa.int64(), 2)

    # Test that different types are not equal
    assert vs_tensor_type == ArrowVariableShapedTensorType(pa.int64(), 3)
    assert vs_tensor_type != ArrowVariableShapedTensorType(pa.float64(), 2)
    assert vs_tensor_type != fs_tensor_type_v1
    assert vs_tensor_type != fs_tensor_type_v2


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_arrow_fixed_shape_tensor_type_eq_with_concat(restore_data_context):
    """Test that ArrowTensorType and ArrowTensorTypeV2 __eq__ methods work correctly
    when concatenating Arrow arrays with the same tensor type."""
    from ray.data.context import DataContext
    from ray.data.extensions.tensor_extension import (
        ArrowTensorArray,
        ArrowTensorType,
        ArrowTensorTypeV2,
    )

    # Test ArrowTensorType V1
    tensor_type_v1 = ArrowTensorType((2, 3), pa.int64())

    DataContext.get_current().use_arrow_tensor_v2 = False
    first = ArrowTensorArray.from_numpy(np.ones((2, 2, 3), dtype=np.int64))
    second = ArrowTensorArray.from_numpy(np.zeros((3, 2, 3), dtype=np.int64))

    assert first.type == second.type
    # Assert commutation
    assert tensor_type_v1 == first.type
    assert first.type == tensor_type_v1

    # Test concatenation works appropriately
    concatenated = pa.concat_arrays([first, second])
    assert len(concatenated) == 5
    assert concatenated.type == tensor_type_v1

    expected = np.vstack([first.to_numpy(), second.to_numpy()])
    np.testing.assert_array_equal(concatenated.to_numpy(), expected)

    # Test ArrowTensorTypeV2
    tensor_type_v2 = ArrowTensorTypeV2((2, 3), pa.int64())

    DataContext.get_current().use_arrow_tensor_v2 = True

    first = ArrowTensorArray.from_numpy(np.ones((2, 2, 3), dtype=np.int64))
    second = ArrowTensorArray.from_numpy(np.ones((3, 2, 3), dtype=np.int64))

    assert first.type == second.type
    # Assert commutation
    assert tensor_type_v2 == first.type
    assert first.type == tensor_type_v2

    # Test concatenation works appropriately
    concatenated_v2 = pa.concat_arrays([first, second])
    assert len(concatenated_v2) == 5
    assert concatenated_v2.type == tensor_type_v2

    # Assert on the full concatenated array
    expected = np.vstack([first.to_numpy(), second.to_numpy()])
    np.testing.assert_array_equal(concatenated_v2.to_numpy(), expected)


@pytest.mark.skipif(
    not _extension_array_concat_supported(),
    reason="ExtensionArrays support concatenation only in Pyarrow >= 12.0",
)
def test_arrow_variable_shaped_tensor_type_eq_with_concat():
    """Test that ArrowVariableShapedTensorType __eq__ method works correctly
    when concatenating Arrow arrays with variable shaped tensors."""
    from ray.data.extensions.tensor_extension import (
        ArrowVariableShapedTensorArray,
    )

    #
    # Case 1: Tensors are variable-shaped but same ``ndim``
    #

    # Create arrays with variable-shaped tensors (but same ndim)
    first_tensors = [
        # (2, 2)
        np.array([[1, 2], [3, 4]]),
        # (2, 3)
        np.array([[5, 6, 7], [8, 9, 10]]),
    ]
    second_tensors = [
        # (1, 4)
        np.array([[11, 12, 13, 14]]),
        # (3, 1)
        np.array([[15], [16], [17]]),
    ]

    first_arr = ArrowVariableShapedTensorArray.from_numpy(first_tensors)
    second_arr = ArrowVariableShapedTensorArray.from_numpy(second_tensors)

    # Assert commutation
    assert first_arr.type == second_arr.type
    assert second_arr.type == first_arr.type
    # Assert hashing is correct
    assert hash(first_arr.type) == hash(second_arr.type)

    assert first_arr.type.ndim == 2
    assert second_arr.type.ndim == 2

    # Test concatenation works appropriately
    concatenated = pa.concat_arrays([first_arr, second_arr])
    assert len(concatenated) == 4
    assert concatenated.type == first_arr.type

    result_ndarray = concatenated.to_numpy()

    for i, expected_ndarray in enumerate(
        itertools.chain.from_iterable([first_tensors, second_tensors])
    ):
        assert result_ndarray[i].shape == expected_ndarray.shape

        np.testing.assert_array_equal(result_ndarray[i], expected_ndarray)

    #
    # Case 2: Tensors are variable-shaped, with diverging ``ndim``s
    #

    # Create arrays with variable-shaped tensors (but different ndim)
    first_tensors = [
        # (1, 2, 1)
        np.array([[[1], [2]], [[3], [4]]]),
        # (2, 3, 1)
        np.array([[[5], [6], [7]], [[8], [9], [10]]]),
    ]
    second_tensors = [
        # (1, 4)
        np.array([[11, 12, 13, 14]]),
        # (3, 1)
        np.array([[15], [16], [17]]),
    ]

    first_arr = ArrowVariableShapedTensorArray.from_numpy(first_tensors)
    second_arr = ArrowVariableShapedTensorArray.from_numpy(second_tensors)

    # Assert commutation
    assert first_arr.type == second_arr.type
    assert second_arr.type == first_arr.type
    # Assert hashing is correct
    assert hash(first_arr.type) == hash(second_arr.type)

    assert first_arr.type.ndim == 3
    assert second_arr.type.ndim == 2

    # Test concatenation works appropriately
    concatenated = pa.concat_arrays([first_arr, second_arr])

    assert len(concatenated) == 4
    assert concatenated.type == first_arr.type

    result_ndarray = concatenated.to_numpy()

    for i, expected_ndarray in enumerate(
        itertools.chain.from_iterable([first_tensors, second_tensors])
    ):
        assert result_ndarray[i].shape == expected_ndarray.shape

        np.testing.assert_array_equal(result_ndarray[i], expected_ndarray)


def test_reverse_order():
    """Test views in reverse order."""
    base = np.arange(100, dtype=np.float64)

    raveled = np.empty(3, dtype=np.object_)
    raveled[0] = base[50:60].ravel()
    raveled[1] = base[30:50].ravel()
    raveled[2] = base[0:30].ravel()

    # Reverse order views should NOT be contiguous
    assert not _are_contiguous_1d_views(raveled)


def test_concat_ndarrays_zero_copy():
    """Test that _concat_ndarrays performs zero-copy concatenation when possible."""
    # Case 1: Create a base array and contiguous views
    base = np.arange(100, dtype=np.int64)

    arrs = [base[0:20], base[20:50], base[50:100]]

    result = _concat_ndarrays(arrs)

    np.testing.assert_array_equal(result, base)
    # Verify it's a zero-copy view (shares memory with base)
    assert np.shares_memory(result, base)

    # Case 2: Verify empty views are skipped
    arrs = [base[0:10], base[10:10], base[10:20]]  # Empty array

    result = _concat_ndarrays(arrs)
    expected = np.concatenate([base[0:10], base[10:20]])

    np.testing.assert_array_equal(result, expected)
    # Verify it's a zero-copy view (shares memory with base)
    assert np.shares_memory(result, base)

    # Case 3: Singleton ndarray is returned as is
    result = _concat_ndarrays([base])

    # Should return the same array or equivalent
    assert result is base


def test_concat_ndarrays_non_contiguous_fallback():
    """Test that _concat_ndarrays falls back to np.concatenate when arrays aren't contiguous."""

    # Case 1: Non-contiguous arrays
    arr1 = np.arange(10, dtype=np.float32)
    _ = np.arange(1000)  # Create gap to prevent contiguity
    arr2 = np.arange(10, 20, dtype=np.float32)
    _ = np.arange(1000)  # Create gap to prevent contiguity
    arr3 = np.arange(20, 30, dtype=np.float32)

    arrs = [arr1, arr2, arr3]

    result = _concat_ndarrays(arrs)

    expected = np.concatenate(arrs)
    np.testing.assert_array_equal(result, expected)

    assert all(not np.shares_memory(result, a) for a in arrs)

    # Case 2: Non-contiguous arrays (take 2)
    base = np.arange(100, dtype=np.float64)

    arrs = [base[0:10], base[20:30], base[30:40]]  # Gap from 10-20

    result = _concat_ndarrays(arrs)
    expected = np.concatenate(arrs)

    np.testing.assert_array_equal(result, expected)
    # Should have created a copy since there's a gap
    assert not np.shares_memory(result, base)


def test_concat_ndarrays_diff_dtypes_fallback():
    """Different dtypes"""

    base_int16 = np.arange(50, dtype=np.int16)
    base_int32 = np.arange(50, dtype=np.int32)

    # Different dtypes should use fallback
    arrs = [base_int16, base_int32]

    # This should use np.concatenate with type promotion
    result = _concat_ndarrays(arrs)
    expected = np.concatenate(arrs)

    np.testing.assert_array_equal(result, expected)
    assert result.dtype == expected.dtype


def test_are_contiguous_1d_views_non_raveled():
    """Test that _are_contiguous_1d_views rejects non-1D arrays."""
    base = np.arange(100, dtype=np.int64).reshape(10, 10)

    arrs = [
        base[0:2].ravel(),  # 1D view
        base[2:4],  # 2D array
    ]

    # Should reject because second array is not 1D
    assert not _are_contiguous_1d_views(arrs)


def test_are_contiguous_1d_views_non_c_contiguous():
    """Test _are_contiguous_1d_views with non-C-contiguous arrays."""
    base = np.arange(100, dtype=np.int64).reshape(10, 10)

    # Column slices are not C-contiguous
    arrs = [base[:, 0], base[:, 1]]

    assert not _are_contiguous_1d_views(arrs)


def test_are_contiguous_1d_views_different_bases():
    """Test _are_contiguous_1d_views with views from different base arrays."""
    base1 = np.arange(50, dtype=np.int64)
    _ = np.arange(1000, dtype=np.int64)  # Create gap to prevent contiguity
    base2 = np.arange(50, 100, dtype=np.int64)

    arrs = [base1, base2]

    # Different base arrays
    assert not _are_contiguous_1d_views(arrs)


def test_are_contiguous_1d_views_overlapping():
    """Test _are_contiguous_1d_views with overlapping views."""
    base = np.arange(100, dtype=np.float64)

    arrs = [base[0:20], base[10:30]]  # Overlaps with first

    # Overlapping views are not contiguous
    assert not _are_contiguous_1d_views(arrs)


def test_concat_ndarrays_complex_views():
    """Test _concat_ndarrays with complex view scenarios."""
    # Create a 2D array and take contiguous row views
    base_2d = np.arange(100, dtype=np.int64).reshape(10, 10)
    base = base_2d.ravel()  # Get 1D view

    # Take contiguous slices of the 1D view
    arrs = [base[0:30], base[30:60], base[60:100]]

    result = _concat_ndarrays(arrs)
    np.testing.assert_array_equal(result, base)
    assert np.shares_memory(
        result, base_2d
    )  # Should share memory with original 2D array


def test_concat_ndarrays_strided_views():
    """Test _concat_ndarrays with strided (non-contiguous) views."""
    base = np.arange(100, dtype=np.float64)

    # Every other element - these are strided views
    arrs = [base[::2], base[1::2]]  # Even indices  # Odd indices

    # Strided views are not C-contiguous
    result = _concat_ndarrays(arrs)
    expected = np.concatenate(arrs)

    np.testing.assert_array_equal(result, expected)
    # Should have created a copy
    assert not np.shares_memory(result, base)


def test_arrow_extension_serialize_deserialize_cache():
    """Test caching behavior of ArrowExtensionSerializeDeserializeCache."""
    # Test 1: Serialization cache is instance-level
    # Create a fresh test instance
    tensor_type = ArrowTensorType(shape=(2, 3), dtype=pa.int64())

    # Clear the instance's serialization cache to ensure fresh test
    tensor_type._serialize_cache = None

    # Track calls to _arrow_ext_serialize_compute to verify caching
    with patch.object(
        tensor_type,
        "_arrow_ext_serialize_compute",
        wraps=tensor_type._arrow_ext_serialize_compute,
    ) as mock_serialize:
        # First serialization should call compute function
        serialized1 = tensor_type.__arrow_ext_serialize__()
        assert mock_serialize.call_count == 1
        assert serialized1 is not None
        assert isinstance(serialized1, bytes)

        # Second serialization should use cache (no additional call)
        serialized2 = tensor_type.__arrow_ext_serialize__()
        assert mock_serialize.call_count == 1  # Still 1, proving cache hit
        assert serialized1 == serialized2

    # Test 2: Deserialization cache is class-level (shared across instances)
    # Clear the lru_cache to ensure fresh test
    ArrowTensorType._arrow_ext_deserialize_cache.cache_clear()
    storage_type = pa.list_(pa.int64())

    # Track calls to _arrow_ext_deserialize_compute to verify caching
    with patch.object(
        ArrowTensorType,
        "_arrow_ext_deserialize_compute",
        wraps=ArrowTensorType._arrow_ext_deserialize_compute,
    ) as mock_deserialize:
        # First deserialization should call compute function
        deserialized1 = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type, serialized1
        )
        assert mock_deserialize.call_count == 1
        assert deserialized1.shape == (2, 3)
        assert deserialized1.scalar_type == pa.int64()

        # Second deserialization with same key should use cache (no additional call)
        deserialized2 = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type, serialized1
        )
        assert mock_deserialize.call_count == 1  # Still 1, proving cache hit
        assert deserialized1.shape == deserialized2.shape
        assert deserialized1.scalar_type == deserialized2.scalar_type
        assert deserialized1.extension_name == deserialized2.extension_name

    # Test 3: Different serialized data produces different cache entries
    tensor_type2 = ArrowTensorType(shape=(3, 4), dtype=pa.int32())
    tensor_type2._serialize_cache = None
    different_serialized = tensor_type2.__arrow_ext_serialize__()
    storage_type2 = pa.list_(pa.int32())

    deserialized3 = ArrowTensorType.__arrow_ext_deserialize__(
        storage_type2, different_serialized
    )
    # Should be different from previous deserialization
    assert deserialized3.shape == (3, 4)
    assert deserialized3.scalar_type == pa.int32()
    assert deserialized3.shape != deserialized1.shape

    # Test 4: Cache parameter generation works correctly
    param1 = ArrowTensorType._get_deserialize_parameter(storage_type, serialized1)
    param2 = ArrowTensorType._get_deserialize_parameter(storage_type, serialized1)
    assert param1 == param2  # Same inputs should produce same parameters

    param3 = ArrowTensorType._get_deserialize_parameter(
        storage_type2, different_serialized
    )
    assert param1 != param3  # Different inputs should produce different parameters

    # Test 5: Multiple instances have separate serialization caches
    tensor_type_a = ArrowTensorType(shape=(2, 3), dtype=pa.int64())
    tensor_type_b = ArrowTensorType(shape=(2, 3), dtype=pa.int64())

    # Clear caches
    tensor_type_a._serialize_cache = None
    tensor_type_b._serialize_cache = None

    # Track calls to verify separate caches
    with patch.object(
        tensor_type_a,
        "_arrow_ext_serialize_compute",
        wraps=tensor_type_a._arrow_ext_serialize_compute,
    ) as mock_a, patch.object(
        tensor_type_b,
        "_arrow_ext_serialize_compute",
        wraps=tensor_type_b._arrow_ext_serialize_compute,
    ) as mock_b:
        # Serialize both instances
        serialized_a = tensor_type_a.__arrow_ext_serialize__()
        serialized_b = tensor_type_b.__arrow_ext_serialize__()

        # Each should have been called once (separate caches)
        assert mock_a.call_count == 1
        assert mock_b.call_count == 1
        # Both should produce the same serialized data (same shape and dtype)
        assert serialized_a == serialized_b

        # Second calls should use respective caches (no additional calls)
        assert tensor_type_a.__arrow_ext_serialize__() == serialized_a
        assert tensor_type_b.__arrow_ext_serialize__() == serialized_b
        assert mock_a.call_count == 1  # Cache hit
        assert mock_b.call_count == 1  # Cache hit

    # Test 6: Deserialization cache is shared (class-level)
    # The cache is class-level, so all instances share it
    # Note: deserialized1 and deserialized2 were already created in Test 2,
    # so the cache should already have this entry. Let's verify it's reused.
    with patch.object(
        ArrowTensorType,
        "_arrow_ext_deserialize_compute",
        wraps=ArrowTensorType._arrow_ext_deserialize_compute,
    ) as mock_deserialize_shared:
        # These should use the cache from Test 2 (no new compute calls)
        deserialized_a = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type, serialized1
        )
        deserialized_b = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type, serialized1
        )
        # Should not call compute again (cache hit from Test 2)
        assert mock_deserialize_shared.call_count == 0
        # Both should be equal (cache hit)
        assert deserialized_a.shape == deserialized_b.shape
        assert deserialized_a.scalar_type == deserialized_b.scalar_type
        assert deserialized_a.extension_name == deserialized_b.extension_name


def test_arrow_extension_deserialize_cache_per_class():
    """Test that different classes have separate deserialization caches."""
    # Create instances of different classes with the same shape and dtype
    tensor_type_v1 = ArrowTensorType(shape=(2, 3), dtype=pa.int64())
    tensor_type_v2 = ArrowTensorTypeV2(shape=(2, 3), dtype=pa.int64())

    # Serialize both (they should produce the same serialized data since shape is the same)
    serialized_v1 = tensor_type_v1.__arrow_ext_serialize__()
    serialized_v2 = tensor_type_v2.__arrow_ext_serialize__()
    # They should have the same serialized representation (same shape)
    assert serialized_v1 == serialized_v2

    # Clear both caches to ensure fresh test
    ArrowTensorType._arrow_ext_deserialize_cache.cache_clear()
    ArrowTensorTypeV2._arrow_ext_deserialize_cache.cache_clear()

    # Get storage types for each class
    storage_type_v1 = pa.list_(pa.int64())  # ArrowTensorType uses list_
    storage_type_v2 = pa.large_list(pa.int64())  # ArrowTensorTypeV2 uses large_list

    # Track calls to verify each class has its own cache
    with patch.object(
        ArrowTensorType,
        "_arrow_ext_deserialize_compute",
        wraps=ArrowTensorType._arrow_ext_deserialize_compute,
    ) as mock_v1, patch.object(
        ArrowTensorTypeV2,
        "_arrow_ext_deserialize_compute",
        wraps=ArrowTensorTypeV2._arrow_ext_deserialize_compute,
    ) as mock_v2:
        # Deserialize using ArrowTensorType
        deserialized_v1_1 = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type_v1, serialized_v1
        )
        assert mock_v1.call_count == 1
        assert mock_v2.call_count == 0  # V2 cache not affected

        # Deserialize using ArrowTensorTypeV2 with compatible parameters
        # Note: We use the same serialized data but different storage type
        deserialized_v2_1 = ArrowTensorTypeV2.__arrow_ext_deserialize__(
            storage_type_v2, serialized_v2
        )
        assert mock_v1.call_count == 1  # V1 cache not affected
        assert mock_v2.call_count == 1

        # Verify they are different instances (different classes)
        assert type(deserialized_v1_1) is not type(deserialized_v2_1)
        assert isinstance(deserialized_v1_1, ArrowTensorType)
        assert isinstance(deserialized_v2_1, ArrowTensorTypeV2)
        assert not isinstance(deserialized_v1_1, ArrowTensorTypeV2)
        assert not isinstance(deserialized_v2_1, ArrowTensorType)

        # Verify they have the same shape and dtype (same logical content)
        assert deserialized_v1_1.shape == deserialized_v2_1.shape
        assert deserialized_v1_1.scalar_type == deserialized_v2_1.scalar_type

        # But different extension names (different classes)
        assert deserialized_v1_1.extension_name != deserialized_v2_1.extension_name
        assert deserialized_v1_1.extension_name == "ray.data.arrow_tensor"
        assert deserialized_v2_1.extension_name == "ray.data.arrow_tensor_v2"

        # Verify each class uses its own cache (second calls should hit cache)
        deserialized_v1_2 = ArrowTensorType.__arrow_ext_deserialize__(
            storage_type_v1, serialized_v1
        )
        deserialized_v2_2 = ArrowTensorTypeV2.__arrow_ext_deserialize__(
            storage_type_v2, serialized_v2
        )

        # Both should use cache (no additional compute calls)
        assert mock_v1.call_count == 1  # Cache hit for V1
        assert mock_v2.call_count == 1  # Cache hit for V2

        # Verify cache returns same instances for same class
        assert deserialized_v1_1 is deserialized_v1_2  # Same instance from V1 cache
        assert deserialized_v2_1 is deserialized_v2_2  # Same instance from V2 cache

        # But instances from different classes are different
        assert deserialized_v1_1 is not deserialized_v2_1
        assert deserialized_v1_2 is not deserialized_v2_2


def test_arrow_extension_serialize_deserialize_cache_thread_safety():
    """Test that ArrowExtensionSerializeDeserializeCache is thread-safe."""
    tensor_type = ArrowTensorType(shape=(2, 3), dtype=pa.int64())
    storage_type = pa.list_(pa.int64())
    serialized = tensor_type.__arrow_ext_serialize__()

    results = []
    errors = []

    def deserialize_worker():
        try:
            result = ArrowTensorType.__arrow_ext_deserialize__(storage_type, serialized)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # Create multiple threads that deserialize simultaneously
    threads = [threading.Thread(target=deserialize_worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Should have no errors
    assert len(errors) == 0, f"Errors occurred: {errors}"

    # All results should be equal (same deserialized type)
    assert len(results) == 10
    for result in results[1:]:
        assert result.shape == results[0].shape
        assert result.scalar_type == results[0].scalar_type


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
