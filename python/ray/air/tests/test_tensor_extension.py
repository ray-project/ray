import itertools

import numpy as np
import pandas as pd
from pkg_resources._vendor.packaging.version import parse as parse_version
import pyarrow as pa
import pytest

from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
)
from ray.air.util.tensor_extensions.pandas import TensorArray, TensorDtype
from ray._private.utils import _get_pyarrow_version


def test_tensor_array_validation():
    # Test unknown input type raises TypeError.
    with pytest.raises(TypeError):
        TensorArray(object())

    # Test non-primitive element raises TypeError.
    with pytest.raises(TypeError):
        TensorArray(np.array([object(), object()]))

    with pytest.raises(TypeError):
        TensorArray([object(), object()])


def test_arrow_scalar_tensor_array_roundtrip():
    arr = np.arange(10)
    ata = ArrowTensorArray.from_numpy(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out, arr)


def test_arrow_scalar_tensor_array_roundtrip_boolean():
    arr = np.array([True, False, False, True])
    ata = ArrowTensorArray.from_numpy(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    # Zero-copy is not possible since Arrow bitpacks boolean arrays while NumPy does
    # not.
    out = ata.to_numpy(zero_copy_only=False)
    np.testing.assert_array_equal(out, arr)


def test_arrow_scalar_tensor_array_roundtrip_string():
    arr = np.array([["Phillip", "Turanga", "Hubert"], ["Fry", "Leela", "Farnsworth"]])
    ata = ArrowTensorArray.from_numpy(arr)
    assert isinstance(ata.type, pa.DataType)
    assert len(ata) == len(arr)
    out = ata.to_numpy()
    np.testing.assert_array_equal(out, arr)


def test_scalar_tensor_array_roundtrip():
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


def test_arrow_variable_shaped_tensor_array_validation():
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


def test_arrow_variable_shaped_tensor_array_roundtrip():
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


def test_arrow_variable_shaped_tensor_array_roundtrip_boolean():
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


def test_arrow_variable_shaped_tensor_array_roundtrip_contiguous_optimization():
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


def test_arrow_variable_shaped_tensor_array_slice():
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


def test_variable_shaped_tensor_array_roundtrip():
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


def test_variable_shaped_tensor_array_slice():
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


def test_tensor_array_ops():
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


def test_tensor_array_array_protocol():
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


def test_tensor_array_dataframe_repr():
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


def test_tensor_array_scalar_cast():
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


def test_tensor_array_reductions():
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


@pytest.mark.parametrize("chunked", [False, True])
def test_arrow_tensor_array_getitem(chunked):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = ArrowTensorArray.from_numpy(arr)
    if chunked:
        t_arr = pa.chunked_array(t_arr)

    pyarrow_version = parse_version(_get_pyarrow_version())
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


@pytest.mark.parametrize("chunked", [False, True])
def test_arrow_variable_shaped_tensor_array_getitem(chunked):
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

    pyarrow_version = parse_version(_get_pyarrow_version())
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
def test_arrow_tensor_array_slice(test_arr, dtype):
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
    np.array([np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3))])
]
pytest_tensor_array_concat_arr_combinations = list(
    itertools.combinations(pytest_tensor_array_concat_arrs, 2)
)


@pytest.mark.parametrize("a1,a2", pytest_tensor_array_concat_arr_combinations)
def test_tensor_array_concat(a1, a2):
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


@pytest.mark.parametrize("a1,a2", pytest_tensor_array_concat_arr_combinations)
def test_arrow_tensor_array_concat(a1, a2):
    ta1 = ArrowTensorArray.from_numpy(a1)
    ta2 = ArrowTensorArray.from_numpy(a2)
    ta = ArrowTensorArray._concat_same_type([ta1, ta2])
    assert len(ta) == a1.shape[0] + a2.shape[0]
    if a1.shape[1:] == a2.shape[1:]:
        assert isinstance(ta.type, ArrowTensorType)
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


def test_variable_shaped_tensor_array_chunked_concat():
    # Test that chunking a tensor column and concatenating its chunks preserves typing
    # and underlying data.
    shape1 = (2, 2, 2)
    shape2 = (3, 4, 4)
    a1 = np.arange(np.prod(shape1)).reshape(shape1)
    a2 = np.arange(np.prod(shape2)).reshape(shape2)
    ta1 = ArrowTensorArray.from_numpy(a1)
    ta2 = ArrowTensorArray.from_numpy(a2)
    chunked_ta = ArrowTensorArray._chunk_tensor_arrays([ta1, ta2])
    ta = ArrowTensorArray._concat_same_type(chunked_ta.chunks)
    assert len(ta) == shape1[0] + shape2[0]
    assert isinstance(ta.type, ArrowVariableShapedTensorType)
    assert pa.types.is_struct(ta.type.storage_type)
    for arr, expected in zip(
        ta.to_numpy(), np.array([e for a in [a1, a2] for e in a], dtype=object)
    ):
        np.testing.assert_array_equal(arr, expected)


def test_variable_shaped_tensor_array_uniform_dim():
    shape1 = (3, 2, 2)
    shape2 = (3, 4, 4)
    a1 = np.arange(np.prod(shape1)).reshape(shape1)
    a2 = np.arange(np.prod(shape2)).reshape(shape2)
    ta = TensorArray([a1, a2])
    assert len(ta) == 2
    assert ta.is_variable_shaped
    for a, expected in zip(ta.to_numpy(), [a1, a2]):
        np.testing.assert_array_equal(a, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
