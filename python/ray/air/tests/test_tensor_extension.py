import itertools

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from ray.air.util.tensor_extensions.arrow import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
)
from ray.air.util.tensor_extensions.pandas import TensorArray, TensorDtype


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
    # Test homogeneous-typed tensor raises ValueError.
    with pytest.raises(ValueError):
        ArrowVariableShapedTensorArray.from_numpy(np.ones((3, 2, 2)))

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
        for o, e in zip(ata[slice_], arr[slice_]):
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


def test_arrow_tensor_array_getitem():
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)

    t_arr = ArrowTensorArray.from_numpy(arr)

    for idx in range(outer_dim):
        np.testing.assert_array_equal(t_arr[idx], arr[idx])

    # Test __iter__.
    for t_subarr, subarr in zip(t_arr, arr):
        np.testing.assert_array_equal(t_subarr, subarr)

    # Test to_pylist.
    np.testing.assert_array_equal(t_arr.to_pylist(), list(arr))

    # Test slicing and indexing.
    t_arr2 = t_arr[1:]

    np.testing.assert_array_equal(t_arr2.to_numpy(), arr[1:])

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


@pytest.mark.parametrize(
    "shape1,shape2", list(itertools.combinations(pytest_tensor_array_concat_shapes, 2))
)
def test_tensor_array_concat(shape1, shape2):
    a1 = np.arange(np.prod(shape1)).reshape(shape1)
    a2 = np.arange(np.prod(shape2)).reshape(shape2)
    ta1 = TensorArray(a1)
    ta2 = TensorArray(a2)
    ta = TensorArray._concat_same_type([ta1, ta2])
    assert len(ta) == shape1[0] + shape2[0]
    assert ta.dtype.element_dtype == ta1.dtype.element_dtype
    if shape1[1:] == shape2[1:]:
        assert ta.dtype.element_shape == shape1[1:]
        np.testing.assert_array_equal(ta.to_numpy(), np.concatenate([a1, a2]))
    else:
        assert ta.dtype.element_shape is None
        for arr, expected in zip(
            ta.to_numpy(), np.array([e for a in [a1, a2] for e in a], dtype=object)
        ):
            np.testing.assert_array_equal(arr, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
