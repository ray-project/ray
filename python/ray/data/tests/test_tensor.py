import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import time

import ray
from ray.air.util.tensor_extensions.utils import _create_possibly_ragged_ndarray
from ray.data.block import BlockAccessor
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowTensorType,
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
    TensorArray,
    TensorDtype,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values


# https://github.com/ray-project/ray/issues/33695
def test_large_tensor_creation(ray_start_regular_shared):
    """Tests that large tensor read task creation can complete successfully without
    hanging."""
    start_time = time.time()
    ray.data.range_tensor(1000, parallelism=1000, shape=(80, 80, 100, 100))
    end_time = time.time()

    # Should not take more than 20 seconds.
    assert end_time - start_time < 20


def test_tensors_basic(ray_start_regular_shared):
    # Create directly.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape, parallelism=6)
    assert str(ds) == (
        "Datastream(\n"
        "   num_blocks=6,\n"
        "   num_rows=6,\n"
        "   schema={data: numpy.ndarray(shape=(3, 5), dtype=int64)}\n"
        ")"
    )
    assert ds.size_bytes() == 5 * 3 * 6 * 8

    # Test row iterator yields tensors.
    for tensor in ds.iter_rows():
        tensor = tensor["data"]
        assert isinstance(tensor, np.ndarray)
        assert tensor.shape == tensor_shape

    # Test batch iterator yields tensors.
    for tensor in ds.iter_batches(batch_size=2):
        tensor = tensor["data"]
        assert isinstance(tensor, np.ndarray)
        assert tensor.shape == (2,) + tensor_shape

    # Native format.
    def np_mapper(arr):
        if "data" in arr:
            arr = arr["data"]
        else:
            arr = arr["id"]
        assert isinstance(arr, np.ndarray)
        return {"data": arr + 1}

    res = ray.data.range_tensor(2, shape=(2, 2)).map(np_mapper).take()
    np.testing.assert_equal(
        extract_values("data", res), [np.ones((2, 2)), 2 * np.ones((2, 2))]
    )

    # Explicit NumPy format.
    res = (
        ray.data.range_tensor(2, shape=(2, 2))
        .map_batches(np_mapper, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        extract_values("data", res), [np.ones((2, 2)), 2 * np.ones((2, 2))]
    )

    # Pandas conversion.
    def pd_mapper(df):
        assert isinstance(df, pd.DataFrame)
        return df + 2

    res = ray.data.range_tensor(2).map_batches(pd_mapper, batch_format="pandas").take()
    np.testing.assert_equal(extract_values("data", res), [np.array([2]), np.array([3])])

    # Arrow columns in NumPy format.
    def multi_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["a", "b", "c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return {"a": col_arrs["a"] + 1, "b": col_arrs["b"] + 1, "c": col_arrs["c"] + 1}

    # Multiple columns.
    t = pa.table(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": ArrowTensorArray.from_numpy(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_arrow(t)
        .map_batches(multi_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        res,
        [
            {"a": 2, "b": 5.0, "c": np.array([2, 3])},
            {"a": 3, "b": 6.0, "c": np.array([4, 5])},
            {"a": 4, "b": 7.0, "c": np.array([6, 7])},
        ],
    )

    def single_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return {"c": col_arrs["c"] + 1}

    # Single column (should still yield ndarray dict batches).
    t = t.select(["c"])
    res = (
        ray.data.from_arrow(t)
        .map_batches(single_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        res,
        [
            {"c": np.array([2, 3])},
            {"c": np.array([4, 5])},
            {"c": np.array([6, 7])},
        ],
    )

    # Pandas columns in NumPy format.
    def multi_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["a", "b", "c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return pd.DataFrame(
            {
                "a": col_arrs["a"] + 1,
                "b": col_arrs["b"] + 1,
                "c": TensorArray(col_arrs["c"] + 1),
            }
        )

    # Multiple columns.
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": TensorArray(np.array([[1, 2], [3, 4], [5, 6]])),
        }
    )
    res = (
        ray.data.from_pandas(df)
        .map_batches(multi_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        res,
        [
            {"a": 2, "b": 5.0, "c": np.array([2, 3])},
            {"a": 3, "b": 6.0, "c": np.array([4, 5])},
            {"a": 4, "b": 7.0, "c": np.array([6, 7])},
        ],
    )

    # Single column (should still yield ndarray dict batches).
    def single_mapper(col_arrs):
        assert isinstance(col_arrs, dict)
        assert list(col_arrs.keys()) == ["c"]
        assert all(isinstance(col_arr, np.ndarray) for col_arr in col_arrs.values())
        return pd.DataFrame({"c": TensorArray(col_arrs["c"] + 1)})

    df = df[["c"]]
    res = (
        ray.data.from_pandas(df)
        .map_batches(single_mapper, batch_size=2, batch_format="numpy")
        .take()
    )
    np.testing.assert_equal(
        res,
        [
            {"c": np.array([2, 3])},
            {"c": np.array([4, 5])},
            {"c": np.array([6, 7])},
        ],
    )

    # Simple dataset in NumPy format.
    def mapper(arr):
        arr = np_mapper(arr)
        return arr

    res = (
        ray.data.range(10, parallelism=2)
        .map_batches(mapper, batch_format="numpy")
        .take()
    )
    assert extract_values("data", res) == list(range(1, 11))


def test_batch_tensors(ray_start_regular_shared):
    import torch

    ds = ray.data.from_items([torch.tensor([0, 0]) for _ in range(40)], parallelism=40)
    res = "MaterializedDatastream(num_blocks=40, num_rows=40, schema={item: object})"
    assert str(ds) == res, str(ds)
    with pytest.raises(pa.lib.ArrowInvalid):
        next(ds.iter_batches(batch_format="pyarrow"))
    df = next(ds.iter_batches(batch_format="pandas"))
    assert df.to_dict().keys() == {"item"}


def test_tensors_shuffle(ray_start_regular_shared):
    # Test Arrow table representation.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape)
    shuffled_ds = ds.random_shuffle()
    shuffled = extract_values("data", shuffled_ds.take())
    base = extract_values("data", ds.take())
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        shuffled,
        base,
    )
    np.testing.assert_equal(
        sorted(shuffled, key=lambda arr: arr.min()),
        sorted(base, key=lambda arr: arr.min()),
    )

    # Test Pandas table representation.
    tensor_shape = (3, 5)
    ds = ray.data.range_tensor(6, shape=tensor_shape)
    ds = ds.map_batches(lambda df: df, batch_format="pandas")
    shuffled_ds = ds.random_shuffle()
    shuffled = extract_values("data", shuffled_ds.take())
    base = extract_values("data", ds.take())
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        shuffled,
        base,
    )
    np.testing.assert_equal(
        sorted(shuffled, key=lambda arr: arr.min()),
        sorted(base, key=lambda arr: arr.min()),
    )


def test_tensors_sort(ray_start_regular_shared):
    # Test Arrow table representation.
    t = pa.table({"a": TensorArray(np.arange(32).reshape((2, 4, 4))), "b": [1, 2]})
    ds = ray.data.from_arrow(t)
    sorted_ds = ds.sort(key="b", descending=True)
    sorted_arrs = [row["a"] for row in sorted_ds.take()]
    base = [row["a"] for row in ds.take()]
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        sorted_arrs,
        base,
    )
    np.testing.assert_equal(
        sorted_arrs,
        sorted(base, key=lambda arr: -arr.min()),
    )

    # Test Pandas table representation.
    df = pd.DataFrame({"a": TensorArray(np.arange(32).reshape((2, 4, 4))), "b": [1, 2]})
    ds = ray.data.from_pandas(df)
    sorted_ds = ds.sort(key="b", descending=True)
    sorted_arrs = [np.asarray(row["a"]) for row in sorted_ds.take()]
    base = [np.asarray(row["a"]) for row in ds.take()]
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_equal,
        sorted_arrs,
        base,
    )
    np.testing.assert_equal(
        sorted_arrs,
        sorted(base, key=lambda arr: -arr.min()),
    )


def test_tensors_inferred_from_map(ray_start_regular_shared):
    # Test map.
    ds = ray.data.range(10, parallelism=10).map(lambda _: {"data": np.ones((4, 4))})
    ds = ds.materialize()
    assert str(ds) == (
        "MaterializedDatastream(\n"
        "   num_blocks=10,\n"
        "   num_rows=10,\n"
        "   schema={data: numpy.ndarray(shape=(4, 4), dtype=double)}\n"
        ")"
    )

    # Test map_batches.
    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: {"data": np.ones((3, 4, 4))}, batch_size=2
    )
    ds = ds.materialize()
    assert str(ds) == (
        "MaterializedDatastream(\n"
        "   num_blocks=4,\n"
        "   num_rows=24,\n"
        "   schema={data: numpy.ndarray(shape=(4, 4), dtype=double)}\n"
        ")"
    )

    # Test flat_map.
    ds = ray.data.range(10, parallelism=10).flat_map(
        lambda _: [{"data": np.ones((4, 4))}, {"data": np.ones((4, 4))}]
    )
    ds = ds.materialize()
    assert str(ds) == (
        "MaterializedDatastream(\n"
        "   num_blocks=10,\n"
        "   num_rows=20,\n"
        "   schema={data: numpy.ndarray(shape=(4, 4), dtype=double)}\n"
        ")"
    )

    # Test map_batches ndarray column.
    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: pd.DataFrame({"a": [np.ones((4, 4))] * 3}), batch_size=2
    )
    ds = ds.materialize()
    assert str(ds) == (
        "MaterializedDatastream(\n"
        "   num_blocks=4,\n"
        "   num_rows=24,\n"
        "   schema={a: numpy.ndarray(shape=(4, 4), dtype=float64)}\n"
        ")"
    )

    ds = ray.data.range(16, parallelism=4).map_batches(
        lambda _: pd.DataFrame({"a": [np.ones((2, 2)), np.ones((3, 3))]}),
        batch_size=2,
    )
    ds = ds.materialize()
    assert str(ds) == (
        "MaterializedDatastream(\n"
        "   num_blocks=4,\n"
        "   num_rows=16,\n"
        "   schema={a: numpy.ndarray(shape=(None, None), dtype=float64)}\n"
        ")"
    )


def test_tensor_array_block_slice():
    # Test that ArrowBlock slicing works with tensor column extension type.
    def check_for_copy(table1, table2, a, b, is_copy):
        expected_slice = table1.slice(a, b - a)
        assert table2.equals(expected_slice)
        assert table2.schema == table1.schema
        assert table1.num_columns == table2.num_columns
        for col1, col2 in zip(table1.columns, table2.columns):
            assert col1.num_chunks == col2.num_chunks
            for chunk1, chunk2 in zip(col1.chunks, col2.chunks):
                bufs1 = chunk1.buffers()
                bufs2 = chunk2.buffers()
                expected_offset = 0 if is_copy else a
                assert chunk2.offset == expected_offset
                assert len(chunk2) == b - a
                if is_copy:
                    assert bufs2[1].address != bufs1[1].address
                else:
                    assert bufs2[1].address == bufs1[1].address

    n = 20
    one_arr = np.arange(4 * n).reshape(n, 2, 2)
    df = pd.DataFrame({"one": TensorArray(one_arr), "two": ["a"] * n})
    table = pa.Table.from_pandas(df)
    a, b = 5, 10
    block_accessor = BlockAccessor.for_block(table)

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=True)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    np.testing.assert_array_equal(table2["one"].chunk(0).to_numpy(), one_arr[a:b, :, :])
    check_for_copy(table, table2, a, b, is_copy=False)


@pytest.mark.parametrize(
    "test_data,a,b",
    [
        ([[False, True], [True, False], [True, True], [False, False]], 1, 3),
        ([[False, True], [True, False], [True, True], [False, False]], 0, 1),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            3,
            6,
        ),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            7,
            11,
        ),
        (
            [
                [False, True],
                [True, False],
                [True, True],
                [False, False],
                [True, False],
                [False, False],
                [False, True],
                [True, True],
                [False, False],
                [True, True],
                [False, True],
                [True, False],
            ],
            9,
            12,
        ),
        # Variable-shaped tensors.
        (
            [[False, True], [True, False, True], [False], [False, False, True, True]],
            1,
            3,
        ),
    ],
)
@pytest.mark.parametrize("init_with_pandas", [True, False])
def test_tensor_array_boolean_slice_pandas_roundtrip(init_with_pandas, test_data, a, b):
    is_variable_shaped = len({len(elem) for elem in test_data}) > 1
    n = len(test_data)
    test_arr = _create_possibly_ragged_ndarray(test_data)
    df = pd.DataFrame({"one": TensorArray(test_arr), "two": ["a"] * n})
    if init_with_pandas:
        table = pa.Table.from_pandas(df)
    else:
        if is_variable_shaped:
            col = ArrowVariableShapedTensorArray.from_numpy(test_arr)
        else:
            col = ArrowTensorArray.from_numpy(test_arr)
        table = pa.table({"one": col, "two": ["a"] * n})
    block_accessor = BlockAccessor.for_block(table)

    # Test without copy.
    table2 = block_accessor.slice(a, b, False)
    out = table2["one"].chunk(0).to_numpy()
    expected = test_arr[a:b]
    if is_variable_shaped:
        for o, e in zip(out, expected):
            np.testing.assert_array_equal(o, e)
    else:
        np.testing.assert_array_equal(out, expected)
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )

    # Test with copy.
    table2 = block_accessor.slice(a, b, True)
    out = table2["one"].chunk(0).to_numpy()
    expected = test_arr[a:b]
    if is_variable_shaped:
        for o, e in zip(out, expected):
            np.testing.assert_array_equal(o, e)
    else:
        np.testing.assert_array_equal(out, expected)
    pd.testing.assert_frame_equal(
        table2.to_pandas().reset_index(drop=True), df[a:b].reset_index(drop=True)
    )


def test_tensors_in_tables_from_pandas(ray_start_regular_shared):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": list(arr)})
    # Cast column to tensor extension dtype.
    df["two"] = df["two"].astype(TensorDtype(shape, np.int64))
    ds = ray.data.from_pandas([df])
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_from_pandas_variable_shaped(ray_start_regular_shared):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": arrs})
    # Cast column to tensor extension dtype.
    df["two"] = df["two"].astype(TensorDtype(None, np.int64))
    ds = ray.data.from_pandas(df)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(range(outer_dim), arrs))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_pandas_roundtrip(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2, batch_format="pandas")
    ds_df = ds.to_pandas()
    expected_df = df + 1
    if enable_automatic_tensor_extension_cast:
        expected_df.loc[:, "two"] = list(expected_df["two"].to_numpy())
    pd.testing.assert_frame_equal(ds_df, expected_df)


def test_tensors_in_tables_pandas_roundtrip_variable_shaped(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arrs)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2, batch_format="pandas")
    ds_df = ds.to_pandas()
    expected_df = df + 1
    if enable_automatic_tensor_extension_cast:
        expected_df.loc[:, "two"] = _create_possibly_ragged_ndarray(
            expected_df["two"].to_numpy()
        )
    pd.testing.assert_frame_equal(ds_df, expected_df)


def test_tensors_in_tables_parquet_roundtrip(ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2, batch_format="pandas")
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(1, outer_dim + 1)), arr + 1))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_roundtrip_variable_shaped(
    ray_start_regular_shared, tmp_path
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    outer_dim = len(arrs)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arrs)})
    ds = ray.data.from_pandas(df)
    ds = ds.map_batches(lambda df: df + 1, batch_size=2, batch_format="pandas")
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(1, outer_dim + 1)), [arr + 1 for arr in arrs]))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_with_schema(ray_start_regular_shared, tmp_path):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame({"one": list(range(outer_dim)), "two": TensorArray(arr)})
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema(
        [
            ("one", pa.int32()),
            ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
        ]
    )
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_pickle_manual_serde(
    ray_start_regular_shared, tmp_path
):
    import pickle

    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [pickle.dumps(a) for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))

    # Manually deserialize the tensor pickle bytes and cast to our tensor
    # extension type.
    def deser_mapper(batch: pd.DataFrame):
        batch["two"] = [pickle.loads(a) for a in batch["two"]]
        batch["two"] = batch["two"].astype(TensorDtype(shape, np.int64))
        return batch

    casted_ds = ds.map_batches(deser_mapper, batch_format="pandas")

    values = [[s["one"], s["two"]] for s in casted_ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)

    # Manually deserialize the pickle tensor bytes and directly cast it to a
    # TensorArray.
    def deser_mapper_direct(batch: pd.DataFrame):
        batch["two"] = TensorArray([pickle.loads(a) for a in batch["two"]])
        return batch

    casted_ds = ds.map_batches(deser_mapper_direct, batch_format="pandas")

    values = [[s["one"], s["two"]] for s in casted_ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path))

    tensor_col_name = "two"

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_mapper(batch: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array(
            [
                np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
                for buf in batch.column(tensor_col_name)
            ]
        )

        return batch.set_column(
            batch._ensure_integer_index(tensor_col_name),
            tensor_col_name,
            ArrowTensorArray.from_numpy(np_col),
        )

    ds = ds.map_batches(np_deser_mapper, batch_format="pyarrow")

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_udf(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), tensor_col_name: [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    # Manually deserialize the tensor bytes and cast to a TensorArray.
    def np_deser_udf(block: pa.Table):
        # NOTE(Clark): We use NumPy to consolidate these potentially
        # non-contiguous buffers, and to do buffer bookkeeping in general.
        np_col = np.array(
            [
                np.ndarray(inner_shape, buffer=buf.as_buffer(), dtype=arr.dtype)
                for buf in block.column(tensor_col_name)
            ]
        )

        return block.set_column(
            block._ensure_integer_index(tensor_col_name),
            tensor_col_name,
            ArrowTensorArray.from_numpy(np_col),
        )

    ds = ray.data.read_parquet(str(tmp_path), _block_udf=np_deser_udf)

    assert isinstance(
        ds.schema().base_schema.field_by_name(tensor_col_name).type, ArrowTensorType
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_parquet_bytes_manual_serde_col_schema(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    tensor_col_name = "two"
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), tensor_col_name: [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))

    def _block_udf(block: pa.Table):
        df = block.to_pandas()
        df[tensor_col_name] += 1
        return pa.Table.from_pandas(df)

    ds = ray.data.read_parquet(
        str(tmp_path),
        tensor_column_schema={tensor_col_name: (arr.dtype, inner_shape)},
        _block_udf=_block_udf,
    )

    assert isinstance(
        ds.schema().base_schema.field_by_name(tensor_col_name).type, ArrowTensorType
    )

    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr + 1))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


@pytest.mark.skip(
    reason=(
        "Waiting for Arrow to support registering custom ExtensionType "
        "casting kernels. See "
        "https://issues.apache.org/jira/browse/ARROW-5890#"
    )
)
def test_tensors_in_tables_parquet_bytes_with_schema(
    ray_start_regular_shared, tmp_path
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df = pd.DataFrame(
        {"one": list(range(outer_dim)), "two": [a.tobytes() for a in arr]}
    )
    ds = ray.data.from_pandas([df])
    ds.write_parquet(str(tmp_path))
    schema = pa.schema(
        [
            ("one", pa.int32()),
            ("two", ArrowTensorType(inner_shape, pa.from_numpy_dtype(arr.dtype))),
        ]
    )
    ds = ray.data.read_parquet(str(tmp_path), schema=schema)
    values = [[s["one"], s["two"]] for s in ds.take()]
    expected = list(zip(list(range(outer_dim)), arr))
    for v, e in zip(sorted(values), expected):
        np.testing.assert_equal(v, e)


def test_tensors_in_tables_iter_batches(
    ray_start_regular_shared,
    enable_automatic_tensor_extension_cast,
):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {"one": TensorArray(arr), "two": TensorArray(arr + 1), "label": [1.0, 2.0, 3.0]}
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": TensorArray(arr2 + 1),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2], ignore_index=True)
    if enable_automatic_tensor_extension_cast:
        df.loc[:, "one"] = list(df["one"].to_numpy())
        df.loc[:, "two"] = list(df["two"].to_numpy())
    ds = ray.data.from_pandas([df1, df2])
    batches = list(ds.iter_batches(batch_size=2, batch_format="pandas"))
    assert len(batches) == 3
    expected_batches = [df.iloc[:2], df.iloc[2:4], df.iloc[4:]]
    for batch, expected_batch in zip(batches, expected_batches):
        batch = batch.reset_index(drop=True)
        expected_batch = expected_batch.reset_index(drop=True)
        pd.testing.assert_frame_equal(batch, expected_batch)


def test_ragged_tensors(ray_start_regular_shared):
    """Test Arrow type promotion between ArrowTensorType and
    ArrowVariableShapedTensorType when a column contains ragged tensors."""
    import numpy as np

    ds = ray.data.from_items(
        [
            {"spam": np.zeros((32, 32, 5))},
            {"spam": np.zeros((64, 64, 5))},
        ]
    )
    new_type = ds.schema().types[0].scalar_type
    assert ds.schema().types == [
        ArrowVariableShapedTensorType(dtype=new_type, ndim=3),
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
