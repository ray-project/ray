import logging
import os
import types

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
import ray.cloudpickle as pickle
import ray.data
import ray.train
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.extensions.object_extension import (
    ArrowPythonObjectArray,
)
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
)
from ray.tests.conftest import *  # noqa


@pytest.fixture
def null_array():
    return pa.array([])


@pytest.fixture
def int_array():
    return pa.array(list(range(1000)))


@pytest.fixture
def int_array_with_nulls():
    return pa.array((list(range(9)) + [None]) * 100)


@pytest.fixture
def float_array():
    return pa.array([float(i) for i in range(1000)])


@pytest.fixture
def boolean_array():
    return pa.array([True, False] * 500)


@pytest.fixture
def string_array():
    return pa.array(["foo", "bar", "bz", None, "quux"] * 200)


@pytest.fixture
def large_string_array():
    return pa.array(["foo", "bar", "bz", None, "quux"] * 200, type=pa.large_string())


@pytest.fixture
def binary_array():
    return pa.array([b"foo", b"bar", b"bz", None, b"quux"] * 200)


@pytest.fixture
def fixed_size_binary_array():
    return pa.array([b"foo", b"bar", b"baz", None, b"qux"] * 200, type=pa.binary(3))


@pytest.fixture
def large_binary_array():
    return pa.array(
        [b"foo", b"bar", b"bz", None, b"quux"] * 200, type=pa.large_binary()
    )


@pytest.fixture
def list_array():
    return pa.array(([None] + [list(range(9)) + [None]] * 9) * 100)


@pytest.fixture
def large_list_array():
    # Large list array with nulls
    return pa.array(
        ([None] + [list(range(9)) + [None]] * 9) * 100,
        type=pa.large_list(pa.int64()),
    )


@pytest.fixture
def fixed_size_list_array():
    # Fixed size list array
    return pa.FixedSizeListArray.from_arrays(
        pa.array((list(range(9)) + [None]) * 1000), 10
    )


@pytest.fixture
def map_array():
    return pa.array(
        [list(zip("abcdefghij", range(10))) for _ in range(1000)],
        type=pa.map_(pa.string(), pa.int64()),
    )


@pytest.fixture
def struct_array():
    # Struct array
    return pa.array({"a": i} for i in range(1000))


@pytest.fixture
def sparse_union_array():
    return pa.UnionArray.from_sparse(
        pa.array([0, 1] * 500, type=pa.int8()),
        [pa.array(list(range(1000))), pa.array([True, False] * 500)],
    )


@pytest.fixture
def dense_union_array():
    return pa.UnionArray.from_dense(
        pa.array([0, 1] * 500, type=pa.int8()),
        pa.array(
            [i if i % 2 == 0 else (i % 3) % 2 for i in range(1000)], type=pa.int32()
        ),
        [pa.array(list(range(1000))), pa.array([True, False])],
    )


@pytest.fixture
def dictionary_array():
    return pa.DictionaryArray.from_arrays(
        pa.array((list(range(9)) + [None]) * 100),
        pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i"]),
    )


@pytest.fixture
def tensor_array():
    return ArrowTensorArray.from_numpy(np.arange(1000 * 4 * 4).reshape((1000, 4, 4)))


@pytest.fixture
def boolean_tensor_array():
    return ArrowTensorArray.from_numpy(
        np.array(
            [True, False, False, True, False, False, True, True] * 2 * 1000
        ).reshape((1000, 4, 4))
    )


@pytest.fixture
def variable_shaped_tensor_array():
    return ArrowVariableShapedTensorArray.from_numpy(
        np.array(
            [
                np.arange(4).reshape((2, 2)),
                np.arange(4, 13).reshape((3, 3)),
            ]
            * 500,
            dtype=object,
        ),
    )


@pytest.fixture
def boolean_variable_shaped_tensor_array():
    return ArrowVariableShapedTensorArray.from_numpy(
        np.array(
            [
                np.array([[True, False], [False, True]]),
                np.array(
                    [
                        [False, True, False],
                        [True, True, False],
                        [False, False, False],
                    ],
                ),
            ]
            * 500,
            dtype=object,
        )
    )


@pytest.fixture
def list_of_struct_array():
    return pa.array([{"a": i}, {"a": -i}] for i in range(1000))


@pytest.fixture
def list_of_empty_struct_array():
    return pa.array([{}, {}] for i in range(1000))


@pytest.fixture
def complex_nested_array():
    return pa.UnionArray.from_sparse(
        pa.array([0, 1] * 500, type=pa.int8()),
        [
            pa.array(
                [
                    {
                        "a": i % 2 == 0,
                        "b": i,
                        "c": "bar",
                    }
                    for i in range(1000)
                ]
            ),
            pa.array(
                [list(zip("abcdefghij", range(10))) for _ in range(1000)],
                type=pa.map_(pa.string(), pa.int64()),
            ),
        ],
    )


@pytest.fixture
def pickled_objects_array():
    elements = ["test", 20, False, {"some": "value"}, None, np.zeros((10, 10))]
    elements *= 1 + 1000 // len(elements)
    elements = elements[:1000]

    arr = np.array(elements, dtype=object)
    return ArrowPythonObjectArray.from_objects(arr)


pytest_custom_serialization_arrays = [
    # Null array
    (lazy_fixture("null_array"), 1.0),
    # Int array
    (lazy_fixture("int_array"), 0.1),
    # Array with nulls
    (lazy_fixture("int_array_with_nulls"), 0.1),
    # Float array
    (lazy_fixture("float_array"), 0.1),
    # Boolean array
    # Due to bit-packing, most of the pickle bytes are metadata.
    (lazy_fixture("boolean_array"), 0.8),
    # String array
    (lazy_fixture("string_array"), 0.1),
    # Large string array
    (lazy_fixture("large_string_array"), 0.1),
    # Binary array
    (lazy_fixture("binary_array"), 0.1),
    # Fixed size binary array
    (lazy_fixture("fixed_size_binary_array"), 0.1),
    # Large binary array
    (lazy_fixture("large_binary_array"), 0.1),
    # List array with nulls
    (lazy_fixture("list_array"), 0.1),
    # Large list array with nulls
    (lazy_fixture("large_list_array"), 0.1),
    # Fixed size list array
    (lazy_fixture("fixed_size_list_array"), 0.1),
    # Map array
    (lazy_fixture("map_array"), 0.1),
    # Struct array
    (lazy_fixture("struct_array"), 0.1),
    # Union array (sparse)
    (lazy_fixture("sparse_union_array"), 0.1),
    # Union array (dense)
    (lazy_fixture("dense_union_array"), 0.1),
    # Dictionary array
    (lazy_fixture("dictionary_array"), 0.1),
    # Tensor extension array
    (lazy_fixture("tensor_array"), 0.1),
    # Boolean tensor extension array
    (lazy_fixture("boolean_tensor_array"), 0.25),
    # Variable-shaped tensor extension array
    (lazy_fixture("variable_shaped_tensor_array"), 0.1),
    # Boolean variable-shaped tensor extension array
    (lazy_fixture("boolean_variable_shaped_tensor_array"), 0.25),
    # List of struct array
    (lazy_fixture("list_of_struct_array"), 0.1),
    # List of empty struct array
    (lazy_fixture("list_of_empty_struct_array"), 0.1),
    # Complex nested array
    (lazy_fixture("complex_nested_array"), 0.1),
    # Array of pickled objects
    (lazy_fixture("pickled_objects_array"), 0.1),
]


@pytest.mark.parametrize("data,cap_mult", pytest_custom_serialization_arrays)
def test_custom_arrow_data_serializer(ray_start_regular_shared, data, cap_mult):
    if len(data) == 0:
        data = pa.table({"a": []})
    else:
        data = pa.Table.from_arrays(
            [data, data, pa.array(range(1000), type=pa.int32())],
            schema=pa.schema(
                [
                    pa.field("arr1", data.type),
                    pa.field("arr2", data.type),
                    pa.field("arr3", pa.int32()),
                ],
                metadata={b"foo": b"bar"},
            ),
        )
    ray._private.worker.global_worker.get_serialization_context()
    data.validate()
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version >= parse_version("7.0.0"):
        # get_total_buffer_size API was added in Arrow 7.0.0.
        buf_size = data.get_total_buffer_size()
    # Create a zero-copy slice view of data.
    view = data.slice(10, 10)
    s_arr = pickle.dumps(data)
    s_view = pickle.dumps(view)
    post_slice = pickle.loads(s_view)
    post_slice.validate()
    # Check for round-trip equality.
    assert view.equals(post_slice), post_slice
    # Check that the slice view was truncated upon serialization.
    assert len(s_view) <= cap_mult * len(s_arr)
    for column, pre_column in zip(post_slice.columns, view.columns):
        # Check that offset was reset on slice.
        if column.num_chunks > 0:
            assert column.chunk(0).offset == 0
        # Check that null count was either properly cached or recomputed.
        assert column.null_count == pre_column.null_count
    if pyarrow_version >= parse_version("7.0.0"):
        # Check that slice buffer only contains slice data.
        slice_buf_size = post_slice.get_total_buffer_size()
        if buf_size > 0:
            assert buf_size / slice_buf_size - len(data) / len(post_slice) < 100


def test_custom_arrow_data_serializer_fallback(
    ray_start_regular_shared, propagate_logs, caplog
):
    # Reset serialization fallback set so warning is logged.
    import ray._private.arrow_serialization as arrow_ser_module

    arrow_ser_module._serialization_fallback_set = set()

    data = pa.table(
        {
            "a": pa.UnionArray.from_dense(
                pa.array([0, 1] * 500, type=pa.int8()),
                pa.array(
                    [i if i % 2 == 0 else (i % 3) % 2 for i in range(1000)],
                    type=pa.int32(),
                ),
                [pa.array(list(range(1000))), pa.array([True, False])],
            )
        }
    )
    cap_mult = 0.1
    ray._private.worker.global_worker.get_serialization_context()
    data.validate()
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version >= parse_version("7.0.0"):
        # get_total_buffer_size API was added in Arrow 7.0.0.
        buf_size = data.get_total_buffer_size()
    # Create a zero-copy slice view of data.
    view = data.slice(10, 10)
    # Confirm that (1) fallback works, and (2) warning is logged.
    with caplog.at_level(
        logging.WARNING,
        logger="ray.data._internal.arrow_serialization",
    ):
        s_arr = pickle.dumps(data)
    assert "Failed to complete optimized serialization" in caplog.text
    caplog.clear()
    # Confirm that we only warn once per process.
    with caplog.at_level(
        logging.WARNING,
        logger="ray.data._internal.arrow_serialization",
    ):
        s_view = pickle.dumps(view)
    assert "Failed to complete optimized serialization" not in caplog.text
    post_slice = pickle.loads(s_view)
    post_slice.validate()
    # Check for round-trip equality.
    assert view.equals(post_slice), post_slice
    # Check that the slice view was truncated upon serialization.
    assert len(s_view) <= cap_mult * len(s_arr)
    for column, pre_column in zip(post_slice.columns, view.columns):
        # Check that offset was reset on slice.
        if column.num_chunks > 0:
            assert column.chunk(0).offset == 0
        # Check that null count was either properly cached or recomputed.
        assert column.null_count == pre_column.null_count
    if pyarrow_version >= parse_version("7.0.0"):
        # Check that slice buffer only contains slice data.
        slice_buf_size = post_slice.get_total_buffer_size()
        if buf_size > 0:
            assert buf_size / slice_buf_size - len(data) / len(post_slice) < 100


def test_arrow_scalar_conversion(ray_start_regular_shared):
    ds = ray.data.from_items([1])

    def fn(batch: list):
        return {"id": np.array([1])}

    ds = ds.map_batches(fn)
    res = ds.take()
    assert res == [{"id": 1}], res


def test_arrow_object_and_array_support(ray_start_regular_shared):
    obj = types.SimpleNamespace(some_attribute="test")

    def f(batch):
        batch_size = len(batch["id"])
        return {
            "array": np.zeros((batch_size, 32, 32, 3)),
            "unsupported": [obj] * batch_size,
        }

    res = ray.data.range(5).map_batches(f, batch_size=None).take(1)
    assert res[0]["array"].shape == (32, 32, 3)
    assert np.all(res[0]["array"] == 0)
    assert res[0]["unsupported"] == obj


def test_custom_arrow_data_serializer_parquet_roundtrip(
    ray_start_regular_shared, tmp_path
):
    ray._private.worker.global_worker.get_serialization_context()
    t = pa.table({"a": list(range(10000000))})
    pq.write_table(t, f"{tmp_path}/test.parquet")
    t2 = pq.read_table(f"{tmp_path}/test.parquet")
    s_t = pickle.dumps(t)
    s_t2 = pickle.dumps(t2)
    # Check that the post-Parquet slice view chunks don't cause a serialization blow-up.
    assert len(s_t2) < 1.1 * len(s_t)
    # Check for round-trip equality.
    assert t2.equals(pickle.loads(s_t2))


def test_arrow_schema_ipc_serialization(ray_start_regular_shared):
    """Test that Arrow Schema uses IPC serialization for performance."""
    from ray._private.arrow_serialization import (
        _arrow_schema_reduce,
        _restore_schema_from_ipc,
    )

    # Verify the reducer is registered
    ray._private.worker.global_worker.get_serialization_context()
    assert pa.Schema in pickle.CloudPickler.dispatch
    assert pickle.CloudPickler.dispatch[pa.Schema] == _arrow_schema_reduce

    # Create a complex schema with various types
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("metadata", pa.map_(pa.string(), pa.string())),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("x", pa.float64()),
                        pa.field("y", pa.float64()),
                    ]
                ),
            ),
            pa.field("category", pa.dictionary(pa.int8(), pa.string())),
            pa.field("decimal_val", pa.decimal128(18, 6)),
        ],
        metadata={b"foo": b"bar"},
    )

    # Test roundtrip serialization
    serialized = pickle.dumps(schema)
    deserialized = pickle.loads(serialized)
    assert schema.equals(deserialized)
    assert schema.metadata == deserialized.metadata

    # Verify the reducer uses IPC format (check via direct call)
    restore_func, (ipc_bytes,) = _arrow_schema_reduce(schema)
    assert restore_func == _restore_schema_from_ipc
    # IPC bytes should match what schema.serialize() produces
    assert ipc_bytes == schema.serialize().to_pybytes()
    # Verify restore works
    restored = restore_func(ipc_bytes)
    assert schema.equals(restored)


def test_custom_arrow_data_serializer_disable(shutdown_only):
    ray.shutdown()
    ray.worker._post_init_hooks = []
    context = ray.worker.global_worker.get_serialization_context()
    context._unregister_cloudpickle_reducer(pa.Table)
    # Disable custom Arrow array serialization.
    os.environ["RAY_DISABLE_CUSTOM_ARROW_ARRAY_SERIALIZATION"] = "1"
    ray.init()
    # Create a zero-copy slice view of table.
    t = pa.table({"a": list(range(10000000))})
    view = t.slice(10, 10)
    s_t = pickle.dumps(t)
    s_view = pickle.dumps(view)
    # Check that the slice view contains the full buffer of the underlying array.
    d_view = pickle.loads(s_view)
    assert d_view["a"].chunk(0).buffers()[1].size == t["a"].chunk(0).buffers()[1].size
    # Check that the serialized slice view is large
    assert len(s_view) > 0.8 * len(s_t)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
