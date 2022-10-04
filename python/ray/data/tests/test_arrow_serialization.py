import os

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

import ray
import ray.cloudpickle as pickle
from ray.tests.conftest import *  # noqa
from ray.data._internal.arrow_serialization import _get_arrow_array_types
from ray.data.extensions.tensor_extension import (
    ArrowTensorArray,
    ArrowVariableShapedTensorArray,
)


@pytest.mark.parametrize(
    "arr,cap_mult",
    [
        # Null array
        (pa.array([]), 1.0),
        # Int array
        (pa.array(list(range(1000))), 0.1),
        # Array with nulls
        (pa.array((list(range(9)) + [None]) * 100), 0.1),
        # Float array
        (pa.array([float(i) for i in range(1000)]), 0.1),
        # Boolean array
        # Due to bit-packing, most of the pickle bytes are metadata.
        (pa.array([True, False] * 500), 0.8),
        # String array
        (pa.array(["foo", "bar", "bz", None, "quux"] * 200), 0.1),
        # Binary array
        (pa.array([b"foo", b"bar", b"bz", None, b"quux"] * 200), 0.1),
        # List array with nulls
        (pa.array(([None] + [list(range(9)) + [None]] * 9) * 100), 0.1),
        # Large list array with nulls
        (
            pa.array(
                ([None] + [list(range(9)) + [None]] * 9) * 100,
                type=pa.large_list(pa.int64()),
            ),
            0.1,
        ),
        # Fixed size list array
        (
            pa.FixedSizeListArray.from_arrays(
                pa.array(([None] + [list(range(9)) + [None]] * 9) * 100), 1000
            ),
            0.1,
        ),
        # Map array
        (
            pa.array(
                [
                    [(key, item) for key, item in zip("abcdefghij", range(10))]
                    for _ in range(1000)
                ],
                type=pa.map_(pa.string(), pa.int64()),
            ),
            0.1,
        ),
        # Struct array
        (pa.array({"a": i} for i in range(1000)), 0.1),
        # Union array (sparse)
        (
            pa.UnionArray.from_sparse(
                pa.array([0, 1] * 500, type=pa.int8()),
                [pa.array(list(range(1000))), pa.array([True, False] * 500)],
            ),
            0.1,
        ),
        # TODO(Clark): Support dense union arrays.
        # # Union array (dense)
        # (
        #     pa.UnionArray.from_dense(
        #         pa.array([0, 1] * 50, type=pa.int8()),
        #         pa.array([
        #             i if i % 2 == 0 else (i % 3) % 2
        #             for i in range(100)
        #         ], type=pa.int32()),
        #         [pa.array(list(range(100))), pa.array([True, False])],
        #     ),
        #     0.5,
        # ),
        # Dictionary array
        (
            pa.DictionaryArray.from_arrays(
                pa.array((list(range(9)) + [None]) * 100),
                pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i"]),
            ),
            0.1,
        ),
        # Tensor extension array
        (
            ArrowTensorArray.from_numpy(np.arange(1000 * 4 * 4).reshape((1000, 4, 4))),
            0.1,
        ),
        # Boolean tensor extension array
        (
            ArrowTensorArray.from_numpy(
                np.array(
                    [True, False, False, True, False, False, True, True] * 2 * 1000
                ).reshape((1000, 4, 4))
            ),
            0.25,
        ),
        # Variable-shaped tensor extension array
        (
            ArrowVariableShapedTensorArray.from_numpy(
                np.array(
                    [
                        np.arange(4).reshape((2, 2)),
                        np.arange(4, 13).reshape((3, 3)),
                    ]
                    * 500,
                    dtype=object,
                ),
            ),
            0.1,
        ),
        # Boolean variable-shaped tensor extension array
        (
            ArrowVariableShapedTensorArray.from_numpy(
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
            ),
            0.25,
        ),
        # Complex nested array
        (
            pa.UnionArray.from_sparse(
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
                        [
                            [(key, item) for key, item in zip("abcdefghij", range(10))]
                            for _ in range(1000)
                        ],
                        type=pa.map_(pa.string(), pa.int64()),
                    ),
                ],
            ),
            0.1,
        ),
    ],
)
def test_custom_arrow_array_serializer(ray_start_regular, arr, cap_mult):
    # Create a zero-copy slice view of arr.
    view = arr.slice(10, 10)
    s_arr = pickle.dumps(arr)
    s_view = pickle.dumps(view)
    # Check that the slice view is at least cap_mult the size of the full array.
    assert len(s_view) <= cap_mult * len(s_arr)
    # Check for round-trip equality.
    assert view.equals(pickle.loads(s_view)), pickle.loads(s_view)


def test_custom_arrow_array_serializer_parquet_roundtrip(ray_start_regular, tmp_path):
    t = pa.table({"a": list(range(10000000))})
    pq.write_table(t, f"{tmp_path}/test.parquet")
    t2 = pq.read_table(f"{tmp_path}/test.parquet")
    s_t = pickle.dumps(t)
    s_t2 = pickle.dumps(t2)
    # Check that the post-Parquet slice view chunks don't cause a serialization blow-up.
    assert len(s_t2) < 1.1 * len(s_t)
    # Check for round-trip equality.
    assert t2.equals(pickle.loads(s_t2))


def test_custom_arrow_array_serializer_disable(shutdown_only):
    ray.shutdown()
    ray.worker._post_init_hooks = []
    context = ray.worker.global_worker.get_serialization_context()
    for array_type in _get_arrow_array_types():
        context._unregister_cloudpickle_reducer(array_type)
    # Disable custom Arrow array serialization.
    os.environ["RAY_DISABLE_CUSTOM_ARROW_ARRAY_SERIALIZATION"] = "1"
    ray.init()
    # Create a zero-copy slice view of arr.
    arr = pa.array(list(range(100)))
    view = arr.slice(10, 10)
    s_arr = pickle.dumps(arr)
    s_view = pickle.dumps(view)
    # Check that the slice view contains the full buffer of the underlying array.
    d_view = pickle.loads(s_view)
    assert d_view.buffers()[1].size == arr.buffers()[1].size
    # Check that the serialized slice view is large
    assert len(s_view) > 0.8 * len(s_arr)
