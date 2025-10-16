import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.air.util.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data.context import DataContext
from ray.data.dataset import Schema
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.extensions.tensor_extension import ArrowTensorType
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def _get_tensor_type():
    return (
        ArrowTensorTypeV2
        if DataContext.get_current().use_arrow_tensor_v2
        else ArrowTensorType
    )


@pytest.mark.parametrize("from_ref", [False, True])
def test_from_numpy(ray_start_regular_shared, from_ref):
    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)
    arrs = [arr1, arr2]
    if from_ref:
        ds = ray.data.from_numpy_refs([ray.put(arr) for arr in arrs])
    else:
        ds = ray.data.from_numpy(arrs)
    values = np.stack(extract_values("data", ds.take(8)))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()

    # Test from single NumPy ndarray.
    if from_ref:
        ds = ray.data.from_numpy_refs(ray.put(arr1))
    else:
        ds = ray.data.from_numpy(arr1)
    values = np.stack(extract_values("data", ds.take(4)))
    np.testing.assert_array_equal(values, arr1)
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()


def test_from_numpy_variable_shaped(ray_start_regular_shared):
    arr = np.array([np.ones((2, 2)), np.ones((3, 3))], dtype=object)
    ds = ray.data.from_numpy(arr)
    values = np.array(extract_values("data", ds.take(2)), dtype=object)

    def recursive_to_list(a):
        if not isinstance(a, (list, np.ndarray)):
            return a
        return [recursive_to_list(e) for e in a]

    # Convert to a nested Python list in order to circumvent failed comparisons on
    # ndarray raggedness.
    np.testing.assert_equal(recursive_to_list(values), recursive_to_list(arr))


def test_to_numpy_refs(ray_start_regular_shared):
    # Tensor Dataset
    ds = ray.data.range_tensor(10, override_num_blocks=2)
    arr = np.concatenate(extract_values("data", ray.get(ds.to_numpy_refs())))
    np.testing.assert_equal(arr, np.expand_dims(np.arange(0, 10), 1))

    # Table Dataset
    ds = ray.data.range(10)
    arr = np.concatenate([t["id"] for t in ray.get(ds.to_numpy_refs())])
    np.testing.assert_equal(arr, np.arange(0, 10))

    # Test multi-column Arrow dataset.
    ds = ray.data.from_arrow(pa.table({"a": [1, 2, 3], "b": [4, 5, 6]}))
    arrs = ray.get(ds.to_numpy_refs())
    np.testing.assert_equal(
        arrs, [{"a": np.array([1, 2, 3]), "b": np.array([4, 5, 6])}]
    )

    # Test multi-column Pandas dataset.
    ds = ray.data.from_pandas(pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}))
    arrs = ray.get(ds.to_numpy_refs())
    np.testing.assert_equal(
        arrs, [{"a": np.array([1, 2, 3]), "b": np.array([4, 5, 6])}]
    )


def test_numpy_roundtrip(ray_start_regular_shared, tmp_path):
    tensor_type = _get_tensor_type()

    ds = ray.data.range_tensor(10, override_num_blocks=2)
    ds.write_numpy(tmp_path, column="data")
    ds = ray.data.read_numpy(tmp_path)
    assert ds.count() == 10
    assert ds.schema() == Schema(pa.schema([("data", tensor_type((1,), pa.int64()))]))
    assert sorted(ds.take_all(), key=lambda row: row["data"]) == [
        {"data": np.array([i])} for i in range(10)
    ]


def test_numpy_read_x(ray_start_regular_shared, tmp_path):
    tensor_type = _get_tensor_type()

    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    np.save(os.path.join(path, "test.npy"), np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(path, override_num_blocks=1)
    assert ds.count() == 10
    assert ds.schema() == Schema(pa.schema([("data", tensor_type((1,), pa.int64()))]))
    np.testing.assert_equal(
        extract_values("data", ds.take(2)), [np.array([0]), np.array([1])]
    )

    # Add a file with a non-matching file extension. This file should be ignored.
    with open(os.path.join(path, "foo.txt"), "w") as f:
        f.write("foobar")

    ds = ray.data.read_numpy(path, override_num_blocks=1)
    assert ds._plan.initial_num_blocks() == 1
    assert ds.count() == 10
    assert ds.schema() == Schema(pa.schema([("data", tensor_type((1,), pa.int64()))]))
    assert [v["data"].item() for v in ds.take(2)] == [0, 1]


def test_numpy_read_meta_provider(ray_start_regular_shared, tmp_path):
    tensor_type = _get_tensor_type()

    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    path = os.path.join(path, "test.npy")
    np.save(path, np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(
        path, meta_provider=FastFileMetadataProvider(), override_num_blocks=1
    )
    assert ds.count() == 10
    assert ds.schema() == Schema(pa.schema([("data", tensor_type((1,), pa.int64()))]))
    np.testing.assert_equal(
        extract_values("data", ds.take(2)), [np.array([0]), np.array([1])]
    )

    with pytest.raises(NotImplementedError):
        ray.data.read_binary_files(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_numpy_write(ray_start_regular_shared, tmp_path):
    ds = ray.data.range_tensor(1)

    ds.write_numpy(tmp_path, column="data")

    actual_array = np.concatenate(
        [np.load(os.path.join(tmp_path, filename)) for filename in os.listdir(tmp_path)]
    )
    assert actual_array == np.array((0,))


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_write_min_rows_per_file(tmp_path, ray_start_regular_shared, min_rows_per_file):
    ray.data.range(100, override_num_blocks=20).write_numpy(
        tmp_path, column="id", min_rows_per_file=min_rows_per_file
    )

    for filename in os.listdir(tmp_path):
        array = np.load(os.path.join(tmp_path, filename))
        assert len(array) == min_rows_per_file


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
