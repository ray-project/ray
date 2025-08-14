import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.air.util.tensor_extensions.arrow import ArrowTensorTypeV2
from ray.data import DataContext, Schema
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    Partitioning,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.extensions.tensor_extension import ArrowTensorType
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def _get_tensor_type():
    return (
        ArrowTensorTypeV2
        if DataContext.get_current().use_arrow_tensor_v2
        else ArrowTensorType
    )


def test_numpy_read_partitioning(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "country=us", "data.npy")
    os.mkdir(os.path.dirname(path))
    np.save(path, np.arange(4).reshape([2, 2]))

    ds = ray.data.read_numpy(path, partitioning=Partitioning("hive"))

    assert ds.schema().names == ["data", "country"]
    assert [r["country"] for r in ds.take()] == ["us", "us"]


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


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_anonymous_crendential"),
            lazy_fixture("s3_path_with_anonymous_crendential"),
        ),
    ],
)
def test_numpy_roundtrip(ray_start_regular_shared, fs, data_path):
    tensor_type = _get_tensor_type()

    ds = ray.data.range_tensor(10, override_num_blocks=2)
    ds.write_numpy(data_path, filesystem=fs, column="data")
    ds = ray.data.read_numpy(data_path, filesystem=fs)
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


@pytest.mark.parametrize("ignore_missing_paths", [True, False])
def test_numpy_read_ignore_missing_paths(
    ray_start_regular_shared, tmp_path, ignore_missing_paths
):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    np.save(os.path.join(path, "test.npy"), np.expand_dims(np.arange(0, 10), 1))

    paths = [
        os.path.join(path, "test.npy"),
        "missing.npy",
    ]

    if ignore_missing_paths:
        ds = ray.data.read_numpy(paths, ignore_missing_paths=ignore_missing_paths)
        assert ds.input_files() == [paths[0]]
    else:
        with pytest.raises(FileNotFoundError):
            ds = ray.data.read_numpy(paths, ignore_missing_paths=ignore_missing_paths)
            ds.materialize()


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


@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_numpy_read_partitioned_with_filter(
    style,
    ray_start_regular_shared,
    tmp_path,
    write_partitioned_df,
    assert_base_partitioned_ds,
):
    tensor_type = _get_tensor_type()

    def df_to_np(dataframe, path, **kwargs):
        np.save(path, dataframe.to_numpy(dtype=np.dtype(np.int8)), **kwargs)

    df = pd.DataFrame({"one": [1, 1, 1, 3, 3, 3], "two": [0, 1, 2, 3, 4, 5]})
    partition_keys = ["one"]

    def skip_unpartitioned(kv_dict):
        return bool(kv_dict)

    base_dir = os.path.join(tmp_path, style.value)
    partition_path_encoder = PathPartitionEncoder.of(
        style=style,
        base_dir=base_dir,
        field_names=partition_keys,
    )
    write_partitioned_df(
        df,
        partition_keys,
        partition_path_encoder,
        df_to_np,
    )
    df_to_np(df, os.path.join(base_dir, "test.npy"))
    partition_path_filter = PathPartitionFilter.of(
        style=style,
        base_dir=base_dir,
        field_names=partition_keys,
        filter_fn=skip_unpartitioned,
    )
    ds = ray.data.read_numpy(base_dir, partition_filter=partition_path_filter)

    def sorted_values_transform_fn(sorted_values):
        # HACK: `assert_base_partitioned_ds` doesn't properly sort the values. This is a
        # hack to make the test pass.
        # TODO(@bveeramani): Clean this up.
        actually_sorted_values = sorted(sorted_values[0], key=lambda item: tuple(item))
        return str([actually_sorted_values])

    vals = [[1, 0], [1, 1], [1, 2], [3, 3], [3, 4], [3, 5]]
    val_str = "".join(f"array({v}, dtype=int8), " for v in vals)[:-2]
    assert_base_partitioned_ds(
        ds,
        schema=Schema(pa.schema([("data", tensor_type((2,), pa.int8()))])),
        sorted_values=f"[[{val_str}]]",
        ds_take_transform_fn=lambda taken: [extract_values("data", taken)],
        sorted_values_transform_fn=sorted_values_transform_fn,
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
