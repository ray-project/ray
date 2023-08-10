import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    Partitioning,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.data.tests.util import Counter, extract_values
from ray.tests.conftest import *  # noqa


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
    ds = ray.data.range_tensor(10, parallelism=2)
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
    ds = ray.data.range_tensor(10, parallelism=2)
    ds.write_numpy(data_path, filesystem=fs, column="data")
    ds = ray.data.read_numpy(data_path, filesystem=fs)
    assert str(ds) == (
        "Dataset(\n"
        "   num_blocks=2,\n"
        "   num_rows=?,\n"
        "   schema={data: numpy.ndarray(shape=(1,), dtype=int64)}\n"
        ")"
    )
    np.testing.assert_equal(
        extract_values("data", ds.take(2)), [np.array([0]), np.array([1])]
    )


def test_numpy_read(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    np.save(os.path.join(path, "test.npy"), np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(path, parallelism=1)
    assert str(ds) == (
        "Dataset(\n"
        "   num_blocks=1,\n"
        "   num_rows=10,\n"
        "   schema={data: numpy.ndarray(shape=(1,), dtype=int64)}\n"
        ")"
    )
    np.testing.assert_equal(
        extract_values("data", ds.take(2)), [np.array([0]), np.array([1])]
    )

    # Add a file with a non-matching file extension. This file should be ignored.
    with open(os.path.join(path, "foo.txt"), "w") as f:
        f.write("foobar")

    ds = ray.data.read_numpy(path, parallelism=1)
    assert ds.num_blocks() == 1
    assert ds.count() == 10
    assert str(ds) == (
        "Dataset(\n"
        "   num_blocks=1,\n"
        "   num_rows=10,\n"
        "   schema={data: numpy.ndarray(shape=(1,), dtype=int64)}\n"
        ")"
    )
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
            ds.fully_executed()


def test_numpy_read_meta_provider(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_np_dir")
    os.mkdir(path)
    path = os.path.join(path, "test.npy")
    np.save(path, np.expand_dims(np.arange(0, 10), 1))
    ds = ray.data.read_numpy(
        path, meta_provider=FastFileMetadataProvider(), parallelism=1
    )
    assert str(ds) == (
        "Dataset(\n"
        "   num_blocks=1,\n"
        "   num_rows=10,\n"
        "   schema={data: numpy.ndarray(shape=(1,), dtype=int64)}\n"
        ")"
    )
    np.testing.assert_equal(
        extract_values("data", ds.take(2)), [np.array([0]), np.array([1])]
    )

    with pytest.raises(NotImplementedError):
        ray.data.read_binary_files(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_numpy_read_partitioned_with_filter(
    ray_start_regular_shared,
    tmp_path,
    write_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_np(dataframe, path, **kwargs):
        np.save(path, dataframe.to_numpy(dtype=np.dtype(np.int8)), **kwargs)

    df = pd.DataFrame({"one": [1, 1, 1, 3, 3, 3], "two": [0, 1, 2, 3, 4, 5]})
    partition_keys = ["one"]
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
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

        vals = [[1, 0], [1, 1], [1, 2], [3, 3], [3, 4], [3, 5]]
        val_str = "".join(f"array({v}, dtype=int8), " for v in vals)[:-2]
        assert_base_partitioned_ds(
            ds,
            schema="{data: numpy.ndarray(shape=(2,), dtype=int8)}",
            sorted_values=f"[[{val_str}]]",
            ds_take_transform_fn=lambda taken: [extract_values("data", taken)],
            sorted_values_transform_fn=lambda sorted_values: str(sorted_values),
        )
        assert ray.get(kept_file_counter.get.remote()) == 2
        assert ray.get(skipped_file_counter.get.remote()) == 1
        ray.get(kept_file_counter.reset.remote())
        ray.get(skipped_file_counter.reset.remote())


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_numpy_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds._set_uuid("data")
    ds.write_numpy(data_path, filesystem=fs, column="data")
    file_path1 = os.path.join(data_path, "data_000000_000000.npy")
    file_path2 = os.path.join(data_path, "data_000001_000000.npy")
    if endpoint_url is None:
        arr1 = np.load(file_path1)
        arr2 = np.load(file_path2)
    else:
        from s3fs.core import S3FileSystem

        s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_url})
        arr1 = np.load(s3.open(file_path1))
        arr2 = np.load(s3.open(file_path2))
    assert ds.count() == 10
    assert len(arr1) == 5
    assert len(arr2) == 5
    assert arr1.sum() == 10
    assert arr2.sum() == 35
    np.testing.assert_equal(extract_values("data", ds.take(1)), [np.array([0])])


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_numpy_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    mock_block_write_path_provider,
):
    ds = ray.data.range_tensor(10, parallelism=2)
    ds._set_uuid("data")
    ds.write_numpy(
        data_path,
        filesystem=fs,
        block_path_provider=mock_block_write_path_provider,
        column="data",
    )
    file_path1 = os.path.join(data_path, "000000_000000_data.test.npy")
    file_path2 = os.path.join(data_path, "000001_000000_data.test.npy")
    if endpoint_url is None:
        arr1 = np.load(file_path1)
        arr2 = np.load(file_path2)
    else:
        from s3fs.core import S3FileSystem

        s3 = S3FileSystem(client_kwargs={"endpoint_url": endpoint_url})
        arr1 = np.load(s3.open(file_path1))
        arr2 = np.load(s3.open(file_path2))
    assert ds.count() == 10
    assert len(arr1) == 5
    assert len(arr2) == 5
    assert arr1.sum() == 10
    assert arr2.sum() == 35
    np.testing.assert_equal(extract_values("data", ds.take(1)), [np.array([0])])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
