import gzip
import json
import os
import shutil
from functools import partial

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.json as pajson
import pytest
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data import Schema
from ray.data._internal.datasource.json_datasource import PandasJSONDatasource
from ray.data._internal.pandas_block import PandasBlockBuilder
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.datasource.file_based_datasource import (
    FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD,
)
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.tests.conftest import *  # noqa


def test_json_read_partitioning(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "country=us")
    os.mkdir(path)
    with open(os.path.join(path, "file1.json"), "w") as file:
        json.dump({"number": 0, "string": "foo"}, file)
    with open(os.path.join(path, "file2.json"), "w") as file:
        json.dump({"number": 1, "string": "bar"}, file)

    ds = ray.data.read_json(path)

    assert sorted(ds.take(), key=lambda row: row["number"]) == [
        {"number": 0, "string": "foo", "country": "us"},
        {"number": 1, "string": "bar", "country": "us"},
    ]


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(path1, filesystem=fs)
    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2], override_num_blocks=2, filesystem=fs)
    dsdf = ds.to_pandas()
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().blocks:
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, override_num_blocks=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2, path3], override_num_blocks=2, filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)

    # Directory, two files.
    path = os.path.join(data_path, "test_json_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(path, filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))

    # Two directories, three files.
    path1 = os.path.join(data_path, "test_json_dir1")
    path2 = os.path.join(data_path, "test_json_dir2")
    if fs is None:
        os.mkdir(path1)
        os.mkdir(path2)
    else:
        fs.create_dir(_unwrap_protocol(path1))
        fs.create_dir(_unwrap_protocol(path2))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.json")
    df1.to_json(
        file_path1, orient="records", lines=True, storage_options=storage_options
    )
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.json")
    df2.to_json(
        file_path2, orient="records", lines=True, storage_options=storage_options
    )
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.json")
    df3.to_json(
        file_path3, orient="records", lines=True, storage_options=storage_options
    )
    ds = ray.data.read_json([path1, path2], filesystem=fs)
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path1)
        shutil.rmtree(path2)
    else:
        fs.delete_dir(_unwrap_protocol(path1))
        fs.delete_dir(_unwrap_protocol(path2))

    # Directory and file, two files.
    dir_path = os.path.join(data_path, "test_json_dir")
    if fs is None:
        os.mkdir(dir_path)
    else:
        fs.create_dir(_unwrap_protocol(dir_path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "data1.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([dir_path, path2], filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(dir_path)
    else:
        fs.delete_dir(_unwrap_protocol(dir_path))

    # Directory, two files and non-json file (test default extension-based filtering).
    path = os.path.join(data_path, "test_json_dir")
    if fs is None:
        os.mkdir(path)
    else:
        fs.create_dir(_unwrap_protocol(path))
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)

    # Add a file with a non-matching file extension. This file should be ignored.
    df_txt = pd.DataFrame({"foobar": [1, 2, 3]})
    df_txt.to_json(
        os.path.join(path, "foo.txt"),
        orient="records",
        lines=True,
        storage_options=storage_options,
    )

    ds = ray.data.read_json(path, filesystem=fs)
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)
    if fs is None:
        shutil.rmtree(path)
    else:
        fs.delete_dir(_unwrap_protocol(path))


@pytest.mark.parametrize("ignore_missing_paths", [True, False])
def test_read_json_ignore_missing_paths(
    ray_start_regular_shared, local_path, ignore_missing_paths
):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(local_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True)

    paths = [
        path1,
        "missing.json",
    ]

    if ignore_missing_paths:
        ds = ray.data.read_json(paths, ignore_missing_paths=ignore_missing_paths)
        assert ds.input_files() == [path1]
    else:
        with pytest.raises(FileNotFoundError):
            ds = ray.data.read_json(paths, ignore_missing_paths=ignore_missing_paths)
            ds.materialize()


def test_zipped_json_read(ray_start_regular_shared, tmp_path):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.json.gz")
    df1.to_json(path1, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json(path1)
    assert df1.equals(ds.to_pandas())
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [path1]

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.json.gz")
    df2.to_json(path2, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json([path1, path2], override_num_blocks=2)
    dsdf = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().blocks:
        BlockAccessor.for_block(ray.get(block)).size_bytes()

    # Directory and file, two files.
    dir_path = os.path.join(tmp_path, "test_json_dir")
    os.mkdir(dir_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.json.gz")
    df1.to_json(path1, compression="gzip", orient="records", lines=True)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "data1.json.gz")
    df2.to_json(path2, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json([dir_path, path2])
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
    assert df.equals(dsdf)
    shutil.rmtree(dir_path)


def test_read_json_fallback_from_pyarrow_failure(ray_start_regular_shared, local_path):
    # Try to read this with read_json() to trigger fallback logic
    # to read bytes with json.load().
    data = [{"one": [1]}, {"one": [1, 2]}]
    path1 = os.path.join(local_path, "test1.json")
    with open(path1, "w") as f:
        json.dump(data, f)

    # pyarrow.json cannot read JSONs containing arrays of different lengths.
    from pyarrow import ArrowInvalid

    with pytest.raises(ArrowInvalid):
        pajson.read_json(path1)

    # Ray Data successfully reads this in by
    # falling back to json.load() when pyarrow fails.
    ds = ray.data.read_json(path1)
    assert ds.take_all() == data


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_meta_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path1,
        filesystem=fs,
        meta_provider=FastFileMetadataProvider(),
    )

    # Expect to lazily compute all metadata correctly.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

    with pytest.raises(NotImplementedError):
        ray.data.read_json(
            path1,
            filesystem=fs,
            meta_provider=BaseFileMetadataProvider(),
        )


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_with_read_options(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    # Arrow's JSON ReadOptions isn't serializable in pyarrow < 8.0.0, so this test
    # covers our custom ReadOptions serializer.
    # TODO(Clark): Remove this test and our custom serializer once we require
    # pyarrow >= 8.0.0.
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path1,
        filesystem=fs,
        read_options=pajson.ReadOptions(use_threads=False, block_size=2**30),
    )
    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_with_parse_options(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
):
    # Arrow's JSON ParseOptions isn't serializable in pyarrow < 8.0.0, so this test
    # covers our custom ParseOptions serializer, similar to ReadOptions in above test.
    # TODO(chengsu): Remove this test and our custom serializer once we require
    # pyarrow >= 8.0.0.
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path1,
        filesystem=fs,
        parse_options=pajson.ParseOptions(
            explicit_schema=pa.schema([("two", pa.string())]),
            unexpected_field_behavior="ignore",
        ),
    )
    dsdf = ds.to_pandas()
    assert len(dsdf.columns) == 1
    assert (df1["two"]).equals(dsdf["two"])
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("two", pa.string())]))


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_json_read_partitioned_with_filter(
    style,
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    write_base_partitioned_df,
    assert_base_partitioned_ds,
):
    def df_to_json(dataframe, path, **kwargs):
        dataframe.to_json(path, **kwargs)

    storage_options = (
        {}
        if endpoint_url is None
        else dict(client_kwargs=dict(endpoint_url=endpoint_url))
    )
    file_writer_fn = partial(
        df_to_json,
        orient="records",
        lines=True,
        storage_options=storage_options,
    )
    partition_keys = ["one"]

    def skip_unpartitioned(kv_dict):
        return bool(kv_dict)

    base_dir = os.path.join(data_path, style.value)
    partition_path_encoder = PathPartitionEncoder.of(
        style=style,
        base_dir=base_dir,
        field_names=partition_keys,
        filesystem=fs,
    )
    write_base_partitioned_df(
        partition_keys,
        partition_path_encoder,
        file_writer_fn,
    )
    file_writer_fn(pd.DataFrame({"1": [1]}), os.path.join(base_dir, "test.json"))
    partition_path_filter = PathPartitionFilter.of(
        style=style,
        base_dir=base_dir,
        field_names=partition_keys,
        filter_fn=skip_unpartitioned,
        filesystem=fs,
    )
    ds = ray.data.read_json(
        base_dir,
        partition_filter=partition_path_filter,
        file_extensions=None,
        filesystem=fs,
    )
    assert_base_partitioned_ds(ds)


@pytest.mark.parametrize("override_num_blocks", [None, 1, 3])
def test_jsonl_lists(ray_start_regular_shared, tmp_path, override_num_blocks):
    """Test JSONL with mixed types and schemas."""
    data = [
        ["ray", "rocks", "hello"],
        ["oh", "no"],
        ["rocking", "with", "ray"],
    ]

    path = os.path.join(tmp_path, "test.jsonl")
    with open(path, "w") as f:
        for record in data:
            json.dump(record, f)
            f.write("\n")

    ds = ray.data.read_json(path, lines=True, override_num_blocks=override_num_blocks)
    result = ds.take_all()

    assert result[0] == {"0": "ray", "1": "rocks", "2": "hello"}
    assert result[1] == {"0": "oh", "1": "no", "2": None}
    assert result[2] == {"0": "rocking", "1": "with", "2": "ray"}


def test_jsonl_mixed_types(ray_start_regular_shared, tmp_path):
    """Test JSONL with mixed types and schemas."""
    data = [
        {"a": 1, "b": {"c": 2}},  # Nested dict
        {"a": 1, "b": {"c": 3}},  # Nested dict
        {"a": 1, "b": {"c": {"hello": "world"}}},  # Mixed Schema
    ]

    path = os.path.join(tmp_path, "test.jsonl")
    with open(path, "w") as f:
        for record in data:
            json.dump(record, f)
            f.write("\n")

    ds = ray.data.read_json(path, lines=True)
    result = ds.take_all()

    assert result[0] == data[0]  # Dict stays as is
    assert result[1] == data[1]
    assert result[2] == data[2]


def test_json_write(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0]})
    ds = ray.data.from_blocks([input_df])

    ds.write_json(tmp_path)

    output_df = pd.concat(
        [
            pd.read_json(os.path.join(tmp_path, filename), lines=True)
            for filename in os.listdir(tmp_path)
        ]
    )

    assert rows_same(input_df, output_df)


@pytest.mark.parametrize("override_num_blocks", [None, 2])
def test_json_roundtrip(ray_start_regular_shared, tmp_path, override_num_blocks):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_json(tmp_path)

    ds2 = ray.data.read_json(tmp_path)
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for block, meta in ds2._plan.execute().blocks:
        assert BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_small_file_unit_block_size(
    ray_start_regular_shared, fs, data_path, endpoint_url
):
    """Test reading a small JSON file with unit block_size."""
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(data_path, "test1.json")
    df1.to_json(path1, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path1, filesystem=fs, read_options=pajson.ReadOptions(block_size=1)
    )
    dsdf = ds.to_pandas()
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_file_larger_than_block_size(
    ray_start_regular_shared, fs, data_path, endpoint_url
):
    """Test reading a JSON file larger than the block size."""
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    block_size = 1024
    num_chars = 2500
    num_rows = 3
    df2 = pd.DataFrame(
        {
            "one": ["a" * num_chars for _ in range(num_rows)],
            "two": ["b" * num_chars for _ in range(num_rows)],
        }
    )
    path2 = os.path.join(data_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(
        path2, filesystem=fs, read_options=pajson.ReadOptions(block_size=block_size)
    )
    dsdf = ds.to_pandas()
    assert df2.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == num_rows
    assert ds.input_files() == [_unwrap_protocol(path2)]
    assert ds.schema() == Schema(
        pa.schema([("one", pa.string()), ("two", pa.string())])
    )


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_negative_block_size_fallback(
    ray_start_regular_shared, fs, data_path, endpoint_url
):
    """Test reading JSON with negative block_size triggers fallback to json.load()."""
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df3 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)

    # Negative Buffer Size, fails with arrow but succeeds in fallback to json.load()
    ds = ray.data.read_json(
        path3, filesystem=fs, read_options=pajson.ReadOptions(block_size=-1)
    )
    dsdf = ds.to_pandas()
    assert df3.equals(dsdf)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_zero_block_size_failure(
    ray_start_regular_shared, fs, data_path, endpoint_url
):
    """Test reading JSON with zero block_size fails in both arrow and fallback."""
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    df3 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)

    # Zero Buffer Size, fails with arrow and fails in fallback to json.load()
    with pytest.raises(json.decoder.JSONDecodeError, match="Extra data"):
        ds = ray.data.read_json(
            path3, filesystem=fs, read_options=pajson.ReadOptions(block_size=0)
        )
        dsdf = ds.to_pandas()
        assert dsdf.equals(df3)


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_write_min_rows_per_file(tmp_path, ray_start_regular_shared, min_rows_per_file):
    ray.data.range(100, override_num_blocks=20).write_json(
        tmp_path, min_rows_per_file=min_rows_per_file
    )

    for filename in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, filename), "r") as file:
            num_rows_written = len(file.read().splitlines())
            assert num_rows_written == min_rows_per_file


def test_mixed_gzipped_json_files(ray_start_regular_shared, tmp_path):
    # Create a non-empty gzipped JSON file
    non_empty_file_path = os.path.join(tmp_path, "non_empty.json.gz")
    data = [{"col1": "value1", "col2": "value2", "col3": "value3"}]
    with gzip.open(non_empty_file_path, "wt", encoding="utf-8") as f:
        for record in data:
            json.dump(record, f)
            f.write("\n")

    # Create an empty gzipped JSON file
    empty_file_path = os.path.join(tmp_path, "empty.json.gz")
    with gzip.open(empty_file_path, "wt", encoding="utf-8"):
        pass  # Write nothing to create an empty file

    # Attempt to read both files with Ray
    ds = ray.data.read_json(
        [non_empty_file_path, empty_file_path],
        arrow_open_stream_args={"compression": "gzip"},
    )

    # The dataset should only contain data from the non-empty file
    assert ds.count() == 1
    # Iterate through each row in the dataset and compare with the expected data
    for row in ds.iter_rows():
        assert row == data[0], f"Row {row} does not match expected {data[0]}"

    # Verify the data content using take
    retrieved_data = ds.take(1)[0]
    assert (
        retrieved_data == data[0]
    ), f"Retrieved data {retrieved_data} does not match expected {data[0]}."


def test_json_with_http_path_parallelization(ray_start_regular_shared, httpserver):
    num_files = FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
    urls = []
    for i in range(num_files):
        httpserver.expect_request(f"/file{i}.json").respond_with_json({"id": i})
        urls.append(httpserver.url_for(f"/file{i}.json"))

    ds = ray.data.read_json(urls)
    actual_rows = ds.take_all()

    expected_rows = [{"id": i} for i in range(num_files)]
    assert sorted(actual_rows, key=lambda row: row["id"]) == sorted(
        expected_rows, key=lambda row: row["id"]
    )


class TestPandasJSONDatasource:
    @pytest.mark.parametrize(
        "data",
        [{"a": []}, {"a": [1]}, {"a": [1, 2, 3]}],
        ids=["empty", "single", "multiple"],
    )
    def test_read_stream(self, data, tmp_path):
        # Setup test file.
        df = pd.DataFrame(data)
        path = os.path.join(tmp_path, "test.json")
        df.to_json(path, orient="records", lines=True)

        # Setup datasource.
        local_filesystem = fs.LocalFileSystem()
        source = PandasJSONDatasource(
            path, target_output_size_bytes=1, filesystem=local_filesystem
        )

        # Read stream.
        block_builder = PandasBlockBuilder()
        with source._open_input_source(local_filesystem, path) as f:
            for block in source._read_stream(f, path):
                block_builder.add_block(block)
        block = block_builder.build()

        # Verify.
        assert rows_same(block, df)

    def test_read_stream_with_target_output_size_bytes(self, tmp_path):
        # Setup test file. It contains 16 lines, each line is 8 MiB.
        df = pd.DataFrame({"data": ["a" * 8 * 1024 * 1024] * 16})
        path = os.path.join(tmp_path, "test.json")
        df.to_json(path, orient="records", lines=True)

        # Setup datasource. It should read 32 MiB (4 lines) per output.
        local_filesystem = fs.LocalFileSystem()
        source = PandasJSONDatasource(
            path,
            target_output_size_bytes=32 * 1024 * 1024,
            filesystem=local_filesystem,
        )

        # Read stream.
        block_builder = PandasBlockBuilder()
        with source._open_input_source(local_filesystem, path) as f:
            for block in source._read_stream(f, path):
                assert len(block) == 4
                block_builder.add_block(block)
        block = block_builder.build()

        # Verify.
        assert rows_same(block, df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
