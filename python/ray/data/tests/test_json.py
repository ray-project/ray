import json
import os
import shutil
from functools import partial

import pandas as pd
import pyarrow as pa
import pyarrow.json as pajson
import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_partitioning import PathPartitionEncoder
from ray.data.tests.util import Counter
from ray.tests.conftest import *  # noqa


def test_json_read_partitioning(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "country=us")
    os.mkdir(path)
    with open(os.path.join(path, "file1.json"), "w") as file:
        json.dump({"number": 0, "string": "foo"}, file)
    with open(os.path.join(path, "file2.json"), "w") as file:
        json.dump({"number": 1, "string": "bar"}, file)

    ds = ray.data.read_json(path)

    assert ds.take() == [
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
    assert "{one: int64, two: string}" in str(ds), ds

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(data_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2], parallelism=2, filesystem=fs)
    dsdf = ds.to_pandas()
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, parallelism=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json([path1, path2, path3], parallelism=2, filesystem=fs)
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
    dsdf = ds.to_pandas()
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
    dsdf = ds.to_pandas()
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
    dsdf = ds.to_pandas()
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
    assert ds.num_blocks() == 2
    df = pd.concat([df1, df2], ignore_index=True)
    dsdf = ds.to_pandas()
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

    # Two files, parallelism=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.json.gz")
    df2.to_json(path2, compression="gzip", orient="records", lines=True)
    ds = ray.data.read_json([path1, path2], parallelism=2)
    dsdf = ds.to_pandas()
    assert pd.concat([df1, df2], ignore_index=True).equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().get_blocks_with_metadata():
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
    assert "{one: int64, two: string}" in str(ds), ds

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
    assert "{one: int64, two: string}" in str(ds), ds


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
    assert "{two: string}" in str(ds), ds


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_partitioned_with_filter(
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
    kept_file_counter = Counter.remote()
    skipped_file_counter = Counter.remote()

    def skip_unpartitioned(kv_dict):
        keep = bool(kv_dict)
        counter = kept_file_counter if keep else skipped_file_counter
        ray.get(counter.increment.remote())
        return keep

    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
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
def test_json_write(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))
    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000_000000.json")
    assert df1.equals(
        pd.read_json(
            file_path, orient="records", lines=True, storage_options=storage_options
        )
    )

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path2 = os.path.join(data_path, "data_000001_000000.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat(
        [
            pd.read_json(
                file_path, orient="records", lines=True, storage_options=storage_options
            ),
            pd.read_json(
                file_path2,
                orient="records",
                lines=True,
                storage_options=storage_options,
            ),
        ]
    )
    assert df.equals(ds_df)


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
def test_json_roundtrip(ray_start_regular_shared, fs, data_path):
    # Single block.
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)
    file_path = os.path.join(data_path, "data_000000_000000.json")
    ds2 = ray.data.read_json([file_path], filesystem=fs)
    ds2df = ds2.to_pandas()
    assert ds2df.equals(df)
    # Test metadata ops.
    for block, meta in ds2._plan.execute().get_blocks_with_metadata():
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    if fs is None:
        os.remove(file_path)
    else:
        fs.delete_file(_unwrap_protocol(file_path))

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df, df2])
    ds._set_uuid("data")
    ds.write_json(data_path, filesystem=fs)

    for read_jsonl in [False, True]:
        if fs is None and read_jsonl:
            # Rename input files extension to .jsonl when testing local files.
            # This is to test reading JSONL files.
            for file_name in os.listdir(data_path):
                old_file_path = os.path.join(data_path, file_name)
                new_file_path = old_file_path.replace(".json", ".jsonl")
                os.rename(old_file_path, new_file_path)
        else:
            ds2 = ray.data.read_json(data_path, parallelism=2, filesystem=fs)
        ds2df = ds2.to_pandas()
        assert pd.concat([df, df2], ignore_index=True).equals(ds2df)
        # Test metadata ops.
        for block, meta in ds2._plan.execute().get_blocks_with_metadata():
            BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_write_block_path_provider(
    ray_start_regular_shared,
    fs,
    data_path,
    endpoint_url,
    mock_block_write_path_provider,
):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single block.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    ds = ray.data.from_pandas([df1])
    ds._set_uuid("data")
    ds.write_json(
        data_path, filesystem=fs, block_path_provider=mock_block_write_path_provider
    )
    file_path = os.path.join(data_path, "000000_000000_data.test.json")
    assert df1.equals(
        pd.read_json(
            file_path, orient="records", lines=True, storage_options=storage_options
        )
    )

    # Two blocks.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_pandas([df1, df2])
    ds._set_uuid("data")
    ds.write_json(
        data_path, filesystem=fs, block_path_provider=mock_block_write_path_provider
    )
    file_path2 = os.path.join(data_path, "000001_000000_data.test.json")
    df = pd.concat([df1, df2])
    ds_df = pd.concat(
        [
            pd.read_json(
                file_path, orient="records", lines=True, storage_options=storage_options
            ),
            pd.read_json(
                file_path2,
                orient="records",
                lines=True,
                storage_options=storage_options,
            ),
        ]
    )
    assert df.equals(ds_df)


@pytest.mark.parametrize(
    "fs,data_path,endpoint_url",
    [
        (None, lazy_fixture("local_path"), None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path"), None),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path"), lazy_fixture("s3_server")),
    ],
)
def test_json_read_across_blocks(ray_start_regular_shared, fs, data_path, endpoint_url):
    if endpoint_url is None:
        storage_options = {}
    else:
        storage_options = dict(client_kwargs=dict(endpoint_url=endpoint_url))

    # Single small file, unit block_size
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
    assert "{one: int64, two: string}" in str(ds), ds

    # Single large file, default block_size
    num_chars = 2500000
    num_rows = 3
    df2 = pd.DataFrame(
        {
            "one": ["a" * num_chars for _ in range(num_rows)],
            "two": ["b" * num_chars for _ in range(num_rows)],
        }
    )
    path2 = os.path.join(data_path, "test2.json")
    df2.to_json(path2, orient="records", lines=True, storage_options=storage_options)
    ds = ray.data.read_json(path2, filesystem=fs)
    dsdf = ds.to_pandas()
    assert df2.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == num_rows
    assert ds.input_files() == [_unwrap_protocol(path2)]
    assert "{one: string, two: string}" in str(ds), ds

    # Single file, negative and zero block_size (expect failure)
    df3 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path3 = os.path.join(data_path, "test3.json")
    df3.to_json(path3, orient="records", lines=True, storage_options=storage_options)

    # Negative Buffer Size, fails with arrow but succeeds in fallback to json.load()
    ds = ray.data.read_json(
        path3, filesystem=fs, read_options=pajson.ReadOptions(block_size=-1)
    )
    dsdf = ds.to_pandas()

    # Zero Buffer Size, fails with arrow and fails in fallback to json.load()
    with pytest.raises(json.decoder.JSONDecodeError, match="Extra data"):
        ds = ray.data.read_json(
            path3, filesystem=fs, read_options=pajson.ReadOptions(block_size=0)
        )
        dsdf = ds.to_pandas()


@pytest.mark.parametrize("num_rows_per_file", [5, 10, 50])
def test_write_num_rows_per_file(tmp_path, ray_start_regular_shared, num_rows_per_file):
    ray.data.range(100, parallelism=20).write_json(
        tmp_path, num_rows_per_file=num_rows_per_file
    )

    for filename in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, filename), "r") as file:
            num_rows_written = len(file.read().splitlines())
            assert num_rows_written == num_rows_per_file


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
