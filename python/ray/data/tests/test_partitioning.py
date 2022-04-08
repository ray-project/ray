import json
import pytest
import posixpath

from pyarrow.fs import FileType
from pytest_lazyfixture import lazy_fixture
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem

from ray.data.datasource.partitioning import PathPartitionBase

from ray.tests.conftest import *  # noqa
from ray.data.datasource import (
    PathPartitionParser,
    PathPartitionGenerator,
    PartitionStyle,
)
from ray.data.tests.conftest import *  # noqa


def test_partition_style_serde_round_trip():
    for style in PartitionStyle:
        serialized = json.dumps(style)
        deserialized = PartitionStyle(json.loads(serialized))
        assert deserialized == style


def test_path_partition_base_properties():
    style = PartitionStyle.DIRECTORY
    base_dir = "/foo/bar"
    field_names = ["baz", "qux"]
    path_partition_base = PathPartitionBase(style, base_dir, field_names)
    assert path_partition_base.style == style
    assert path_partition_base.base_dir == base_dir
    assert path_partition_base.normalized_base_dir is None
    assert path_partition_base.field_names == field_names

    path_partition_base = PathPartitionBase(style, None, field_names)
    assert path_partition_base.style == style
    assert path_partition_base.base_dir == ""
    assert path_partition_base.normalized_base_dir is None
    assert path_partition_base.field_names == field_names


def test_path_partition_generator_errors():
    # no field names for default HIVE path partitioning
    with pytest.raises(ValueError):
        PathPartitionGenerator()
    # explicit no field names for HIVE path partitioning
    with pytest.raises(ValueError):
        PathPartitionGenerator(style=PartitionStyle.HIVE, field_names=[])
    # invalid path partitioning style
    with pytest.raises(ValueError):
        PathPartitionGenerator(style=None)
    # partition field name and field value length mismatch
    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        path_partition_generator = PathPartitionGenerator(
            style,
            field_names=["foo", "bar"],
        )
        with pytest.raises(TypeError):
            path_partition_generator(None, None)
        with pytest.raises(AssertionError):
            path_partition_generator([], None)
        with pytest.raises(AssertionError):
            path_partition_generator(["1"], None)
        with pytest.raises(AssertionError):
            path_partition_generator(["1", "2", "3"], None)


@pytest.mark.parametrize(
    "fs,base_dir",
    [
        (None, None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
        ),
    ],
)
def test_path_partition_generator_hive(fs, base_dir):
    field_names = ["foo", "bar"]
    path_partition_generator = PathPartitionGenerator(
        field_names=field_names,
        base_dir=base_dir,
    )
    assert path_partition_generator.normalized_base_dir is None
    partition_values = ["1", "2"]
    partition_path = path_partition_generator(partition_values, fs)
    assert path_partition_generator.normalized_base_dir is not None
    assert partition_path == posixpath.join(
        path_partition_generator.normalized_base_dir,
        "foo=1",
        "bar=2",
    )
    if fs is not None:
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.NotFound
        fs.create_dir(partition_path)
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.Directory


@pytest.mark.parametrize(
    "fs,base_dir",
    [
        (None, None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
        ),
    ],
)
def test_path_partition_generator_directory(fs, base_dir):
    path_partition_generator = PathPartitionGenerator(
        style=PartitionStyle.DIRECTORY,
        field_names=["foo", "bar"],
        base_dir=base_dir,
    )
    assert path_partition_generator.normalized_base_dir is None
    partition_values = ["1", "2"]
    partition_path = path_partition_generator(partition_values, fs)
    assert path_partition_generator.normalized_base_dir is not None
    assert partition_path == posixpath.join(
        path_partition_generator.normalized_base_dir,
        *partition_values,
    )
    if fs is not None:
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.NotFound
        fs.create_dir(partition_path)
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.Directory
    path_partition_generator = PathPartitionGenerator(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
    )
    partition_path = path_partition_generator([], fs)
    assert partition_path == path_partition_generator.normalized_base_dir
    partition_path = path_partition_generator(partition_values, fs)
    assert partition_path == posixpath.join(
        path_partition_generator.normalized_base_dir,
        *partition_values,
    )


def test_path_partition_filter_errors():
    # no field names for DIRECTORY path partitioning
    with pytest.raises(ValueError):
        PathPartitionParser(style=PartitionStyle.DIRECTORY)
    # explicit no field names for DIRECTORY path partitioning
    with pytest.raises(ValueError):
        PathPartitionParser(style=PartitionStyle.DIRECTORY, field_names=[])
    # invalid path partitioning style
    with pytest.raises(ValueError):
        PathPartitionParser(style=None)
    # HIVE partition field name and field value length or order mismatch
    path_partition_parser = PathPartitionParser(
        PartitionStyle.HIVE,
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d["foo"] == 1,
    )
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["foo=1/"], None)
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["bar=1/foo=2/"], None)
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["foo=1/bar=2/qux=3/"], None)
    # ensure HIVE partition base directory is not considered a partition
    path_partition_parser = PathPartitionParser(
        PartitionStyle.HIVE,
        base_dir="foo=1",
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d["foo"] == 1,
    )
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["foo=1/bar=2/"], None)
    # DIRECTORY partition field name and field value length mismatch
    path_partition_parser = PathPartitionParser(
        PartitionStyle.DIRECTORY,
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d["foo"] == 1,
    )
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["1/"], None)
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["1/2/3/"], None)
    # ensure DIRECTORY partition base directory is not considered a partition
    path_partition_parser = PathPartitionParser(
        PartitionStyle.DIRECTORY,
        base_dir="1",
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d["foo"] == 1,
    )
    with pytest.raises(AssertionError):
        path_partition_parser.filter_paths(["1/2/"], None)


@pytest.mark.parametrize(
    "fs,base_dir",
    [
        (None, None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
        ),
    ],
)
def test_path_partition_filter_hive(fs, base_dir):
    pass_through = PathPartitionParser(base_dir=base_dir)
    paths = pass_through.filter_paths([], fs)
    assert paths == []
    paths = pass_through.filter_paths(["foo/1", "bar/2", "baz/3"], fs)
    assert paths == ["foo/1", "bar/2", "baz/3"]

    filter_unpartitioned = PathPartitionParser(
        base_dir=base_dir,
        filter_fn=lambda d: bool(d),
    )
    base_dir = base_dir or ""
    dirs, _ = _resolve_paths_and_filesystem(base_dir, fs)
    base_dir = dirs[0]
    test_paths = [
        posixpath.join(base_dir, "test.txt"),
        posixpath.join(base_dir, "foo/test.txt"),
        posixpath.join(base_dir, "foo=1/test.txt"),
        posixpath.join(base_dir, "foo/bar=2/test.txt"),
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "foo/bar/qux=3/"),
        posixpath.join(base_dir, "foo/bar/qux=3"),
        posixpath.join(base_dir, "test=1.txt"),
    ]
    if base_dir:
        test_paths.extend(["test.txt", "foo=1/test.txt"])
    assert filter_unpartitioned.normalized_base_dir is None
    paths = filter_unpartitioned.filter_paths(test_paths, fs)
    assert filter_unpartitioned.normalized_base_dir is not None
    assert paths == [
        posixpath.join(base_dir, "foo=1/test.txt"),
        posixpath.join(base_dir, "foo/bar=2/test.txt"),
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "foo/bar/qux=3/"),
    ]

    filter_values = PathPartitionParser(
        base_dir=base_dir,
        filter_fn=lambda d: d
        and (d.get("qux") == "3" or (d.get("foo") == "1" and d.get("bar") == "2")),
    )
    paths = filter_values.filter_paths(test_paths, fs)
    assert paths == [
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "foo/bar/qux=3/"),
    ]

    filter_field_name_values = PathPartitionParser(
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d.get("foo") == "1" and d.get("bar") == "2",
    )
    test_paths = [
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "prefix/foo=1/padding/bar=2/test"),
    ]
    paths = filter_field_name_values.filter_paths(test_paths, fs)
    assert paths == test_paths


@pytest.mark.parametrize(
    "fs,base_dir",
    [
        (None, None),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
        (
            lazy_fixture("s3_fs_with_special_chars"),
            lazy_fixture("s3_path_with_special_chars"),
        ),
    ],
)
def test_path_partition_filter_directory(fs, base_dir):
    pass_through = PathPartitionParser(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
    )
    paths = pass_through.filter_paths([], fs)
    assert paths == []
    paths = pass_through.filter_paths(["foo/1", "bar/2", "baz/3"], fs)
    assert paths == ["foo/1", "bar/2", "baz/3"]

    filter_unpartitioned = PathPartitionParser(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filter_fn=lambda d: bool(d),
    )
    base_dir = base_dir or ""
    dirs, _ = _resolve_paths_and_filesystem(base_dir, fs)
    base_dir = dirs[0]
    test_paths = [
        posixpath.join(base_dir, "test.txt"),
        posixpath.join(base_dir, "1/2/test.txt"),
        posixpath.join(base_dir, "1/2/"),
        posixpath.join(base_dir, "2/1/"),
        posixpath.join(base_dir, "1/2/3"),
    ]
    if base_dir:
        # files outside of the base directory are implicitly unpartitioned
        test_paths.extend(["test.txt", "1/2/test.txt"])
    assert filter_unpartitioned.normalized_base_dir is None
    paths = filter_unpartitioned.filter_paths(test_paths, fs)
    assert filter_unpartitioned.normalized_base_dir is not None
    assert paths == [
        posixpath.join(base_dir, "1/2/test.txt"),
        posixpath.join(base_dir, "1/2/"),
        posixpath.join(base_dir, "2/1/"),
        posixpath.join(base_dir, "1/2/3"),
    ]

    filter_values = PathPartitionParser(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filter_fn=lambda d: d and d["foo"] == "1" and d["bar"] == "2",
    )
    paths = filter_values.filter_paths(test_paths, fs)
    assert paths == [
        posixpath.join(base_dir, "1/2/test.txt"),
        posixpath.join(base_dir, "1/2/"),
        posixpath.join(base_dir, "1/2/3"),
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
