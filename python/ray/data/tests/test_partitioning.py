import json
import os
import posixpath
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import pandas as pd
import pyarrow
import pyarrow as pa
import pytest
from pyarrow.fs import FileType
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.block import Block
from ray.data.dataset import Dataset
from ray.data.datasource import FileBasedDatasource, PathPartitionParser
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.partitioning import (
    Partitioning,
    PartitionStyle,
    PathPartitionFilter,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class CSVDatasource(FileBasedDatasource):
    def __init__(
        self,
        paths,
        block_type: Union[pd.DataFrame, pa.Table],
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self._block_type = block_type

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[Block]:
        assert self._block_type in {pd.DataFrame, pa.Table}

        if self._block_type is pa.Table:
            from pyarrow import csv

            yield csv.read_csv(f)

        if self._block_type is pd.DataFrame:
            yield pd.read_csv(f)


def write_csv(data: Dict[str, List[Any]], path: str) -> None:
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, na_rep="NA")


def read_csv(
    paths: Union[str, List[str]],
    *,
    partitioning: Partitioning,
    block_type: Union[pd.DataFrame, pa.Table],
) -> Dataset:
    datasource = CSVDatasource(paths, block_type=block_type, partitioning=partitioning)
    return ray.data.read_datasource(datasource)


class PathPartitionEncoder:
    """Callable that generates directory path strings for path-based partition formats.
    Path-based partition formats embed all partition keys and values directly in
    their dataset file paths.
    Two path partition formats are currently supported - `HIVE` and `DIRECTORY`.
    For `HIVE` Partitioning, all partition directories will be generated using a
    `{key1}={value1}/{key2}={value2}` naming convention under the base directory.
    An accompanying ordered list of partition key field names must also be
    provided, where the order and length of all partition values must match the
    order and length of field names
    For `DIRECTORY` Partitioning, all directories will be generated from partition
    values using a `{value1}/{value2}` naming convention under the base directory.
    """

    @staticmethod
    def of(
        style: PartitionStyle = PartitionStyle.HIVE,
        base_dir: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ) -> "PathPartitionEncoder":
        """Creates a new partition path encoder.
        Args:
            style: The partition style - may be either HIVE or DIRECTORY.
            base_dir: "/"-delimited base directory that all partition paths will be
                generated under (exclusive).
            field_names: The partition key field names (i.e. column names for tabular
                datasets). Required for HIVE partition paths, optional for DIRECTORY
                partition paths. When non-empty, the order and length of partition key
                field names must match the order and length of partition values.
            filesystem: Filesystem that will be used for partition path file I/O.
        Returns:
            The new partition path encoder.
        """
        scheme = Partitioning(style, base_dir, field_names, None, filesystem)
        return PathPartitionEncoder(scheme)

    def __init__(self, partitioning: Partitioning):
        """Creates a new partition path encoder.
        Args:
            partitioning: The path-based partition scheme. All partition paths
                will be generated under this scheme's base directory. Field names are
                required for HIVE partition paths, optional for DIRECTORY partition
                paths. When non-empty, the order and length of partition key field
                names must match the order and length of partition values.
        """
        style = partitioning.style
        field_names = partitioning.field_names
        if style == PartitionStyle.HIVE and not field_names:
            raise ValueError(
                "Hive partition path generation requires a corresponding list of "
                "partition key field names. Please retry your request with one "
                "or more field names specified."
            )
        generators = {
            PartitionStyle.HIVE: self._as_hive_partition_dirs,
            PartitionStyle.DIRECTORY: self._as_directory_partition_dirs,
        }
        self._encoder_fn: Callable[[List[str]], List[str]] = generators.get(style)
        if self._encoder_fn is None:
            raise ValueError(
                f"Unsupported partition style: {style}. "
                f"Supported styles: {generators.keys()}"
            )
        self._scheme = partitioning

    def __call__(self, partition_values: List[str]) -> str:
        """Returns the partition directory path for the given partition value strings.
        All files for this partition should be written to this directory. If a base
        directory is set, then the partition directory path returned will be rooted in
        this base directory.
        Args:
            partition_values: The partition value strings to include in the partition
                path. For HIVE partition paths, the order and length of partition
                values must match the order and length of partition key field names.
        Returns:
            Partition directory path for the given partition values.
        """
        partition_dirs = self._as_partition_dirs(partition_values)
        return posixpath.join(self._scheme.normalized_base_dir, *partition_dirs)

    @property
    def scheme(self) -> Partitioning:
        """Returns the partitioning for this encoder."""
        return self._scheme

    def _as_hive_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates HIVE directory names for the given values."""
        field_names = self._scheme.field_names
        return [f"{field_names[i]}={val}" for i, val in enumerate(values)]

    def _as_directory_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates DIRECTORY partition directory names for the given values."""
        return values

    def _as_partition_dirs(self, values: List[str]) -> List[str]:
        """Creates a list of partition directory names for the given values."""
        field_names = self._scheme.field_names
        if field_names:
            assert len(values) == len(field_names), (
                f"Expected {len(field_names)} partition value(s) but found "
                f"{len(values)}: {values}."
            )
        return self._encoder_fn(values)


@pytest.mark.parametrize("block_type", [pd.DataFrame, pa.Table])
class TestReadHivePartitionedFiles:
    def test_read_single_file(self, tmp_path, block_type, ray_start_regular_shared):
        path = os.path.join(tmp_path, "year=1970", "country=fr", "data.csv")
        write_csv({"number": [1, 2, 3]}, path)

        ds = read_csv(path, partitioning=Partitioning("hive"), block_type=block_type)

        df = ds.to_pandas()
        assert list(df.columns) == ["number", "year", "country"]
        assert list(df["number"]) == [1, 2, 3]
        assert list(df["year"]) == ["1970", "1970", "1970"]
        assert list(df["country"]) == ["fr", "fr", "fr"]

    def test_read_multiple_files(self, tmp_path, block_type, ray_start_regular_shared):
        path1 = os.path.join(tmp_path, "year=1970", "country=fr", "data.csv")
        write_csv({"number": [1, 2, 3]}, path1)
        path2 = os.path.join(tmp_path, "year=1971", "country=ir", "data.csv")
        write_csv({"number": [4, 5, 6]}, path2)

        ds = read_csv(
            [path1, path2], partitioning=Partitioning("hive"), block_type=block_type
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["number", "year", "country"]
        assert list(df[df["year"] == "1970"]["number"]) == [1, 2, 3]
        assert list(df[df["year"] == "1970"]["country"]) == ["fr", "fr", "fr"]
        assert list(df[df["year"] == "1971"]["number"]) == [4, 5, 6]
        assert list(df[df["year"] == "1971"]["country"]) == ["ir", "ir", "ir"]

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.csv", "year=1971/language=ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/data.csv"],
        ],
    )
    @pytest.mark.skip  # TODO: Unskip this test once #28869 is fixed.
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, block_type, ray_start_regular_shared
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_csv({"number": [0, 0, 0]}, path)

        with pytest.raises(ValueError):
            read_csv(paths, partitioning=Partitioning("hive"), block_type=block_type)

    def test_read_files_with_conflicting_key(
        self, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "month=01", "data.csv")
        write_csv({"month": [1, 2, 3]}, path)
        with pytest.raises(ValueError):
            # `read_csv` should error because `month` is a field in both the CSV and
            # the path, and the data is different.
            ds = read_csv(
                path, partitioning=Partitioning("hive"), block_type=block_type
            )
            ds.schema()

    @pytest.mark.parametrize("data", [[1, 1, 1], [1, None, 1]])
    def test_read_files_with_legally_conflicting_key(
        self, data, tmp_path, block_type, ray_start_regular_shared
    ):
        # `month` is a field in both the path and the CSV, but because the data is
        # identical, we don't raise an error.
        path = os.path.join(tmp_path, "month=01", "data.csv")
        write_csv({"month": data}, path)

        ds = read_csv(path, partitioning=Partitioning("hive"), block_type=block_type)

        df = ds.to_pandas()
        assert list(df.columns) == ["month"]
        assert list(df["month"]) == [1, 1, 1]


@pytest.mark.parametrize("block_type", [pd.DataFrame, pa.Table])
class TestReadUnpartitionedFiles:
    @pytest.mark.parametrize(
        "relative_path", ["year=1970/country=fr/data.csv", "1970/fr/data.csv"]
    )
    def test_read_single_file(
        self, relative_path, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, relative_path)
        write_csv({"number": [1, 2, 3]}, path)

        ds = read_csv(path, partitioning=None, block_type=block_type)

        # `read_csv` shouldn't include fields like `year` and `country`.`
        assert list(ds.to_pandas().columns) == ["number"]

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.csv", "year=1971/language=ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/data.csv"],
            ["1970/fr/data.csv", "1971/data.csv"],
        ],
    )
    @pytest.mark.skip  # TODO: Unskip this test once #28869 is fixed.
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, block_type, ray_start_regular_shared
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_csv({"number": [0, 0, 0]})

        # `read_csv` shouldn't raise an error if `partitioning` is set to `None`.
        read_csv(paths, partitioning=None, block_type=block_type)


@pytest.mark.parametrize("block_type", [pd.DataFrame, pa.Table])
class TestReadDirPartitionedFiles:
    def test_read_single_file(self, tmp_path, block_type, ray_start_regular_shared):
        path = os.path.join(tmp_path, "1970", "fr", "data.csv")
        write_csv({"number": [1, 2, 3]}, path)

        ds = read_csv(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
            block_type=block_type,
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["number", "year", "country"]
        assert list(df["number"]) == [1, 2, 3]
        assert list(df["year"]) == ["1970", "1970", "1970"]
        assert list(df["country"]) == ["fr", "fr", "fr"]

    def test_read_single_file_with_null_field(
        self, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "1970", "data", "data.csv")
        write_csv({"number": [1, 2, 3]}, path)

        ds = read_csv(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", None], base_dir=tmp_path
            ),
            block_type=block_type,
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["number", "year"]
        assert list(df["number"]) == [1, 2, 3]
        assert list(df["year"]) == ["1970", "1970", "1970"]

    def test_read_single_file_with_missing_field(
        self, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "1970", "data.csv")
        write_csv({"number": [0, 0, 0]}, path)

        # `read_csv` should error because `path` is missing the `country` field.
        with pytest.raises(ValueError):
            read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
                block_type=block_type,
            ).schema()

    @pytest.mark.parametrize(
        "relative_path", ["1970/data.csv", "1970/us/94704/data.csv"]
    )
    def test_read_single_file_with_invalid_field_names(
        self, relative_path, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, relative_path)
        write_csv({"number": [0, 0, 0]}, path)

        with pytest.raises(ValueError):
            read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
                block_type=block_type,
            ).schema()

    def test_read_files_with_conflicting_key(
        self, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "01", "data.csv")
        write_csv({"month": [1, 2, 3]}, path)
        with pytest.raises(ValueError):
            # `read_csv` should error because `month` is a field in both the CSV and
            # the path, and the data is different.
            read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["month"], base_dir=tmp_path
                ),
                block_type=block_type,
            ).schema()

    @pytest.mark.parametrize("data", [[1, 1, 1], [1, None, 1]])
    def test_read_files_with_legally_conflicting_key(
        self, data, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "01", "data.csv")
        write_csv({"month": data}, path)

        # `month` is a field in both the path and the CSV, but because the data is
        # identical, we don't raise an error.
        ds = read_csv(
            path,
            partitioning=Partitioning("dir", field_names=["month"], base_dir=tmp_path),
            block_type=block_type,
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["month"]
        assert list(df["month"]) == [1, 1, 1]

    def test_read_multiple_files(self, tmp_path, block_type, ray_start_regular_shared):
        path1 = os.path.join(tmp_path, "1970", "fr", "data.csv")
        write_csv({"number": [1, 2, 3]}, path1)
        path2 = os.path.join(tmp_path, "1971", "ir", "data.csv")
        write_csv({"number": [4, 5, 6]}, path2)

        ds = read_csv(
            [path1, path2],
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
            block_type=block_type,
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["number", "year", "country"]
        assert list(df[df["year"] == "1970"]["number"]) == [1, 2, 3]
        assert list(df[df["year"] == "1970"]["country"]) == ["fr", "fr", "fr"]
        assert list(df[df["year"] == "1971"]["number"]) == [4, 5, 6]
        assert list(df[df["year"] == "1971"]["country"]) == ["ir", "ir", "ir"]


def _verify_resolved_paths_and_filesystem(scheme: Partitioning):
    assert scheme.base_dir is not None
    assert scheme.normalized_base_dir is not None
    paths, expected_fs = _resolve_paths_and_filesystem(
        scheme.base_dir,
        scheme.filesystem,
    )
    path = paths[0]
    expected_path = f"{path}/" if path and not path.endswith("/") else path
    assert scheme.normalized_base_dir == expected_path
    assert isinstance(scheme.resolved_filesystem, type(expected_fs))


def test_partition_style_serde_round_trip():
    for style in PartitionStyle:
        serialized = json.dumps(style)
        deserialized = PartitionStyle(json.loads(serialized))
        assert deserialized == style


def test_path_partition_base_properties():
    style = PartitionStyle.DIRECTORY
    base_dir = "/foo/bar"
    field_names = ["baz", "qux"]
    scheme = Partitioning(style, base_dir, field_names, None)
    assert scheme.style == style
    assert scheme.base_dir == base_dir
    assert scheme.field_names == field_names
    _verify_resolved_paths_and_filesystem(scheme)

    scheme = Partitioning(style, None, field_names, None)
    assert scheme.style == style
    assert scheme.base_dir == ""
    assert scheme.field_names == field_names
    _verify_resolved_paths_and_filesystem(scheme)


def test_path_partition_encoder_errors():
    # no field names for default HIVE path partitioning
    with pytest.raises(ValueError):
        PathPartitionEncoder.of()
    # explicit no field names for HIVE path partitioning
    with pytest.raises(ValueError):
        PathPartitionEncoder.of(style=PartitionStyle.HIVE, field_names=[])
    # invalid path partitioning style
    with pytest.raises(ValueError):
        PathPartitionEncoder.of(style=None)
    # partition field name and field value length mismatch
    for style in [PartitionStyle.HIVE, PartitionStyle.DIRECTORY]:
        path_partition_encoder = PathPartitionEncoder.of(
            style,
            field_names=["foo", "bar"],
        )
        with pytest.raises(TypeError):
            path_partition_encoder(None)
        with pytest.raises(AssertionError):
            path_partition_encoder([])
        with pytest.raises(AssertionError):
            path_partition_encoder(["1"])
        with pytest.raises(AssertionError):
            path_partition_encoder(["1", "2", "3"])


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
def test_path_partition_encoder_hive(fs, base_dir):
    field_names = ["foo", "bar"]
    path_partition_encoder = PathPartitionEncoder.of(
        field_names=field_names,
        base_dir=base_dir,
        filesystem=fs,
    )
    _verify_resolved_paths_and_filesystem(path_partition_encoder.scheme)
    partition_values = ["1", "2"]
    partition_path = path_partition_encoder(partition_values)
    assert partition_path == posixpath.join(
        path_partition_encoder.scheme.normalized_base_dir,
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
def test_path_partition_encoder_directory(fs, base_dir):
    path_partition_encoder = PathPartitionEncoder.of(
        style=PartitionStyle.DIRECTORY,
        field_names=["foo", "bar"],
        base_dir=base_dir,
        filesystem=fs,
    )
    _verify_resolved_paths_and_filesystem(path_partition_encoder.scheme)
    partition_values = ["1", "2"]
    partition_path = path_partition_encoder(partition_values)
    assert partition_path == posixpath.join(
        path_partition_encoder.scheme.normalized_base_dir,
        *partition_values,
    )
    if fs is not None:
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.NotFound
        fs.create_dir(partition_path)
        file_info = fs.get_file_info(partition_path)
        assert file_info.type == FileType.Directory
    path_partition_encoder = PathPartitionEncoder.of(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        filesystem=fs,
    )
    partition_path = path_partition_encoder([])
    assert partition_path == path_partition_encoder.scheme.normalized_base_dir
    partition_path = path_partition_encoder(partition_values)
    assert partition_path == posixpath.join(
        path_partition_encoder.scheme.normalized_base_dir,
        *partition_values,
    )


def test_path_partition_parser_errors():
    # no field names for DIRECTORY path partitioning
    with pytest.raises(ValueError):
        PathPartitionParser.of(style=PartitionStyle.DIRECTORY)
    # explicit no field names for DIRECTORY path partitioning
    with pytest.raises(ValueError):
        PathPartitionParser.of(style=PartitionStyle.DIRECTORY, field_names=[])
    # invalid path partitioning style
    with pytest.raises(ValueError):
        PathPartitionParser.of(style=None)
    # HIVE partition field name and field value length or order mismatch
    path_partition_parser = PathPartitionParser.of(
        style=PartitionStyle.HIVE,
        field_names=["foo", "bar"],
    )
    with pytest.raises(ValueError):
        path_partition_parser("foo=1/")
    with pytest.raises(ValueError):
        path_partition_parser("bar=1/foo=2/")
    with pytest.raises(ValueError):
        path_partition_parser("foo=1/bar=2/qux=3/")
    # ensure HIVE partition base directory is not considered a partition
    path_partition_parser = PathPartitionParser.of(
        style=PartitionStyle.HIVE,
        base_dir="foo=1",
        field_names=["foo", "bar"],
    )
    with pytest.raises(ValueError):
        path_partition_parser("foo=1/bar=2/")
    # DIRECTORY partition field name and field value length mismatch
    path_partition_parser = PathPartitionParser.of(
        style=PartitionStyle.DIRECTORY,
        field_names=["foo", "bar"],
    )
    with pytest.raises(ValueError):
        path_partition_parser("1/")
    with pytest.raises(ValueError):
        path_partition_parser("1/2/3/")
    # ensure DIRECTORY partition base directory is not considered a partition
    path_partition_parser = PathPartitionParser.of(
        style=PartitionStyle.DIRECTORY,
        base_dir="1",
        field_names=["foo", "bar"],
    )
    with pytest.raises(ValueError):
        path_partition_parser("1/2/")


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
def test_path_partition_parser_hive(fs, base_dir):
    partition_parser = PathPartitionParser.of(base_dir=base_dir, filesystem=fs)
    _verify_resolved_paths_and_filesystem(partition_parser.scheme)
    base_dir = partition_parser.scheme.normalized_base_dir

    # parse unpartitioned paths...
    partition_kvs = partition_parser("")
    assert partition_kvs == {}
    unpartitioned_paths = [
        "",
        "foo/1",
        "bar/2",
        "baz/3",
        posixpath.join(base_dir, "test.txt"),
        posixpath.join(base_dir, "foo/test.txt"),
        posixpath.join(base_dir, "foo/bar/qux=3"),
        posixpath.join(base_dir, "test=1.txt"),
    ]
    for path in unpartitioned_paths:
        assert partition_parser(path) == {}

    partitioned_path = posixpath.join(base_dir, "foo=1/test.txt")
    assert partition_parser(partitioned_path) == {"foo": "1"}
    partitioned_path = posixpath.join(base_dir, " foo = 1  /test.txt")
    assert partition_parser(partitioned_path) == {" foo ": " 1  "}
    partitioned_path = posixpath.join(base_dir, "foo/bar=2/test.txt")
    assert partition_parser(partitioned_path) == {"bar": "2"}
    partitioned_path = posixpath.join(base_dir, "bar=2/foo=1/test")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}
    partitioned_path = posixpath.join(base_dir, "foo/bar/qux=3/")
    assert partition_parser(partitioned_path) == {"qux": "3"}

    partition_parser = PathPartitionParser.of(
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
    )
    partitioned_path = posixpath.join(base_dir, "foo=1/bar=2/test")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}
    partitioned_path = posixpath.join(base_dir, "prefix/foo=1/padding/bar=2/test")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}


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
def test_path_partition_parser_dir(fs, base_dir):
    partition_parser = PathPartitionParser.of(
        PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
    )
    _verify_resolved_paths_and_filesystem(partition_parser.scheme)
    base_dir = partition_parser.scheme.normalized_base_dir

    # parse unpartitioned paths...
    partition_kvs = partition_parser("")
    assert partition_kvs == {}
    if base_dir:
        unpartitioned_paths = [
            "",
            "foo/1",
            "bar/2",
            "baz/3",
            posixpath.join(base_dir, "test.txt"),
        ]
        for path in unpartitioned_paths:
            assert partition_parser(path) == {}

    partitioned_path = posixpath.join(base_dir, "1/2/test.txt")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}
    partitioned_path = posixpath.join(base_dir, " 1  / t w o /test.txt")
    assert partition_parser(partitioned_path) == {"foo": " 1  ", "bar": " t w o "}
    partitioned_path = posixpath.join(base_dir, "2/1/test.txt")
    assert partition_parser(partitioned_path) == {"foo": "2", "bar": "1"}
    partitioned_path = posixpath.join(base_dir, "1/2/")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}
    partitioned_path = posixpath.join(base_dir, "1/2/3")
    assert partition_parser(partitioned_path) == {"foo": "1", "bar": "2"}

    partition_parser = PathPartitionParser.of(
        PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["bar", "foo"],
        filesystem=fs,
    )
    partitioned_path = posixpath.join(base_dir, "1/2/test")
    assert partition_parser(partitioned_path) == {"bar": "1", "foo": "2"}
    partitioned_path = posixpath.join(base_dir, "2/1/test")
    assert partition_parser(partitioned_path) == {"bar": "2", "foo": "1"}

    partition_parser = PathPartitionParser.of(
        PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["year", None, "country"],
        filesystem=fs,
    )

    partitioned_path = posixpath.join(base_dir, "1970/countries/fr/products.csv")
    assert partition_parser(partitioned_path) == {"year": "1970", "country": "fr"}


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
    pass_through = PathPartitionFilter.of(None, base_dir=base_dir, filesystem=fs)
    _verify_resolved_paths_and_filesystem(pass_through.parser.scheme)
    paths = pass_through([])
    assert paths == []
    paths = pass_through(["foo/1", "bar/2", "baz/3"])
    assert paths == ["foo/1", "bar/2", "baz/3"]

    filter_unpartitioned = PathPartitionFilter.of(
        base_dir=base_dir,
        filesystem=fs,
        filter_fn=lambda d: bool(d),
    )
    _verify_resolved_paths_and_filesystem(filter_unpartitioned.parser.scheme)
    base_dir = filter_unpartitioned.parser.scheme.normalized_base_dir
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
    paths = filter_unpartitioned(test_paths)
    assert paths == [
        posixpath.join(base_dir, "foo=1/test.txt"),
        posixpath.join(base_dir, "foo/bar=2/test.txt"),
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "foo/bar/qux=3/"),
    ]

    filter_values = PathPartitionFilter.of(
        base_dir=base_dir,
        filesystem=fs,
        filter_fn=lambda d: d
        and (d.get("qux") == "3" or (d.get("foo") == "1" and d.get("bar") == "2")),
    )
    _verify_resolved_paths_and_filesystem(filter_values.parser.scheme)
    paths = filter_values(test_paths)
    assert paths == [
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "foo/bar/qux=3/"),
    ]

    filter_field_name_values = PathPartitionFilter.of(
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
        filter_fn=lambda d: d and d.get("foo") == "1" and d.get("bar") == "2",
    )
    test_paths = [
        posixpath.join(base_dir, "foo=1/bar=2/test"),
        posixpath.join(base_dir, "prefix/foo=1/padding/bar=2/test"),
    ]
    paths = filter_field_name_values(test_paths)
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
    pass_through = PathPartitionFilter.of(
        None,
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
    )
    paths = pass_through([])
    assert paths == []
    paths = pass_through(["foo/1", "bar/2", "baz/3"])
    assert paths == ["foo/1", "bar/2", "baz/3"]

    filter_unpartitioned = PathPartitionFilter.of(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
        filter_fn=lambda d: bool(d),
    )
    _verify_resolved_paths_and_filesystem(filter_unpartitioned.parser.scheme)
    base_dir = filter_unpartitioned.parser.scheme.normalized_base_dir
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
    paths = filter_unpartitioned(test_paths)
    assert paths == [
        posixpath.join(base_dir, "1/2/test.txt"),
        posixpath.join(base_dir, "1/2/"),
        posixpath.join(base_dir, "2/1/"),
        posixpath.join(base_dir, "1/2/3"),
    ]

    filter_values = PathPartitionFilter.of(
        style=PartitionStyle.DIRECTORY,
        base_dir=base_dir,
        field_names=["foo", "bar"],
        filesystem=fs,
        filter_fn=lambda d: d and d["foo"] == "1" and d["bar"] == "2",
    )
    _verify_resolved_paths_and_filesystem(filter_values.parser.scheme)
    paths = filter_values(test_paths)
    assert paths == [
        posixpath.join(base_dir, "1/2/test.txt"),
        posixpath.join(base_dir, "1/2/"),
        posixpath.join(base_dir, "1/2/3"),
    ]


@pytest.mark.parametrize(
    "partition_value,expected_type",
    [
        ("1", int),
        ("1.0", float),
        ("spam", str),
        ("true", bool),
    ],
)
def test_field_types(partition_value, expected_type):
    partitioning = Partitioning(style="hive", field_types={"key": expected_type})
    parse = PathPartitionParser(partitioning)

    partitions = parse(f"key={partition_value}/data.parquet")

    assert set(partitions.keys()) == {"key"}
    assert isinstance(partitions["key"], expected_type)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
