from typing import List, Union
import os
import pytest
import pyarrow as pa

import ray
from ray.data.block import Block
from ray.data.dataset import Dataset
from ray.data.datasource import FileBasedDatasource
from ray.data.datasource.partitioning import Partitioning
from ray.tests.conftest import *  # noqa


class BinaryDatasource(FileBasedDatasource):

    _COLUMN_NAME = "bytes"

    def _read_file(self, f: pa.NativeFile, path: str, **reader_args) -> Block:
        return [f.readall()]


def write_bytes(data: bytes, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as file:
        file.write(data)


def read_bytes(
    paths: Union[str, List[str]],
    *,
    partitioning: Partitioning,
) -> Dataset:
    return ray.data.read_datasource(
        BinaryDatasource(), paths=paths, partitioning=partitioning
    )


class TestReadHivePartitionedFiles:
    def test_read_single_file(self, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, "year=1970", "country=fr", "data.bin")
        write_bytes(b"foo", path)

        ds = read_bytes(path, partitioning=Partitioning("hive"))

        df = ds.to_pandas()
        assert list(df.columns) == ["bytes", "year", "country"]
        assert list(df["bytes"]) == [b"foo"]
        assert list(df["year"]) == ["1970"]
        assert list(df["country"]) == ["fr"]

    def test_read_multiple_files(self, tmp_path, ray_start_regular_shared):
        path1 = os.path.join(tmp_path, "year=1970", "country=fr", "data.bin")
        write_bytes(b"foo", path1)
        path2 = os.path.join(tmp_path, "year=1971", "country=ir", "data.bin")
        write_bytes(b"bar", path2)

        ds = read_bytes([path1, path2], partitioning=Partitioning("hive"))

        df = ds.to_pandas()
        assert list(df.columns) == ["bytes", "year", "country"]
        assert df[df["year"] == "1970"].values.tolist() == [[b"foo", "1970", "fr"]]
        assert df[df["year"] == "1971"].values.tolist() == [[b"bar", "1971", "ir"]]

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.bin", "year=1971/language=ir/data.bin"],
            ["year=1970/country=fr/data.bin", "year=1971/ir/data.bin"],
            ["year=1970/country=fr/data.bin", "year=1971/data.bin"],
        ],
    )
    @pytest.mark.skip
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, ray_start_regular_shared
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_bytes(b"foo", path)

        with pytest.raises(ValueError):
            read_bytes(paths, partitioning=Partitioning("hive"))

    def test_read_files_with_conflicting_key(self, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, "bytes=baz", "data.bin")
        write_bytes(b"foo", path)

        with pytest.raises(AssertionError):
            # `read_bytes` should error because `bytes` is both a partition key and the
            # name of the column where data is stored.
            read_bytes(path, partitioning=Partitioning("hive"))


class TestReadUnpartitionedFiles:
    @pytest.mark.parametrize(
        "relative_path", ["year=1970/country=fr/data.bin", "1970/fr/data.bin"]
    )
    def test_read_single_file(self, relative_path, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, relative_path)
        write_bytes(b"foo", path)

        ds = read_bytes(path, partitioning=None)

        # `read_bytes` shouldn't include fields like `year` and `country`.`
        assert ds.schema() is bytes

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.bin", "year=1971/language=ir/data.bin"],
            ["year=1970/country=fr/data.bin", "year=1971/ir/data.bin"],
            ["year=1970/country=fr/data.bin", "year=1971/data.bin"],
            ["1970/fr/data.bin", "1971/data.bin"],
        ],
    )
    @pytest.mark.skip
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, ray_start_regular_shared
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_bytes(b"foo")

        # `read_bytes` shouldn't raise an error if `partitioning` is set to `None`.
        read_bytes(paths, partitioning=None)


class TestReadDirPartitionedFiles:
    def test_read_single_file(self, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, "1970", "fr", "data.bin")
        write_bytes(b"foo", path)

        ds = read_bytes(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["bytes", "year", "country"]
        assert list(df["bytes"]) == [b"foo"]
        assert list(df["year"]) == ["1970"]
        assert list(df["country"]) == ["fr"]

    def test_read_single_file_with_null_field(self, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, "1970", "data", "data.bin")
        write_bytes(b"foo", path)

        ds = read_bytes(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", None], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["bytes", "year"]
        assert list(df["bytes"]) == [b"foo"]
        assert list(df["year"]) == ["1970"]

    def test_read_single_file_with_missing_field(
        self, tmp_path, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "1970", "data.bin")
        write_bytes(b"foo", path)

        # `read_bytes` should error because `path` is missing the `country` field.
        with pytest.raises(ValueError):
            read_bytes(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
            )

    @pytest.mark.parametrize(
        "relative_path", ["1970/data.bin", "1970/us/94704/data.bin"]
    )
    def test_read_single_file_with_invalid_field_names(
        self, relative_path, tmp_path, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, relative_path)
        write_bytes(b"foo", path)

        with pytest.raises(ValueError):
            read_bytes(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
            )

    def test_read_files_with_conflicting_key(self, tmp_path, ray_start_regular_shared):
        path = os.path.join(tmp_path, "bar", "data.bin")
        write_bytes(b"foo", path)

        with pytest.raises(AssertionError):
            # `read_bytes` should error because `bytes` is both a partition key and the
            # name of the column where data is stored.
            read_bytes(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["bytes"], base_dir=tmp_path
                ),
            )

    def test_read_multiple_files(self, tmp_path, ray_start_regular_shared):
        path1 = os.path.join(tmp_path, "1970", "fr", "data.bin")
        write_bytes(b"foo", path1)
        path2 = os.path.join(tmp_path, "1971", "ir", "data.bin")
        write_bytes(b"bar", path2)

        ds = read_bytes(
            [path1, path2],
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["bytes", "year", "country"]
        assert df[df["year"] == "1970"].values.tolist() == [[b"foo", "1970", "fr"]]
        assert df[df["year"] == "1971"].values.tolist() == [[b"bar", "1971", "ir"]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
