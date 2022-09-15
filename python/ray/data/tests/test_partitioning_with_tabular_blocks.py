from typing import List, Union
import os
import pandas as pd
import pytest
from typing import Any, Dict
import pyarrow as pa

import ray
from ray.data.block import Block
from ray.data.dataset import Dataset
from ray.data.datasource import FileBasedDatasource
from ray.data.datasource.partitioning import Partitioning
from ray.tests.conftest import *  # noqa


class CSVDatasource(FileBasedDatasource):
    def __init__(self, block_type: Union[pd.DataFrame, pa.Table]):
        self._block_type = block_type

    def _read_file(self, f: pa.NativeFile, path: str, **kwargs) -> Block:
        assert self._block_type in {pd.DataFrame, pa.Table}

        if self._block_type is pa.Table:
            from pyarrow import csv

            return csv.read_csv(f)

        if self._block_type is pd.DataFrame:
            return pd.read_csv(f)


def write_csv(data: Dict[str, List[Any]], path: str) -> None:
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def read_csv(
    paths: Union[str, List[str]],
    *,
    partitioning: Partitioning,
    block_type: Union[pd.DataFrame, pa.Table],
) -> Dataset:
    datasource = CSVDatasource(block_type=block_type)
    return ray.data.read_datasource(datasource, paths=paths, partitioning=partitioning)


@pytest.mark.parametrize("block_type", [pd.DataFrame, pa.Table])
class TestReadHivePartitionedFilesWithTabularBlocks:
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
    @pytest.mark.skip
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
        path = os.path.join(tmp_path, "country=fr", "data.csv")
        write_csv({"country": ["ir", "ir", "ir"]}, path)

        with pytest.raises(AssertionError):
            # `read_csv` should error because `country` is a field in both the CSV and
            # the path.
            read_csv(path, partitioning=Partitioning("hive"), block_type=block_type)


@pytest.mark.parametrize("block_type", [pd.DataFrame, pa.Table])
class TestReadUnpartitionedFilesWithTabularBlocks:
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
    @pytest.mark.skip
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
class TestReadDirPartitionedFilesWithTabularBlocks:
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
            )

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
            )

    def test_read_files_with_conflicting_key(
        self, tmp_path, block_type, ray_start_regular_shared
    ):
        path = os.path.join(tmp_path, "fr", "data.csv")
        write_csv({"country": ["ir", "ir", "ir"]}, path)

        with pytest.raises(AssertionError):
            # `read_csv` should error because `country` is a field in both the CSV and
            # the path.
            read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["country"], base_dir=tmp_path
                ),
                block_type=block_type,
            )

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
