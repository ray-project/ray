import os
import pandas as pd
import pytest
from typing import Any, Dict

import ray
from ray.data.datasource.partitioning import Partitioning
from ray.tests.conftest import *  # noqa


def write_csv(data: Dict[str, Any], path: str) -> None:
    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


class TestReadCSVWithHivePartitioning:
    def test_read_single_file(self, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, "year=1970", "country=fr", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        ds = ray.data.read_csv(path, partitioning=Partitioning("hive"))

        df = ds.to_pandas()
        assert list(df.columns) == ["X", "Y", "year", "country"]
        assert list(df["year"]) == ["1970", "1970", "1970"]
        assert list(df["country"]) == ["fr", "fr", "fr"]

    def test_read_multiple_files(self, tmp_path, shutdown_only):
        path1 = os.path.join(tmp_path, "year=1970", "country=fr", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path1)
        path2 = os.path.join(tmp_path, "year=1971", "country=ir", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path2)

        ds = ray.data.read_csv([path1, path2], partitioning=Partitioning("hive"))

        df = ds.to_pandas()
        assert list(df.columns) == ["X", "Y", "year", "country"]
        assert list(df[df["year"] == "1970"]["country"]) == ["fr", "fr", "fr"]
        assert list(df[df["year"] == "1971"]["country"]) == ["ir", "ir", "ir"]

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.csv", "year=1971/language=ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/data.csv"],
        ],
    )
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, shutdown_only
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        with pytest.raises(ValueError):
            ds = ray.data.read_csv(paths, partitioning=Partitioning("hive"))
            print(ds.to_pandas())

    def test_read_files_with_conflicting_key(self, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, "country=fr", "data.csv")
        write_csv({"country": ["ir", "ir", "ir"]}, path)

        with pytest.raises(ValueError):
            # `read_csv` should error because `country` is a field in both the CSV and
            # the path.
            ray.data.read_csv(path, partitioning=Partitioning("hive"))


class TestReadCSVWithNoPartitioning:
    @pytest.mark.parametrize(
        "relative_path", ["year=1970/country=fr/data.csv", "1970/fr/data.csv"]
    )
    def test_read_single_file(self, relative_path, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, relative_path)
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        ds = ray.data.read_csv(path, partitioning=None)

        # `read_csv` shouldn't include fields like `year` and `country`.`
        assert list(ds.to_pandas().columns) == ["X", "Y"]

    @pytest.mark.parametrize(
        "relative_paths",
        [
            ["year=1970/country=fr/data.csv", "year=1971/language=ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/ir/data.csv"],
            ["year=1970/country=fr/data.csv", "year=1971/data.csv"],
            ["1970/fr/data.csv", "1971/data.csv"],
        ],
    )
    def test_read_files_with_mismatched_fields(
        self, relative_paths, tmp_path, shutdown_only
    ):
        paths = [
            os.path.join(tmp_path, relative_path) for relative_path in relative_paths
        ]
        for path in paths:
            write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        # `read_csv` shouldn't raise an error if `partitioning` is set to `None`.
        ds = ray.data.read_csv(paths, partitioning=None)


class TestReadCSVWithDirPartitioning:
    def test_read_single_file(self, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, "1970", "fr", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        ds = ray.data.read_csv(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["X", "Y", "year", "country"]
        assert list(df["year"]) == ["1970", "1970", "1970"]
        assert list(df["country"]) == ["fr", "fr", "fr"]

    def test_read_single_file_with_null_field(self, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, "1970", "data", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        ds = ray.data.read_csv(
            path,
            partitioning=Partitioning(
                "dir", field_names=["year", None], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["X", "Y", "year"]
        assert list(df["year"]) == ["1970", "1970", "1970"]

    def test_read_single_file_with_missing_field(self, tmp_path, shutdown_only):
        path = os.path.join(tmp_path, "1970", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        # `read_csv` should error because `path` is missing the `country` field.
        with pytest.raises(ValueError):
            ds = ray.data.read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
            )

    @pytest.mark.parametrize(
        "relative_path", ["1970/data.csv", "1970/us/94704/data.csv"]
    )
    def test_read_single_file_with_invalid_field_names(
        self, relative_path, tmp_path, shutdown_only
    ):
        path = os.path.join(tmp_path, relative_path)
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path)

        with pytest.raises(ValueError):
            ray.data.read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["year", "country"], base_dir=tmp_path
                ),
            )

    def test_read_files_with_conflicting_key(
        self, relative_paths, tmp_path, shutdown_only
    ):
        path = os.path.join(tmp_path, "fr", "data.csv")
        write_csv({"country": ["ir", "ir", "ir"]}, path)

        with pytest.raises(ValueError):
            # `read_csv` should error because `country` is a field in both the CSV and
            # the path.
            ray.data.read_csv(
                path,
                partitioning=Partitioning(
                    "dir", field_names=["country"], base_dir=tmp_path
                ),
            )

    def test_read_multiple_files(self, tmp_path, shutdown_only):
        path1 = os.path.join(tmp_path, "1970", "fr", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path1)
        path2 = os.path.join(tmp_path, "1971", "ir", "data.csv")
        write_csv({"X": [0, 0, 0], "Y": [0, 0, 0]}, path2)

        ds = ray.data.read_csv(
            [path1, path2],
            partitioning=Partitioning(
                "dir", field_names=["year", "country"], base_dir=tmp_path
            ),
        )

        df = ds.to_pandas()
        assert list(df.columns) == ["X", "Y", "year", "country"]
        assert list(df[df["year"] == "1970"]["country"]) == ["fr", "fr", "fr"]
        assert list(df[df["year"] == "1971"]["country"]) == ["ir", "ir", "ir"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
