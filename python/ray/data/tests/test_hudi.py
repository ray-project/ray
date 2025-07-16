import os
import zipfile

import pytest
from packaging.version import parse as parse_version
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _unwrap_protocol,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

MIN_PYARROW_VERSION_FOR_HUDI = parse_version("11.0.0")
PYARROW_VERSION = get_pyarrow_version()
PYARROW_VERSION_MEETS_REQUIREMENT = (
    PYARROW_VERSION and PYARROW_VERSION >= MIN_PYARROW_VERSION_FOR_HUDI
)
PYARROW_HUDI_TEST_SKIP_REASON = (
    f"Hudi only supported if pyarrow >= {MIN_PYARROW_VERSION_FOR_HUDI}"
)


def _extract_testing_table(fixture_path: str, table_dir: str, target_dir: str) -> str:
    with zipfile.ZipFile(fixture_path, "r") as zip_ref:
        zip_ref.extractall(target_dir)
    return os.path.join(target_dir, table_dir)


def _get_hudi_table_path(fs, data_path, table_name, testing_dir="test_hudi") -> str:
    setup_data_path = _unwrap_protocol(data_path)
    target_testing_dir = os.path.join(setup_data_path, testing_dir)
    fixture_path, _ = _resolve_paths_and_filesystem(
        f"example://hudi-tables/{table_name}.zip", fs
    )
    return _extract_testing_table(fixture_path[0], table_name, target_testing_dir)


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_hudi_snapshot_query_v6_trips_table(ray_start_regular_shared, fs, data_path):
    table_path = _get_hudi_table_path(fs, data_path, "v6_trips_8i1u")

    ds = ray.data.read_hudi(table_path, filters=[("city", "=", "san_francisco")])

    assert ds.schema().names == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert ds.count() == 4
    rows = (
        ds.select_columns(["_hoodie_commit_time", "ts", "rider", "fare"])
        .sort("fare")
        .take_all()
    )
    first_commit = "20250715043008154"
    second_commit = "20250715043011090"
    assert rows == [
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695159649087,
            "rider": "rider-A",
            "fare": 19.10,
        },
        {
            "_hoodie_commit_time": second_commit,
            "ts": 1695046462179,
            "rider": "rider-D",
            "fare": 25.0,
        },
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695091554788,
            "rider": "rider-C",
            "fare": 27.70,
        },
        {
            "_hoodie_commit_time": first_commit,
            "ts": 1695332066204,
            "rider": "rider-E",
            "fare": 93.50,
        },
    ]


@pytest.mark.skipif(
    not PYARROW_VERSION_MEETS_REQUIREMENT,
    reason=PYARROW_HUDI_TEST_SKIP_REASON,
)
@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_hudi_incremental_query_v6_trips_table(ray_start_regular_shared, fs, data_path):
    table_path = _get_hudi_table_path(fs, data_path, "v6_trips_8i1u")

    first_commit = "20250715043008154"
    second_commit = "20250715043011090"
    ds = ray.data.read_hudi(
        table_path,
        query_type="incremental",
        hudi_options={
            "hoodie.read.file_group.start_timestamp": first_commit,
            "hoodie.read.file_group.end_timestamp": second_commit,
        },
    )

    assert ds.schema().names == [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
        "ts",
        "uuid",
        "rider",
        "driver",
        "fare",
        "city",
    ]
    assert ds.count() == 1
    rows = ds.select_columns(["_hoodie_commit_time", "ts", "rider", "fare"]).take_all()
    assert rows == [
        {
            "_hoodie_commit_time": second_commit,
            "ts": 1695046462179,
            "rider": "rider-D",
            "fare": 25.0,
        },
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
