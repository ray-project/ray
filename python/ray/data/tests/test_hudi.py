import os
import zipfile

import pytest
from packaging.version import parse as parse_version
from pytest_lazyfixture import lazy_fixture

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
def test_read_hudi_simple_cow_table(ray_start_regular_shared, fs, data_path):
    setup_data_path = _unwrap_protocol(data_path)
    target_testing_dir = os.path.join(setup_data_path, "test_hudi")
    fixture_path, _ = _resolve_paths_and_filesystem(
        "example://hudi-tables/0.x_cow_partitioned.zip", fs
    )
    target_table_path = _extract_testing_table(
        fixture_path[0], "trips_table", target_testing_dir
    )

    ds = ray.data.read_hudi(target_table_path)

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
    assert ds.count() == 5
    rows = (
        ds.select_columns(["_hoodie_commit_time", "ts", "uuid", "fare"])
        .sort("fare")
        .take_all()
    )
    assert rows == [
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695115999911,
            "uuid": "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa",
            "fare": 17.85,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695159649087,
            "uuid": "334e26e9-8355-45cc-97c6-c31daf0df330",
            "fare": 19.1,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695091554788,
            "uuid": "e96c4396-3fad-413a-a942-4cb36106d721",
            "fare": 27.7,
        },
        {
            "_hoodie_commit_time": "20240402123035233",
            "ts": 1695516137016,
            "uuid": "e3cf430c-889d-4015-bc98-59bdce1e530c",
            "fare": 34.15,
        },
        {
            "_hoodie_commit_time": "20240402144910683",
            "ts": 1695046462179,
            "uuid": "9909a8b1-2d15-4d3d-8ec9-efc48c536a00",
            "fare": 339.0,
        },
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
