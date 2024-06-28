import os
import zipfile

import pyarrow as pa
import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.datasource.path_util import _unwrap_protocol

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (
    8,
    0,
    0,
)
pytestmark = pytest.mark.skipif(
    PYARROW_LE_8_0_0, reason="hudi only supported if pyarrow >= 8.0.0"
)


def _extract_testing_table(target_path) -> str:
    fixture_path = "../examples/data/hudi-tables/0.x_cow_partitioned.zip"
    with zipfile.ZipFile(fixture_path, "r") as zip_ref:
        zip_ref.extractall(target_path)
    return os.path.join(target_path, "trips_table")


@pytest.mark.parametrize(
    "fs,data_path",
    [
        (None, lazy_fixture("local_path")),
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
    ],
)
def test_read_hudi_table(fs, data_path):
    setup_data_path = _unwrap_protocol(data_path)
    target_testing_path = os.path.join(setup_data_path, "test_hudi")
    target_table_path = _extract_testing_table(target_testing_path)

    ds = ray.data.read_hudi_table(target_table_path)

    assert ds.count() == 5
    assert ds.schema() is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
