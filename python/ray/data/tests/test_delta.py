import os

import pyarrow as pa
import pytest
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data import Schema
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "data_path",
    [
        lazy_fixture("local_path"),
        lazy_fixture("s3_path"),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_read_basic(data_path, batch_size, write_mode):
    import pandas as pd
    from deltalake import write_deltalake

    # Parse the data path.
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "tmp_test_delta")

    # Create a sample Delta Lake table
    df = pd.DataFrame(
        {"x": [42] * batch_size, "y": ["a"] * batch_size, "z": [3.14] * batch_size}
    )
    if write_mode == "append":
        write_deltalake(path, df, mode=write_mode)
        write_deltalake(path, df, mode=write_mode)
    elif write_mode == "overwrite":
        write_deltalake(path, df, mode=write_mode)

    # Read the Delta Lake table
    ds = ray.data.read_delta(path)

    if write_mode == "append":
        assert ds.count() == batch_size * 2
    elif write_mode == "overwrite":
        assert ds.count() == batch_size

    assert ds.schema() == Schema(
        pa.schema(
            {
                "x": pa.int64(),
                "y": pa.string(),
                "z": pa.float64(),
            }
        )
    )

    if batch_size > 0:
        assert ds.take(1)[0] == {"x": 42, "y": "a", "z": 3.14}
    assert ds.schema().names == ["x", "y", "z"]


@pytest.mark.parametrize(
    "data_path",
    [
        lazy_fixture("local_path"),
        lazy_fixture("s3_path"),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_write_partitioned(data_path, batch_size, write_mode):
    """Test writing a partitioned Delta table and reading it back."""
    import pandas as pd

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "tmp_test_delta_partitioned")

    # Add a partition column 'part'
    partition_data = ["GroupA" if i % 2 == 0 else "GroupB" for i in range(batch_size)]
    df = pd.DataFrame(
        {
            "x": [42] * batch_size,
            "y": ["a"] * batch_size,
            "z": [3.14] * batch_size,
            "part": partition_data,
        }
    )
    ds = ray.data.from_pandas(df)

    ds.write_delta(path, mode=write_mode, partition_cols=["part"])

    # If 'append', write again (should double count)
    if write_mode == "append":
        ds.write_delta(path, mode=write_mode, partition_cols=["part"])

    # Read the table back
    res_ds = ray.data.read_delta(path)

    expected_rows = batch_size if write_mode == "overwrite" else batch_size * 2
    assert res_ds.count() == expected_rows

    # Ensure schema has both data and partition fields
    assert set(res_ds.schema().names) == {"x", "y", "z", "part"}

    # Check that all partition values exist in the result
    result_parts = {row["part"] for row in res_ds.take(20)}  # Just sample 20
    if batch_size > 1:
        assert result_parts == {"GroupA", "GroupB"}
    else:
        # Only one row, must be either
        assert list(result_parts)[0] in {"GroupA", "GroupB"}

    # Optionally: check that partitioned directories exist (for local_path only)
    if data_path == lazy_fixture("local_path"):
        assert any(
            d.startswith("part=") for d in os.listdir(path)
        ), f"No partition directories found in {path}"

    # Optionally: check correctness of all sample fields
    one_row = res_ds.take(1)[0]
    assert one_row["x"] == 42
    assert one_row["y"] == "a"
    assert one_row["z"] == 3.14
    assert one_row["part"] in {"GroupA", "GroupB"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
