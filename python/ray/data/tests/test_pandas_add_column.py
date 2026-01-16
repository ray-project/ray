import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_add_column_to_pandas(ray_start_regular_shared):
    # Refer to issue https://github.com/ray-project/ray/issues/51758
    ds = ray.data.from_pandas(
        pd.DataFrame({"a": list(range(20))}), override_num_blocks=2
    )

    ds = ds.add_column(
        "foo1", lambda df: pd.Series([1] * len(df)), batch_format="pandas"
    )
    ds = ds.add_column(
        "foo2", lambda df: pd.DatetimeIndex([1] * len(df)), batch_format="pandas"
    )
    ds = ds.add_column(
        "foo3", lambda df: pd.DataFrame({"foo": [1] * len(df)}), batch_format="pandas"
    )
    for row in ds.iter_rows():
        assert row["foo1"] == 1 and row["foo2"] == pd.Timestamp(1) and row["foo3"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
