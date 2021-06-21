import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_basic(ray_start_regular_shared):
    ds = ray.experimental.data.range(5)
    assert ds.map(lambda x: x + 1).take() == [1, 2, 3, 4, 5]


def test_parquet(ray_start_regular_shared, tmp_path):
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    table = pa.Table.from_pandas(df1)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    table = pa.Table.from_pandas(df2)
    pq.write_table(table, os.path.join(tmp_path, "test2.parquet"))

    ds = ray.experimental.data.read_parquet(tmp_path)
    assert sorted(ds.take()) == [[4, 'e'], [4, 'e'], [5, 'f'], [5, 'f'],
                                 [6, 'g'], [6, 'g']]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
