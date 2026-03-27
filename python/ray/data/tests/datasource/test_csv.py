import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import Version

import ray
from ray.data import Schema
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


def test_csv_read(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.csv")
    df1.to_csv(path1, index=False)
    ds = ray.data.read_csv(path1, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df1.equals(dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.csv")
    df2.to_csv(path2, index=False)
    ds = ray.data.read_csv([path1, path2], override_num_blocks=2, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    df = pd.concat([df1, df2], ignore_index=True)
    assert df.equals(dsdf)
    # Test metadata ops.
    for block, meta in ds._plan.execute().blocks:
        BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, override_num_blocks=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.csv")
    df3.to_csv(path3, index=False)
    ds = ray.data.read_csv(
        [path1, path2, path3],
        override_num_blocks=2,
        partitioning=None,
    )
    df = pd.concat([df1, df2, df3], ignore_index=True)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert df.equals(dsdf)


def test_csv_write(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    input_df = pd.DataFrame({"id": [0]})
    ds = ray.data.from_blocks([input_df])

    ds.write_csv(tmp_path)

    output_df = pd.concat(
        [
            pd.read_csv(os.path.join(tmp_path, filename))
            for filename in os.listdir(tmp_path)
        ]
    )
    assert rows_same(input_df, output_df)


@pytest.mark.parametrize("override_num_blocks", [None, 2])
def test_csv_roundtrip(
    ray_start_regular_shared,
    tmp_path,
    override_num_blocks,
    target_max_block_size_infinite_or_default,
):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_csv(tmp_path)

    ds2 = ray.data.read_csv(tmp_path)
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for block, meta in ds2._plan.execute().blocks:
        assert BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes


def test_csv_read_invalid_format(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    # Setup: CSV and Parquet files in the same directory.
    csv_path = os.path.join(tmp_path, "test.csv")
    df.to_csv(csv_path, index=False)

    table = pa.Table.from_pandas(df)
    parquet_path = os.path.join(tmp_path, "test.parquet")
    pq.write_table(table, parquet_path)

    # Test 1: CSV parser should fail on Parquet file.
    error_message = "Failed to read CSV file"
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(parquet_path).materialize()

    # Test 2: CSV parser should fail when directory contains non-CSV files.
    with pytest.raises(ValueError, match=error_message):
        ray.data.read_csv(tmp_path).materialize()


def test_csv_read_no_header(ray_start_regular_shared, tmp_path):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False, header=False)
    ds = ray.data.read_csv(
        file_path,
        read_options=csv.ReadOptions(column_names=["one", "two"]),
    )
    out_df = ds.to_pandas()
    assert df.equals(out_df)


def test_csv_read_with_column_type_specified(ray_start_regular_shared, tmp_path):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3e1], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False)

    # Incorrect to parse scientific notation in int64 as PyArrow represents
    # it as double.
    with pytest.raises(ValueError):
        ray.data.read_csv(
            file_path,
            convert_options=csv.ConvertOptions(
                column_types={"one": "int64", "two": "string"}
            ),
        ).schema()

    # Parsing scientific notation in double should work.
    ds = ray.data.read_csv(
        file_path,
        convert_options=csv.ConvertOptions(
            column_types={"one": "float64", "two": "string"}
        ),
    )
    expected_df = pd.DataFrame({"one": [1.0, 2.0, 30.0], "two": ["a", "b", "c"]})
    assert ds.to_pandas().equals(expected_df)


@pytest.mark.skipif(
    Version(pa.__version__) < Version("7.0.0"),
    reason="invalid_row_handler was added in pyarrow 7.0.0",
)
def test_csv_invalid_file_handler(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    from pyarrow import csv

    invalid_txt = "f1,f2\n2,3\nx\n4,5"
    invalid_file = os.path.join(tmp_path, "invalid.csv")
    with open(invalid_file, "wt") as f:
        f.write(invalid_txt)

    ray.data.read_csv(
        invalid_file,
        parse_options=csv.ParseOptions(
            delimiter=",", invalid_row_handler=lambda i: "skip"
        ),
    )


def test_read_example_data(ray_start_regular_shared, tmp_path):
    ds = ray.data.read_csv("example://iris.csv")
    assert ds.count() == 150
    assert ds.take(1) == [
        {
            "sepal.length": 5.1,
            "sepal.width": 3.5,
            "petal.length": 1.4,
            "petal.width": 0.2,
            "variety": "Setosa",
        }
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
