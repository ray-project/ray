import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.http import HTTPFileSystem

import ray
from ray.data import Schema
from ray.data._internal.logical.operators import ReadFiles
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.data.tests.mock_http_server import (
    data_file,
    no_ranges_file,
    no_ranges_num_lines,
)
from ray.tests.conftest import *  # noqa


def df_to_csv(dataframe, path, **kwargs):
    dataframe.to_csv(path, **kwargs)


@pytest.fixture
def csv_v2_small_chunks(restore_data_context, monkeypatch):
    """Use the V2 reader with 256-byte chunks and 1 KiB read tasks."""
    restore_data_context.use_datasource_v2 = True
    restore_data_context.target_min_block_size = 1
    restore_data_context.target_max_block_size = 1024
    monkeypatch.setenv("RAY_DATA_LINE_CHUNK_BYTES", "256")


def _write_ids_csv(path, num_rows: int) -> str:
    path.write_text("id,name\n" + "".join(f"{i},n{i}\n" for i in range(num_rows)))
    return str(path)


def test_csv_read(
    ray_start_regular_shared,
    tmp_path,
    target_max_block_size_infinite_or_default,
    datasource_v2,
):
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.csv")
    df1.to_csv(path1, index=False)
    ds = ray.data.read_csv(path1, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(df1.astype(dsdf.dtypes.to_dict()), dsdf)
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.schema() == Schema(pa.schema([("one", pa.int64()), ("two", pa.string())]))

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.csv")
    df2.to_csv(path2, index=False)
    ds = ray.data.read_csv([path1, path2], override_num_blocks=2, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    df = pd.concat([df1, df2], ignore_index=True)
    pd.testing.assert_frame_equal(df.astype(dsdf.dtypes.to_dict()), dsdf)
    # Test metadata ops.
    for entry in ds._execute().blocks:
        assert (
            # pyrefly: ignore[no-matching-overload]
            BlockAccessor.for_block(ray.get(entry.ref)).size_bytes()
            == entry.metadata.size_bytes
        )

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
    pd.testing.assert_frame_equal(df.astype(dsdf.dtypes.to_dict()), dsdf)


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
    datasource_v2,
):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_csv(tmp_path)

    ds2 = ray.data.read_csv(tmp_path)
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for entry in ds2._execute().blocks:
        # pyrefly: ignore[no-matching-overload]
        assert (
            BlockAccessor.for_block(ray.get(entry.ref)).size_bytes()
            == entry.metadata.size_bytes
        )


def test_csv_read_invalid_format(ray_start_regular_shared, tmp_path, datasource_v2):
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


def test_csv_read_no_header(ray_start_regular_shared, tmp_path, datasource_v2):
    from pyarrow import csv

    file_path = os.path.join(tmp_path, "test.csv")
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df.to_csv(file_path, index=False, header=False)
    ds = ray.data.read_csv(
        file_path,
        read_options=csv.ReadOptions(column_names=["one", "two"]),
    )
    out_df = ds.to_pandas()
    pd.testing.assert_frame_equal(df.astype(out_df.dtypes.to_dict()), out_df)


def test_csv_read_with_column_type_specified(
    ray_start_regular_shared, tmp_path, datasource_v2
):
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
    actual_df = ds.to_pandas()
    pd.testing.assert_frame_equal(
        expected_df.astype(actual_df.dtypes.to_dict()), actual_df
    )


def test_csv_invalid_file_handler(
    ray_start_regular_shared,
    tmp_path,
    target_max_block_size_infinite_or_default,
    datasource_v2,
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


def test_read_example_data(ray_start_regular_shared, tmp_path, datasource_v2):
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


def test_read_csv_v2_splits_large_files(
    ray_start_regular_shared, tmp_path, csv_v2_small_chunks
):
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    path = _write_ids_csv(partition_dir / "data.csv", 1000)

    ds = ray.data.read_csv(str(tmp_path), include_paths=True).materialize()
    rows = ds.take_all()

    assert ds.num_blocks() > 1
    assert sorted(row["id"] for row in rows) == list(range(1000))
    assert {row["year"] for row in rows} == {"2024"}
    assert {row["path"] for row in rows} == {path}


def test_read_csv_v2_preserve_order_keeps_chunks_in_order(
    ray_start_regular_shared, tmp_path, csv_v2_small_chunks, restore_data_context
):
    restore_data_context.execution_options.preserve_order = True
    first = _write_ids_csv(tmp_path / "a.csv", 500)
    second = _write_ids_csv(tmp_path / "b.csv", 500)

    rows = ray.data.read_csv([first, second], partitioning=None).take_all()

    assert [row["id"] for row in rows] == list(range(500)) * 2


def test_read_csv_v2_reads_a_split_file_with_another_column_order(
    ray_start_regular_shared, tmp_path, csv_v2_small_chunks
):
    (tmp_path / "a.csv").write_text("id,name\n1,x\n")
    (tmp_path / "b.csv").write_text(
        "name,id\n" + "".join(f"n{i},{i}\n" for i in range(200))
    )

    rows = ray.data.read_csv(str(tmp_path), partitioning=None).take_all()

    assert {(row["id"], row["name"]) for row in rows} == {(1, "x")} | {
        (i, f"n{i}") for i in range(200)
    }


@pytest.mark.parametrize(
    "url, num_lines",
    [(data_file, 1000), (no_ranges_file, no_ranges_num_lines)],
    ids=["no-size", "no-ranges"],
)
@pytest.mark.parametrize("filesystem", [HTTPFileSystem(), None], ids=["fsspec", "auto"])
@pytest.mark.parametrize("num_paths", [1, 2])
def test_read_csv_http_file_system(
    ray_start_regular_shared,
    http_server,
    datasource_v2,
    url,
    num_lines,
    filesystem,
    num_paths,
):
    """``no-size``: the server sends no ``Content-Length``, so the filesystem
    reports no size for the file and V2 must read it whole, not skip it.
    ``no-ranges``: the server sends the size but ignores range requests."""
    ds = ray.data.read_csv([url] * num_paths, filesystem=filesystem)

    assert isinstance(ds._logical_plan.dag, ReadFiles) == datasource_v2
    # The first line is the header.
    assert ds.count() == (num_lines - 1) * num_paths


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))


def test_csv_input_files_v1(ray_start_regular_shared, tmp_path, restore_data_context):
    # V2 lists files at execution time, so it has no input files up front.
    restore_data_context.use_datasource_v2 = False
    path = os.path.join(tmp_path, "test1.csv")
    pd.DataFrame({"one": [1, 2, 3]}).to_csv(path, index=False)
    assert ray.data.read_csv(path).input_files() == [_unwrap_protocol(path)]
