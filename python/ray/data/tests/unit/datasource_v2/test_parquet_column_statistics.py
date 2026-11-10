import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.formats.parquet.parquet_column_statistics import (
    column_statistics,
)


def _metadata(path):
    return pq.ParquetFile(path).metadata


def test_scalar_bounds_nulls_and_unsupported_types(tmp_path):
    path = str(tmp_path / "data.parquet")
    pq.write_table(
        pa.table(
            {
                "x": [1, 2, 3, 4, 5, None],
                "ts": [
                    datetime.datetime(2026, 1, 1) + datetime.timedelta(days=i)
                    for i in range(6)
                ],
                "s": ["a", "b", "c", "d", "e", "f"],
                "f": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "all_null": pa.array([None] * 6, pa.int64()),
                "b": [True, False, True, False, True, False],
                "l": [[1, 2], [3], [4, 5], [6], [7], [8, 9]],
            }
        ),
        path,
        row_group_size=3,
    )

    metadata = _metadata(path)

    x = column_statistics(metadata, "x")
    assert x.exact
    assert (x.num_rows, x.null_count, x.min, x.max) == (6, 1, 1, 5)

    b = column_statistics(metadata, "b")
    assert b.exact
    assert (b.min, b.max) == (False, True)

    assert not column_statistics(metadata, "l").exact

    ts = column_statistics(metadata, "ts")
    assert ts.exact
    assert ts.min == datetime.datetime(2026, 1, 1)
    assert ts.max == datetime.datetime(2026, 1, 6)

    assert not column_statistics(metadata, "s").exact
    assert not column_statistics(metadata, "f").exact
    assert not column_statistics(metadata, "missing").exact

    all_null = column_statistics(metadata, "all_null")
    assert all_null.exact
    assert (all_null.null_count, all_null.min, all_null.max) == (6, None, None)


def test_multi_row_group_fold_and_empty_file(tmp_path):
    path = str(tmp_path / "data.parquet")
    pq.write_table(
        pa.table({"x": [None, None, 3, 1, 5, 4]}),
        path,
        row_group_size=2,
    )

    x = column_statistics(_metadata(path), "x")
    assert x.exact
    assert (x.num_rows, x.null_count, x.min, x.max) == (6, 2, 1, 5)

    empty_path = str(tmp_path / "empty.parquet")
    pq.write_table(pa.table({"x": pa.array([], pa.int64())}), empty_path)
    empty = column_statistics(_metadata(empty_path), "x")
    assert empty.exact
    assert (empty.num_rows, empty.null_count, empty.min, empty.max) == (
        0,
        0,
        None,
        None,
    )


def test_missing_statistics_and_nested_columns_are_not_exact(tmp_path):
    no_stats_path = str(tmp_path / "no_stats.parquet")
    pq.write_table(
        pa.table({"x": [1, 2, 3]}),
        no_stats_path,
        write_statistics=False,
    )
    assert not column_statistics(_metadata(no_stats_path), "x").exact

    nested_path = str(tmp_path / "nested.parquet")
    pq.write_table(
        pa.table({"outer": [{"a": 1}, {"a": 2}]}),
        nested_path,
    )
    assert not column_statistics(_metadata(nested_path), "outer").exact
