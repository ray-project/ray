import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.formats.parquet.parquet_column_statistics import (
    column_statistics,
)
from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatisticsState,
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
    assert x.state is ColumnStatisticsState.BOUNDED
    assert (x.num_rows, x.null_count, x.min, x.max) == (6, 1, 1, 5)

    b = column_statistics(metadata, "b")
    assert b.state is ColumnStatisticsState.BOUNDED
    assert (b.min, b.max) == (False, True)

    assert column_statistics(metadata, "l").state is ColumnStatisticsState.UNKNOWN

    ts = column_statistics(metadata, "ts")
    assert ts.state is ColumnStatisticsState.BOUNDED
    assert ts.min == datetime.datetime(2026, 1, 1)
    assert ts.max == datetime.datetime(2026, 1, 6)

    assert column_statistics(metadata, "s").state is ColumnStatisticsState.UNKNOWN
    assert column_statistics(metadata, "f").state is ColumnStatisticsState.UNKNOWN
    assert column_statistics(metadata, "missing").state is ColumnStatisticsState.UNKNOWN

    all_null = column_statistics(metadata, "all_null")
    assert all_null.state is ColumnStatisticsState.ALL_NULL
    assert (all_null.null_count, all_null.min, all_null.max) == (6, None, None)


def test_multi_row_group_fold_and_empty_file(tmp_path):
    path = str(tmp_path / "data.parquet")
    pq.write_table(
        pa.table({"x": [None, None, 3, 1, 5, 4]}),
        path,
        row_group_size=2,
    )

    x = column_statistics(_metadata(path), "x")
    assert x.state is ColumnStatisticsState.BOUNDED
    assert (x.num_rows, x.null_count, x.min, x.max) == (6, 2, 1, 5)

    empty_path = str(tmp_path / "empty.parquet")
    pq.write_table(pa.table({"x": pa.array([], pa.int64())}), empty_path)
    empty = column_statistics(_metadata(empty_path), "x")
    assert empty.state is ColumnStatisticsState.EMPTY
    assert (empty.num_rows, empty.null_count, empty.min, empty.max) == (
        0,
        0,
        None,
        None,
    )


def test_missing_statistics_and_nested_columns_are_unknown(tmp_path):
    no_stats_path = str(tmp_path / "no_stats.parquet")
    pq.write_table(
        pa.table({"x": [1, 2, 3]}),
        no_stats_path,
        write_statistics=False,
    )
    assert (
        column_statistics(_metadata(no_stats_path), "x").state
        is ColumnStatisticsState.UNKNOWN
    )

    nested_path = str(tmp_path / "nested.parquet")
    pq.write_table(
        pa.table({"outer": [{"a": 1}, {"a": 2}]}),
        nested_path,
    )
    assert (
        column_statistics(_metadata(nested_path), "outer").state
        is ColumnStatisticsState.UNKNOWN
    )


@pytest.mark.parametrize(
    "column_type,values",
    [
        (pa.uint32(), [0, 2**31, 5]),
        (pa.uint64(), [0, 2**63, 5]),
    ],
    ids=["uint32", "uint64"],
)
def test_unsigned_columns_are_unknown(tmp_path, column_type, values):
    """Unsigned bounds can be signed-comparison values, so never trust them.

    The physical type is INT32/INT64, which would otherwise pass the allowlist,
    but a row group spanning the sign boundary can carry a bound that is
    neither the true minimum nor the true maximum.
    """
    path = str(tmp_path / "unsigned.parquet")
    pq.write_table(
        pa.table({"u": pa.array(values, column_type)}),
        path,
        row_group_size=2,
    )

    statistics = column_statistics(_metadata(path), "u")

    assert statistics.state is ColumnStatisticsState.UNKNOWN


def test_exact_statistics_type_matches_the_physical_allowlist():
    """The planning hint accepts exactly the scalar types Parquet bounds exactly.

    Everything else is declined before the footer is read: floats (NaN is
    excluded from bounds), strings and binary (writers may truncate bounds),
    and any nested, dictionary, or interval type.
    """
    from ray.data._internal.datasource_v2.formats.parquet.parquet_column_statistics import (
        is_exact_statistics_type,
    )

    accepted = [
        pa.int8(),
        pa.int32(),
        pa.int64(),
        pa.bool_(),
        pa.date32(),
        pa.time32("s"),
        pa.time64("us"),
        pa.timestamp("ms"),
        pa.timestamp("us", tz="UTC"),
    ]
    declined = [
        # Unsigned physical types are signed on disk, and legacy min/max
        # fields carry signed-comparison bounds.
        pa.uint8(),
        pa.uint32(),
        pa.uint64(),
        pa.float32(),
        pa.float64(),
        pa.string(),
        pa.large_string(),
        pa.binary(),
        pa.decimal128(10, 2),
        pa.duration("ms"),
        pa.month_day_nano_interval(),
        pa.struct([("a", pa.int64())]),
        pa.list_(pa.int64()),
        pa.dictionary(pa.int8(), pa.string()),
    ]

    for column_type in accepted:
        assert is_exact_statistics_type(column_type), column_type
    for column_type in declined:
        assert not is_exact_statistics_type(column_type), column_type


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
