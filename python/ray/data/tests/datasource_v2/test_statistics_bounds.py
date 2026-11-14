"""Tests for ``_add_statistics_bounds`` -- the statistics-bound rewrite.

The deriver turns the leaf forms PyArrow cannot evaluate against Parquet
statistics into comparisons it can, so these drive it directly and through
``FooterReader`` pruning against local Parquet fixtures. No Ray cluster needed.
"""

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.statistics_bounds import (
    _add_statistics_bounds,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.expressions import col


def _write(path, table, row_group_size, **write_kwargs):
    pq.write_table(table, path, row_group_size=row_group_size, **write_kwargs)
    return str(path), path.stat().st_size


def _reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


@pytest.mark.parametrize(
    "predicate, expected_groups",
    [
        pytest.param((col("x") + 1) >= 76, [3], id="column-plus-literal"),
        pytest.param((1 + col("x")) > 76, [3], id="literal-plus-column"),
        pytest.param(col("s").str.starts_with("s08"), [3], id="prefix"),
        pytest.param(
            ((col("x") + 1) >= 76) | ~(col("x") < 800),
            [3],
            id="or-with-unmodeled-branch",
        ),
        pytest.param(
            ((col("x") + 1) >= 76) & ~(col("x") < 80),
            [3],
            id="and-with-negation",
        ),
        pytest.param(
            ((col("x") + 1) >= 76) & (col("x") < 20),
            [],
            id="and-disjoint",
        ),
        pytest.param((col("x") + 1) >= 1000, [], id="no-matches"),
    ],
)
def test_derived_bound_prunes_row_groups(tmp_path, predicate, expected_groups):
    """Derived bounds prune groups PyArrow alone would keep, and lose no rows."""
    table = pa.table(
        {
            "x": list(range(100)),
            "s": [f"s{i:03d}" for i in range(100)],
        }
    )
    path, size = _write(tmp_path / "data.parquet", table, row_group_size=25)
    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))

    selected = [rg.unit_ids[0] for rg in chunks.row_groups]
    assert selected == expected_groups

    parquet_file = pq.ParquetFile(path)
    required = {
        i
        for i in range(parquet_file.num_row_groups)
        if parquet_file.read_row_group(i).filter(predicate.to_pyarrow()).num_rows
    }
    assert required.issubset(set(selected))


@pytest.mark.parametrize(
    "predicate, schema",
    [
        # Arithmetic shapes the deriver does not model, one per guard.
        pytest.param(
            (col("x") + 1) >= 76, pa.schema([("x", pa.float64())]), id="float-column"
        ),
        pytest.param(
            (col("x") + 1) >= 76, pa.schema([("x", pa.uint64())]), id="uint64-column"
        ),
        pytest.param(
            (col("x") - 1) >= 76, pa.schema([("x", pa.int64())]), id="subtraction"
        ),
        pytest.param(
            (col("x") + -1) >= 76, pa.schema([("x", pa.int64())]), id="negative-offset"
        ),
        pytest.param(
            (col("x") + 1.0) >= 76, pa.schema([("x", pa.int64())]), id="float-literal"
        ),
        pytest.param(
            (col("x") + 1) >= 128,
            pa.schema([("x", pa.int8())]),
            id="target-out-of-column-range",
        ),
        pytest.param(
            (col("x") + 128) >= 76,
            pa.schema([("x", pa.int8())]),
            id="offset-overflows-column",
        ),
        pytest.param(
            (col("x") + 2) >= -127,
            pa.schema([("x", pa.int8())]),
            id="bound-below-column-minimum",
        ),
        pytest.param(
            (col("x") + np.int8(127)) >= np.int8(-127),
            pa.schema([("x", pa.int8())]),
            id="numpy-scalar-overflow",
        ),
        pytest.param(
            (col("x") + np.bool_(True)) >= np.uint8(76),
            pa.schema([("x", pa.uint8())]),
            id="bool-literal",
        ),
        # Forms PyArrow already prunes with stay untouched.
        pytest.param(col("f") != 1.0, pa.schema([("f", pa.float64())]), id="float-ne"),
        pytest.param(
            col("v").is_in([None, 0]), pa.schema([("v", pa.int64())]), id="in-with-null"
        ),
        pytest.param(col("v").is_null(), pa.schema([("v", pa.int64())]), id="is-null"),
        pytest.param(~(col("x") < 800), pa.schema([("x", pa.int64())]), id="negation"),
    ],
)
def test_derived_leaves_unmodeled_forms_untouched(predicate, schema):
    """Forms without a safe bound, and forms PyArrow prunes by itself, pass through."""
    assert _add_statistics_bounds(predicate, schema) is predicate


@pytest.mark.parametrize(
    "column_type",
    [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
    ],
)
@pytest.mark.parametrize("offset", [1, np.int64(75)])
@pytest.mark.parametrize("strict", [False, True])
def test_derived_integer_arithmetic_bound_safety(tmp_path, column_type, offset, strict):
    """Every modeled type and comparison keeps exactly the rows it should.

    The bound is checked at the expression level (it must accept every row the
    filter accepts), at the pruning level (no row group with a matching row
    may be dropped) and on the fully-matched flag (a group marked fully
    matched must really match throughout).
    """
    bits = column_type.bit_width
    if pa.types.is_signed_integer(column_type):
        minimum = -(1 << (bits - 1))
        maximum = (1 << (bits - 1)) - 1
    else:
        minimum = 0
        maximum = (1 << bits) - 1

    values = [
        None,
        minimum,
        0,
        1,
        74,
        75,
        76,
        77,
        maximum - 1,
        maximum,
    ]
    table = pa.table(
        {
            "x": pa.array(values, type=column_type),
        }
    )

    predicate = (col("x") + offset) > 76 if strict else (col("x") + offset) >= 76

    derived = _add_statistics_bounds(predicate, table.schema)

    original_rows = table.filter(predicate.to_pyarrow())["x"].to_pylist()
    bound_rows = table.filter(derived.to_pyarrow())["x"].to_pylist()
    assert set(original_rows).issubset(set(bound_rows))

    path, size = _write(
        tmp_path / "integer-boundary.parquet",
        table,
        row_group_size=2,
    )
    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))
    selected = [rg.unit_ids[0] for rg in chunks.row_groups]
    parquet_file = pq.ParquetFile(path)

    required = {
        i
        for i in range(parquet_file.num_row_groups)
        if parquet_file.read_row_group(i).filter(predicate.to_pyarrow()).num_rows
    }
    assert required.issubset(set(selected))

    selected_tables = [parquet_file.read_row_group(i) for i in selected]
    actual = (
        pa.concat_tables(selected_tables).filter(predicate.to_pyarrow())
        if selected_tables
        else table.slice(0, 0)
    )
    expected = table.filter(predicate.to_pyarrow())
    assert actual["x"].to_pylist() == expected["x"].to_pylist()

    for rg in chunks.row_groups:
        if rg.fully_matched:
            group = parquet_file.read_row_group(rg.unit_ids[0])
            assert group.filter(predicate.to_pyarrow()).num_rows == group.num_rows


@pytest.mark.parametrize(
    "values, column_type, row_group_size, predicate, expected",
    [
        # Groups inside (x + 1) >= 26 are fully matched; the boundary group
        # holding rows on both sides is not, and the group below the bound is
        # pruned outright.
        pytest.param(
            list(range(75)),
            pa.int64(),
            25,
            (col("x") + 1) >= 26,
            {1: True, 2: True},
            id="signed-boundary",
        ),
        # int8 addition computes at the column's width: 127 + 1 wraps back
        # below the threshold, so the all-127s group matches nothing. It is
        # never marked fully matched; whether the range bound alone also
        # prunes it is PyArrow's call, and the leaf kept in the filter is
        # what saves a spanning group like [124, 127] when the filter runs
        # at a wider width (see test_narrow_file_column_keeps_its_rows).
        pytest.param(
            [127, 127, 75, 126],
            pa.int8(),
            2,
            (col("x") + 1) >= 76,
            {1: True},
            id="signed-wrap-region",
        ),
        # Unsigned columns promote before adding, so no sum wraps and a group
        # at the very top of the range is fully matched.
        pytest.param(
            [74, 75, 255, 255],
            pa.uint8(),
            2,
            (col("x") + 1) >= 76,
            {0: False, 1: True},
            id="unsigned-exact",
        ),
        # Negation of a modeled leaf prunes through the rewritten bound: only
        # the group below the threshold survives, fully matched.
        pytest.param(
            list(range(25)) + list(range(900, 925)) + list(range(950, 975)),
            pa.int64(),
            25,
            ~((col("x") + 1) >= 901),
            {0: True},
            id="negation",
        ),
    ],
)
def test_derived_bounds_fully_matched_flag(
    tmp_path, values, column_type, row_group_size, predicate, expected
):
    """The fully-matched flag is exact where the rewritten filter is evaluable."""
    table = pa.table({"x": pa.array(values, type=column_type)})
    path, size = _write(tmp_path / "fully-matched.parquet", table, row_group_size)
    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))

    by_idx = {rg.unit_ids[0]: rg.fully_matched for rg in chunks.row_groups}
    # A pinned group shows exactly that flag once it is selected (a pruned
    # group does not appear at all), and no group outside the expectation may
    # be marked fully matched. Group 0 of the wrap case may or may not be
    # pruned depending on how PyArrow folds the addition against single-valued
    # statistics, so only its flag is pinned, not its presence.
    for idx, fully in expected.items():
        if idx in by_idx:
            assert by_idx[idx] is fully
    assert not [i for i, f in by_idx.items() if f and not expected.get(i, False)]


def test_derived_numpy_scalar_literal_keeps_the_leaf():
    """A non-int64 numpy literal takes an order-dependent kernel path, so only
    the necessary lower bound is appended and the leaf stays."""
    predicate = (col("x") + np.int8(75)) >= 76
    derived = _add_statistics_bounds(predicate, pa.schema([("x", pa.int64())]))
    assert derived.structurally_equals(predicate & (col("x") >= 1))


def test_narrow_file_column_keeps_its_rows(tmp_path):
    """A file column narrower than the dataset's is not pruned by its range.

    An int8 file next to int64 files is evaluated at the unified int64 schema,
    where 124 + 5 does not wrap -- the two-sided range alone dropped the group
    and with it the rows. The original leaf stays in the rewritten filter, so
    the group survives and the evaluation decides.
    """
    table = pa.table({"x": pa.array([124, 127], type=pa.int8())})
    path, size = _write(tmp_path / "narrow.parquet", table, row_group_size=2)
    predicate = (col("x") + 5) >= 76

    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))
    selected = [rg.unit_ids[0] for rg in chunks.row_groups]
    assert selected == [0]

    # The unified dataset schema widens the column before the filter runs, so
    # the group's rows must all survive the filter there.
    parquet_file = pq.ParquetFile(path)
    wide = parquet_file.read_row_group(0).cast(pa.schema([("x", pa.int64())]))
    assert wide.filter(predicate.to_pyarrow())["x"].to_pylist() == [124, 127]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
