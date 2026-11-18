"""Tests for ``_add_statistics_bounds`` -- the statistics-bound rewrite.

The rewrite turns the leaf forms PyArrow cannot evaluate against Parquet
statistics into comparisons it can, so these drive it directly and through
``FooterReader`` pruning against local Parquet fixtures; the narrow-column
case reads end to end through ``read_parquet`` on a local cluster.
"""

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

import ray
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
        # Arithmetic shapes the rewrite does not model, one per guard.
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
            (col("x") + np.bool_(True)) >= np.uint8(76),
            pa.schema([("x", pa.uint8())]),
            id="bool-literal",
        ),
        # Forms PyArrow already prunes with stay untouched.
        pytest.param(col("f") != 1.0, pa.schema([("f", pa.float64())]), id="float-ne"),
        pytest.param(~(col("x") < 800), pa.schema([("x", pa.int64())]), id="negation"),
    ],
)
def test_derived_leaves_unmodeled_forms_untouched(predicate, schema):
    """Forms without a safe bound, and forms PyArrow prunes by itself, pass through."""
    assert _add_statistics_bounds(predicate, schema) is predicate


@pytest.mark.parametrize(
    "column_type, offset, strict",
    [
        pytest.param(pa.int8(), 1, False, id="int8-literal-ge"),
        pytest.param(pa.uint8(), 1, True, id="uint8-literal-gt"),
        pytest.param(pa.int8(), np.int8(75), True, id="int8-numpy-gt"),
        pytest.param(pa.uint8(), np.int8(75), False, id="uint8-numpy-ge"),
    ],
)
def test_derived_integer_arithmetic_bound_safety(tmp_path, column_type, offset, strict):
    """Each modeled shape keeps exactly the rows it should.

    The four shapes span the branch outcomes: signed and unsigned columns,
    Python-int and NumPy literals, and both comparisons. The bound is checked
    at the expression level (it must accept exactly the rows the filter
    accepts) and on the read level (the chunked rows match the filter's, so
    no group holding a matching row can have been dropped), and a fully
    matched group must really match throughout.
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
    assert set(original_rows) == set(bound_rows)

    path, size = _write(
        tmp_path / "integer-boundary.parquet",
        table,
        row_group_size=2,
    )
    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))
    selected = [rg.unit_ids[0] for rg in chunks.row_groups]
    parquet_file = pq.ParquetFile(path)

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


def test_derived_prefix_boundary_bounds(tmp_path):
    """A prefix ending at U+D7FF bounds across the surrogate block.

    U+D7FF has no single-unit successor: the upper end jumps the surrogate
    block to U+E000, and the two-sided range still prunes.
    """
    values = ["zz", "\ud7ff", "\ud7ffz", "\ue000", chr(0x10FFFF), chr(0x10FFFF) + "a"]
    table = pa.table({"s": values})
    path, size = _write(tmp_path / "prefix-boundary.parquet", table, row_group_size=2)
    predicate = col("s").str.starts_with("\ud7ff")

    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))
    selected = [rg.unit_ids[0] for rg in chunks.row_groups]
    assert selected == [0, 1]


def test_prefix_without_successor_keeps_its_leaf(tmp_path):
    """A prefix ending at U+10FFFF keeps its leaf through the negation.

    "b" sorts above a prefix ending in U+10FFFF but does not start with it,
    so the lower bound alone is not equivalent to the leaf. Were the branch
    to drop the leaf, ``~lower`` would prune every surviving group of the
    negated filter -- the "zz" and "b" rows would vanish.
    """
    values = ["zz", "a" + chr(0x10FFFF), "b"]
    table = pa.table({"s": values})
    path, size = _write(tmp_path / "prefix-max.parquet", table, row_group_size=1)
    predicate = ~(col("s").str.starts_with("a" + chr(0x10FFFF)))

    chunks = _reader(filter_expr=predicate)._read_and_chunk(FileInfo(path, size))
    by_idx = {rg.unit_ids[0]: rg.fully_matched for rg in chunks.row_groups}
    assert by_idx == {0: True, 2: True}


def test_narrow_file_column_keeps_its_rows(tmp_path, ray_start_regular_shared):
    """A file column narrower than the dataset's is not pruned by its range.

    An int8 file next to an int64 file is evaluated at the unified int64
    schema, where 124 + 5 does not wrap -- the two-sided range alone dropped
    the int8 file's rows. The original leaf stays in the rewritten filter, so
    the group survives and the evaluation decides.
    """
    pq.write_table(
        pa.table({"x": pa.array([124, 127], type=pa.int8())}),
        str(tmp_path / "narrow.parquet"),
        row_group_size=2,
    )
    pq.write_table(
        pa.table({"x": pa.array([0, 1], type=pa.int64())}),
        str(tmp_path / "wide.parquet"),
    )

    ds = ray.data.read_parquet(str(tmp_path))
    rows = ds.filter(expr=(col("x") + 5) >= 76).take_all()
    assert sorted(row["x"] for row in rows) == [124, 127]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
