import math
from dataclasses import replace

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.listing_pruning import (
    substitute_partition_values,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
    FooterReader,
    physical_schema_decline_reason,
)
from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
    ListingPruningHint,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col

TYPES = (("p", "int64"), ("x", "int64"), ("s", "string"), ("b", "bool"))


def _literal(expr):
    from ray.data.expressions import LiteralExpr

    assert isinstance(expr, LiteralExpr)
    return expr


def _binary(expr):
    from ray.data.expressions import BinaryExpr

    assert isinstance(expr, BinaryExpr)
    return expr


@pytest.mark.parametrize(
    "values,field,data",
    [
        ([1, 3], "x", [None, 0, 1, 2, 3]),
        (["a", "c"], "s", [None, "", "a", "b", "c"]),
        ([True], "b", [None, False, True, False, True]),
    ],
)
@pytest.mark.parametrize("negate", [False, True])
def test_in_native_nullable_semantics(values, field, data, negate):
    expr = (col("p") == 9) | col(field).is_in(values)
    if negate:
        expr = ~expr
    result = substitute_partition_values(expr, {"p": 0}, TYPES)
    assert result is not None
    table = pa.table({"p": [0] * 5, field: data})
    assert (
        ds.dataset(table)
        .to_table(filter=expr.to_pyarrow())
        .equals(ds.dataset(table).to_table(filter=result.to_pyarrow()))
    )


@pytest.mark.parametrize(
    "values",
    [
        [],
        [None],
        [1, None],
        [1, True],
        [1, "1"],
        [1.0],
        [float("nan")],
        [2**63],
        list(range(257)),
    ],
)
def test_in_invalid_set_declines(values):
    assert (
        substitute_partition_values(
            (col("p") == 9) | col("x").is_in(values), {"p": 0}, TYPES
        )
        is None
    )


def test_in_snapshot_detaches_and_restore_has_fresh_list():
    values = [1, 2]
    scanner = ParquetScanner(
        schema=pa.schema([("p", pa.int64()), ("x", pa.int64())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, field_names=["p"], field_types={"p": int}
        ),
    )
    expr = (col("p") == 9) | col("x").is_in(values)
    context = scanner.listing_pruning_context()
    assert context is not None
    hint = ListingPruningHint.from_residual(context, expr)
    assert hint is not None
    _literal(_binary(_binary(expr).right).right).value.append(3)
    first = hint.residual
    _literal(_binary(_binary(first).right).right).value.append(4)
    assert _literal(_binary(_binary(hint.residual).right).right).value == [1, 2]


@pytest.mark.parametrize(
    "expr", [col("x") >= col("p"), col("p") <= col("x"), ~(col("p") > col("x"))]
)
def test_column_comparison_after_typed_substitution(expr):
    result = substitute_partition_values(expr, {"p": 3}, TYPES)
    assert result is not None
    table = pa.table({"p": [3] * 5, "x": [None, 1, 2, 3, 4]})
    assert (
        ds.dataset(table)
        .to_table(filter=expr.to_pyarrow())
        .equals(ds.dataset(table).to_table(filter=result.to_pyarrow()))
    )


@pytest.mark.parametrize(
    "expr", [col("x") == col("s"), col("x") == col("x"), col("x") >= col("p")]
)
def test_column_comparison_type_and_unresolved_guard(expr):
    assert substitute_partition_values(expr, {"b": True}, TYPES) is None


@pytest.mark.parametrize(
    "expr",
    [
        col("f").is_null(),
        col("f") == col("f"),
        col("f").is_in([1]),
        (col("p") == 9) | col("f").is_not_null(),
    ],
)
def test_float_expression_stays_unsupported(expr):
    assert (
        substitute_partition_values(expr, {"p": 0}, TYPES + (("f", "float64"),)) is None
    )


@pytest.mark.parametrize(
    "case", ["in", "column", "float_payload", "in_all_null", "in_no_statistics"]
)
def test_actual_footer_selected_groups_and_rows(tmp_path, case):
    directory = tmp_path / "p=30"
    directory.mkdir()
    path = directory / "data.parquet"
    x = [None] * 40 if case == "in_all_null" else list(range(40))
    table = pa.table({"x": pa.array(x, type=pa.int64())})
    if case == "float_payload":
        table = table.append_column(
            "f",
            pa.array([None, float("nan"), float("inf"), -0.0] * 10, type=pa.float64()),
        )
    pq.write_table(
        table, path, row_group_size=10, write_statistics=case != "in_no_statistics"
    )
    schema = table.schema.append(pa.field("p", pa.int64()))
    scanner = ParquetScanner(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            base_dir=str(tmp_path),
            field_names=["p"],
            field_types={"p": int},
        ),
    )
    context = scanner.listing_pruning_context()
    assert context is not None
    expr = (
        col("x") >= col("p")
        if case == "column"
        else (col("p") == 99) | (col("x") >= 30)
        if case == "float_payload"
        else (col("p") == 99) | col("x").is_in([30, 31])
    )
    hint = ListingPruningHint.from_residual(context, expr)
    assert hint is not None
    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo

    def selected(h):
        r = FooterReader(fs.LocalFileSystem(), io_concurrency=1, listing_hint=h)
        try:
            return [
                i
                for g in r._read_and_chunk(
                    FileInfo(str(path), path.stat().st_size)
                ).row_groups
                for i in g.unit_ids
            ]
        finally:
            r.pool.shutdown(wait=True)

    baseline = selected(None)
    narrowed = selected(hint)
    assert baseline == [0, 1, 2, 3]
    substituted = substitute_partition_values(
        expr,
        {"p": 30},
        tuple(
            (f.name, "float64" if f.type == pa.float64() else "int64") for f in schema
        ),
    )
    fragment = ds.ParquetFileFormat().make_fragment(
        str(path), filesystem=fs.LocalFileSystem()
    )
    assert substituted is not None
    native = [
        part.row_groups[0].id
        for part in fragment.split_by_row_group(
            substituted.to_pyarrow(), schema=context.file_schema
        )
    ]
    assert narrowed == sorted(set(native))
    expected = (
        [0, 1, 2, 3]
        if case == "in_no_statistics" or (case == "in" and pa.__version__ == "17.0.0")
        else []
        if case == "in_all_null"
        else [3]
    )
    assert narrowed == expected, (pa.__version__, case, native, narrowed)

    def result(ids):
        t = (
            pq.ParquetFile(path)
            .read_row_groups(ids)
            .append_column("p", pa.array([30] * (10 * len(ids)), type=pa.int64()))
        )
        return ds.dataset(t).to_table(filter=expr.to_pyarrow())

    def normalized(t):
        return [
            {
                k: ("NaN" if isinstance(v, float) and math.isnan(v) else v)
                for k, v in row.items()
            }
            for row in t.to_pylist()
        ]

    assert normalized(result(baseline)) == normalized(result(narrowed))
    assert result(baseline).schema.equals(result(narrowed).schema, check_metadata=True)
    assert (
        physical_schema_decline_reason(
            pq.read_metadata(path), context.file_schema_signature
        )
        is None
    )


def test_float_payload_type_and_metadata_guards(tmp_path):
    for dtype in [pa.float32(), pa.float64()]:
        schema = pa.schema([("x", pa.int64()), ("f", dtype), ("p", pa.int64())])
        scanner = ParquetScanner(
            schema=schema,
            partitioning=Partitioning(
                PartitionStyle.HIVE, field_names=["p"], field_types={"p": int}
            ),
        )
        assert (scanner.listing_pruning_context() is not None) == (
            dtype == pa.float64()
        )
    schema = pa.schema(
        [
            ("x", pa.int64()),
            pa.field("f", pa.float64(), metadata={b"unit": b"v"}),
            ("p", pa.int64()),
        ]
    )
    assert replace(scanner, schema=schema).listing_pruning_context() is None
    assert (
        replace(
            scanner, schema=pa.schema([("x", pa.int64()), ("p", pa.float64())])
        ).listing_pruning_context()
        is None
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
