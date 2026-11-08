"""Exact-width signed integer admission, native footers and fail-closed controls."""


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
    logical_schema_signature,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import BinaryExpr, LiteralExpr, Operation, col

SIGNED = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
OPS = [
    Operation.EQ,
    Operation.NE,
    Operation.LT,
    Operation.LE,
    Operation.GT,
    Operation.GE,
]


def _bounds(dtype):
    return -(2 ** (dtype.bit_width - 1)), 2 ** (dtype.bit_width - 1) - 1


def _scanner(schema, root=""):
    return ParquetScanner(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            base_dir=str(root),
            field_names=["p"],
            field_types={"p": int},
        ),
    )


@pytest.mark.parametrize("dtype", SIGNED)
def test_exact_width_signatures_and_scanner(dtype):
    schema = pa.schema([("p", dtype), ("x", dtype), ("payload", dtype)])
    assert logical_schema_signature(schema) == tuple(
        (name, str(dtype)) for name in schema.names
    )
    context = _scanner(schema).listing_pruning_context()
    assert context is not None
    assert context.schema.equals(schema)
    assert context.file_schema.equals(pa.schema([("x", dtype), ("payload", dtype)]))
    assert context.file_schema_signature == (("x", str(dtype)), ("payload", str(dtype)))
    # The parser's Python type is distinct from the logical field width.
    assert context.partitioning.field_types == (("p", "int64"),)


@pytest.mark.parametrize("dtype", SIGNED)
@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("reverse", [False, True])
def test_comparison_boundaries_native_semantics(dtype, op, reverse):
    low, high = _bounds(dtype)
    table = pa.table(
        {
            "p": pa.array([0] * 6, type=dtype),
            "x": pa.array([None, low, -1, 0, 1, high], type=dtype),
        }
    )
    signature = logical_schema_signature(table.schema)
    assert signature is not None
    for value in [low, -1, 0, 1, high]:
        args = (
            (LiteralExpr(value), col("x"))
            if reverse
            else (col("x"), LiteralExpr(value))
        )
        comparison = BinaryExpr(op, *args)
        residual = (col("p") == 9) | comparison
        original = str(residual)
        substituted = substitute_partition_values(residual, {"p": 0}, signature)
        assert substituted is not None
        assert str(residual) == original
        assert (
            ds.dataset(table)
            .to_table(filter=residual.to_pyarrow())
            .equals(ds.dataset(table).to_table(filter=substituted.to_pyarrow()))
        )
    for value in [low - 1, high + 1, True, False]:
        args = (
            (LiteralExpr(value), col("x"))
            if reverse
            else (col("x"), LiteralExpr(value))
        )
        residual = (col("p") == 9) | BinaryExpr(op, *args)
        assert substitute_partition_values(residual, {"p": 0}, signature) is None


@pytest.mark.parametrize("dtype", SIGNED)
def test_partition_replacement_bounds(dtype):
    low, high = _bounds(dtype)
    signature = (("p", str(dtype)), ("x", "int64"))
    residual = (col("p") == 0) | (col("x") > 2)
    for value in [low, -1, 0, 1, high]:
        assert (
            substitute_partition_values(residual, {"p": value}, signature) is not None
        )
    for value in [low - 1, high + 1, True, False, 1.0, "1", None]:
        assert substitute_partition_values(residual, {"p": value}, signature) is None


@pytest.mark.parametrize("dtype", SIGNED[:-1])
def test_no_narrow_in_arithmetic_or_cross_width_expansion(dtype):
    signature = (("p", str(dtype)), ("x", str(dtype)), ("wide", "int64"))
    for residual in [
        (col("p") == 0) | col("x").is_in([1, 2]),
        col("p").is_in([1, 2]),
        (col("p") == 0) | ((col("x") + 1) > 2),
        col("p") < col("wide"),
        col("wide") == col("p"),
    ]:
        assert substitute_partition_values(residual, {"p": 0}, signature) is None
    # Same-width column comparison is resolved only after typed substitution.
    assert (
        substitute_partition_values(col("x") >= col("p"), {"p": 1}, signature)
        is not None
    )
    assert (
        substitute_partition_values(col("x") >= col("x"), {"p": 1}, signature) is None
    )


@pytest.mark.parametrize("dtype", SIGNED)
@pytest.mark.parametrize("store_schema", [False, True])
def test_real_physical_signed_widths(tmp_path, dtype, store_schema):
    path = tmp_path / "ints.parquet"
    pq.write_table(
        pa.table({"x": pa.array([None, -1, 0, 1], type=dtype)}),
        path,
        store_schema=store_schema,
    )
    metadata = pq.read_metadata(path)
    column = metadata.schema.column(0)
    assert column.physical_type == ("INT64" if dtype == pa.int64() else "INT32")
    assert physical_schema_decline_reason(metadata, (("x", str(dtype)),)) is None
    for other in SIGNED:
        if other != dtype:
            assert (
                physical_schema_decline_reason(metadata, (("x", str(other)),))
                is not None
            )


@pytest.mark.parametrize(
    "dtype",
    [
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.date32(),
        pa.decimal128(4, 0),
    ],
)
@pytest.mark.parametrize("store_schema", [False, True])
def test_non_signed_native_footers_decline(tmp_path, dtype, store_schema):
    assert logical_schema_signature(pa.schema([("x", dtype)])) is None
    path = tmp_path / "other.parquet"
    pq.write_table(
        pa.table({"x": pa.array([1], type=dtype)}), path, store_schema=store_schema
    )
    metadata = pq.read_metadata(path)
    for target in SIGNED:
        assert (
            physical_schema_decline_reason(metadata, (("x", str(target)),)) is not None
        )


def _selected(path, hint, q=None, excluded=frozenset()):
    reader = FooterReader(
        fs.LocalFileSystem(),
        io_concurrency=1,
        listing_hint=hint,
        filter_expr=q,
        excluded_read_unit_ids=excluded,
    )
    try:
        chunks = reader._read_and_chunk(
            FileInfo(str(path), path.stat().st_size)
        ).row_groups
        return [(i, c.fully_matched) for c in chunks for i in c.unit_ids]
    finally:
        reader.pool.shutdown(wait=True)


@pytest.mark.parametrize("dtype", SIGNED)
@pytest.mark.parametrize("position", ["partition", "filter", "payload"])
@pytest.mark.parametrize(
    "case",
    [
        "selective",
        "q",
        "all_kept",
        "all_false",
        "no_statistics",
        "all_null",
        "partial_q",
        "excluded",
    ],
)
def test_actual_footer_three_positions(tmp_path, dtype, position, case):
    partition = 99 if case == "all_kept" else 0
    path = tmp_path / f"p={partition}" / "data.parquet"
    path.parent.mkdir()
    p_type = dtype if position == "partition" else pa.int64()
    x_type = dtype if position == "filter" else pa.int64()
    table = pa.table(
        {"x": pa.array([None] * 40 if case == "all_null" else range(40), type=x_type)}
    )
    if position == "payload":
        table = table.append_column(
            "payload", pa.array([None, -1, 0, 1] * 10, type=dtype)
        )
    pq.write_table(
        table, path, row_group_size=10, write_statistics=case != "no_statistics"
    )
    schema = table.schema.append(pa.field("p", p_type))
    context = _scanner(schema, tmp_path).listing_pruning_context()
    assert context is not None
    residual = (col("p") == 99) | (col("x") >= (100 if case == "all_false" else 30))
    original = str(residual)
    q = col("x") >= 10 if case == "q" else None
    if case == "partial_q":
        q = (col("x") == 5) | (col("x") == 15) | (col("x") == 25) | (col("x") == 35)
    excluded = [f"{path}#rg0"] if case == "excluded" else []
    hint = ListingPruningHint.from_residual(context, residual)
    assert hint is not None
    baseline = _selected(path, None, q, excluded)
    narrowed = _selected(path, hint, q, excluded)
    expected = {
        "selective": [3],
        "q": [3],
        "all_kept": [0, 1, 2, 3],
        "all_false": [],
        "no_statistics": [0, 1, 2, 3],
        "all_null": [],
        "partial_q": [3],
        "excluded": [1, 2, 3],
    }[case]
    assert [i for i, _ in narrowed] == expected
    if case == "excluded":
        assert narrowed == baseline
    assert all(dict(baseline)[i] == fully for i, fully in narrowed)
    assert str(residual) == original

    def result(groups):
        t = pq.ParquetFile(path).read_row_groups([i for i, _ in groups])
        t = t.append_column("p", pa.array([partition] * len(t), type=p_type))
        return ds.dataset(t).to_table(
            filter=residual.to_pyarrow() if q is None else (q & residual).to_pyarrow()
        )

    assert result(baseline).equals(result(narrowed), check_metadata=True)
    assert result(narrowed).schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize("dtype", SIGNED[:-1])
def test_out_of_range_comparison_preserves_footer_baseline(tmp_path, dtype):
    path = tmp_path / "p=0" / "data.parquet"
    path.parent.mkdir()
    table = pa.table({"x": pa.array(range(40), type=dtype)})
    pq.write_table(table, path, row_group_size=10)
    context = _scanner(
        table.schema.append(pa.field("p", dtype)), tmp_path
    ).listing_pruning_context()
    assert context is not None
    low, high = _bounds(dtype)
    baseline = _selected(path, None)
    for value in [low - 1, high + 1, True]:
        hint = ListingPruningHint.from_residual(
            context, (col("p") == 99) | (col("x") > value)
        )
        assert hint is not None
        assert _selected(path, hint) == baseline


@pytest.mark.parametrize("dtype", SIGNED[:-1])
def test_narrow_footer_embedded_schema_mismatch_does_not_decode(
    tmp_path, monkeypatch, dtype
):
    import base64

    path = tmp_path / "mismatch.parquet"
    encoded = base64.b64encode(pa.schema([("x", pa.int64())]).serialize().to_pybytes())
    table = pa.table({"x": pa.array([1, 2], type=dtype)})
    collected = []
    with pq.ParquetWriter(path, table.schema, metadata_collector=collected) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({"ARROW:schema": encoded})
    # Native writer footer: inspect metadata without reader schema restoration.
    metadata = collected[0]
    assert metadata.metadata[b"ARROW:schema"] == encoded

    def forbidden(*args, **kwargs):
        pytest.fail("Physical certificate must not deserialize embedded IPC schema")

    monkeypatch.setattr(pa.ipc, "read_schema", forbidden)
    assert (
        physical_schema_decline_reason(metadata, (("x", str(dtype)),))
        == "noncanonical_embedded_schema"
    )


def test_int32_decimal_physical_storage_declines(tmp_path):
    import inspect

    if "store_decimal_as_integer" not in inspect.signature(pq.write_table).parameters:
        pytest.skip("This Arrow writer cannot generate INT32 decimal storage")
    path = tmp_path / "decimal-int32.parquet"
    pq.write_table(
        pa.table({"x": pa.array([1, 2], type=pa.decimal128(4, 0))}),
        path,
        store_decimal_as_integer=True,
        store_schema=False,
    )
    metadata = pq.read_metadata(path)
    assert metadata.schema.column(0).physical_type == "INT32"
    assert metadata.schema.column(0).logical_type.type == "DECIMAL"
    for target in SIGNED:
        assert (
            physical_schema_decline_reason(metadata, (("x", str(target)),))
            == "unsupported_physical_column"
        )


@pytest.mark.parametrize("dtype", SIGNED)
@pytest.mark.parametrize(
    "op,literal,branch_true",
    [
        (Operation.LT, 1, True),
        (Operation.LT, -1, False),
        (Operation.LE, 0, True),
        (Operation.LE, -1, False),
        (Operation.GT, -1, True),
        (Operation.GT, 1, False),
        (Operation.GE, 0, True),
        (Operation.GE, 1, False),
    ],
)
@pytest.mark.parametrize("swap_or", [False, True])
def test_partition_ranges_real_footer_and_full_rows(
    tmp_path, dtype, op, literal, branch_true, swap_or
):
    path = tmp_path / "p=0" / "data.parquet"
    path.parent.mkdir()
    table = pa.table({"x": range(40)})
    pq.write_table(table, path, row_group_size=10)
    schema = table.schema.append(pa.field("p", dtype))
    context = _scanner(schema, tmp_path).listing_pruning_context()
    assert context is not None
    partition_predicate = BinaryExpr(op, col("p"), LiteralExpr(literal))
    file_predicate = col("x") >= 30
    residual = (
        file_predicate | partition_predicate
        if swap_or
        else partition_predicate | file_predicate
    )
    hint = ListingPruningHint.from_residual(context, residual)
    assert hint is not None
    selected = _selected(path, hint)
    assert [i for i, _ in selected] == ([0, 1, 2, 3] if branch_true else [3])
    baseline = table.append_column("p", pa.array([0] * len(table), type=dtype))
    read = pq.ParquetFile(path).read_row_groups([i for i, _ in selected])
    read = read.append_column("p", pa.array([0] * len(read), type=dtype))
    assert (
        ds.dataset(read)
        .to_table(filter=residual.to_pyarrow())
        .equals(
            ds.dataset(baseline).to_table(filter=residual.to_pyarrow()),
            check_metadata=True,
        )
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
