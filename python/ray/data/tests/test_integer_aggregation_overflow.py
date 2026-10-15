from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data._internal.arrow_aggregation import (
    integer_sum_metadata,
    widen_integer_sum_partials,
)
from ray.data._internal.arrow_block import ArrowBlockColumnAccessor
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _fallback_aggregating_reduce_fn,
    _fallback_aggregating_transformer,
    _make_vectorized_aggregating_reduce_fn,
    _make_vectorized_aggregating_transformer,
)
from ray.data._internal.pandas_block import PandasBlockColumnAccessor
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.aggregate import Count, Max, Mean, Min, Sum, Unique
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _serialize_partial(partial):
    partial = BlockAccessor.for_block(partial).to_arrow()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, partial.schema) as writer:
        writer.write_table(partial)
    return pa.ipc.open_stream(sink.getvalue()).read_all()


@pytest.mark.parametrize("invalid_name", ["missing", "duplicate"])
def test_integer_sum_metadata_skips_invalid_fields(invalid_name):
    table = pa.table(
        [pa.array([1, 2]), pa.array([3, 4]), pa.array([5, 6]), pa.array([7, 8])],
        names=["v", "duplicate", "duplicate", "last"],
    ).replace_schema_metadata(
        {
            **integer_sum_metadata(invalid_name, pa.int64()),
            **integer_sum_metadata("v", pa.int64()),
            b"unrelated": b"preserved",
        }
    )
    result = widen_integer_sum_partials(table)
    assert result.column(0).type == pa.decimal128(38, 0)
    assert result.column(0).to_pylist() == [Decimal(1), Decimal(2)]
    assert result.schema.metadata == table.schema.metadata
    for i in range(1, table.num_columns):
        assert result.column(i).equals(table.column(i))


@pytest.mark.parametrize("dtype", ["Int64", "UInt64"])
@pytest.mark.parametrize("values", ["mixed_nulls", "all_nulls", "overflow"])
@pytest.mark.parametrize("ignore_nulls", [False, True])
@pytest.mark.parametrize("as_py", [False, True])
def test_pandas_nullable_integer_sum(dtype, values, ignore_nulls, as_py):
    value = 2**63 if dtype == "UInt64" else 2**62
    data = {
        "mixed_nulls": [value, None, value],
        "all_nulls": [None, None],
        "overflow": [value, value],
    }[values]
    result = PandasBlockColumnAccessor(pd.Series(data, dtype=dtype)).sum(
        ignore_nulls=ignore_nulls, as_py=as_py
    )
    if values == "all_nulls":
        assert result is None
    elif values == "mixed_nulls" and not ignore_nulls:
        if as_py:
            assert result is None
        else:
            assert type(result) is float and np.isnan(result)
    else:
        assert result == 2 * value and type(result) is int


@pytest.mark.parametrize(
    "dtype,values",
    [
        pytest.param("Int64", [1], id="small"),
        pytest.param("Int64", [2**53 + 1], id="signed-precision"),
        pytest.param("UInt64", [2**53 + 1], id="unsigned-precision"),
        pytest.param("Int64", [-(2**53 + 1)], id="negative"),
        pytest.param("Int64", [2**63 - 1], id="signed-boundary"),
        pytest.param("UInt64", [2**64 - 1], id="unsigned-boundary"),
        pytest.param("Int64", [2**62 + 1] * 2, id="overflow"),
        pytest.param("Int64", [2**62 + 1, -(2**62)], id="cancellation"),
    ],
)
@pytest.mark.parametrize("ignore_nulls", [False, True])
@pytest.mark.parametrize("compact_builder", [False, True])
def test_pandas_integer_sum_mixed_null_groups(
    ray_start_regular_shared_2_cpus,
    monkeypatch,
    dtype,
    values,
    ignore_nulls,
    compact_builder,
):
    # The valid and null groups must share one Pandas partial. Otherwise its
    # builder cannot coerce the integer-plus-null sum column to float64.
    source = pd.DataFrame(
        {
            "g": [0] * len(values) + [1, 1, 2, 2],
            "v": pd.Series(values + [None, None, values[0], None], dtype=dtype),
        }
    )
    aggs = (
        Sum("v", ignore_nulls),
        Sum("v", ignore_nulls),
        Sum("v", ignore_nulls, alias_name="total"),
        Count("v", ignore_nulls=True),
        Mean("v", ignore_nulls),
    )
    if compact_builder:
        monkeypatch.setattr(
            "ray.data._internal.table_block.MAX_UNCOMPACTED_SIZE_BYTES", 0
        )
    expected = {
        0: sum(values),
        1: None,
        2: values[0] if ignore_nulls else None,
    }

    def check(block, multiplier):
        assert isinstance(block, pa.Table)
        rows = {row["g"]: row for row in block.to_pylist()}
        assert set(rows) == set(expected)
        for name in ["sum(v)", "sum(v)_2", "total"]:
            assert block[name].type == pa.decimal128(38, 0)
            assert {g: row[name] for g, row in rows.items()} == {
                g: Decimal(value * multiplier) if value is not None else None
                for g, value in expected.items()
            }
        assert rows[0]["count(v)"] == multiplier * len(values)
        assert rows[1]["count(v)"] == 0
        assert rows[2]["count(v)"] == multiplier

    partial = BlockAccessor.for_block(source)._aggregate(SortKey("g"), aggs)
    check(partial, 1)
    partial = _serialize_partial(partial)
    compacted, metadata = BlockAccessor.for_block(partial)._combine_aggregated_blocks(
        [partial, partial], SortKey("g"), aggs, finalize=False
    )
    check(compacted, 2)
    assert compacted.schema.metadata == partial.schema.metadata
    assert metadata.schema == compacted.schema
    final, metadata = BlockAccessor.for_block(compacted)._combine_aggregated_blocks(
        [_serialize_partial(compacted), partial], SortKey("g"), aggs
    )
    assert isinstance(final, pa.Table)
    check(final, 3)
    assert not final.schema.metadata
    assert metadata.schema == final.schema
    rows = {row["g"]: row for row in final.to_pylist()}
    assert rows[0]["mean(v)"] == sum(values) / len(values)
    assert rows[1]["mean(v)"] is None
    assert rows[2]["mean(v)"] == (float(values[0]) if ignore_nulls else None)


@pytest.mark.parametrize("dtype", ["Int64", "UInt64"])
@pytest.mark.parametrize("ignore_nulls", [False, True])
def test_distributed_pandas_integer_sum_mixed_null_groups(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    monkeypatch,
    tmp_path,
    dtype,
    ignore_nulls,
):
    monkeypatch.setattr(
        ray.data.context.DataContext.get_current(), "enable_pandas_block", True
    )
    value = 2**53 + 1
    source = pd.DataFrame(
        {
            "g": [0, 1, 1, 2, 2],
            "v": pd.Series([value, None, None, value, None], dtype=dtype),
        }
    )
    ds = ray.data.from_pandas([source] * 4)
    refs = [
        ref for bundle in ds.iter_internal_ref_bundles() for ref in bundle.block_refs
    ]
    assert all(isinstance(block, pd.DataFrame) for block in ray.get(refs))
    # The collection aggregation forces the Python fallback in shuffle-v2.
    result = (
        ds.groupby("g", num_partitions=2)
        .aggregate(
            Sum("v", ignore_nulls),
            Sum("v", ignore_nulls, alias_name="total"),
            Count("v", ignore_nulls=True),
            Unique("g"),
        )
        .materialize()
    )
    schema = result.schema().base_schema
    assert schema.field("sum(v)").type == pa.decimal128(38, 0)
    assert schema.field("total").type == pa.decimal128(38, 0)
    for block in ray.get(result.to_arrow_refs()):
        # Legacy sort shuffle can retain empty Pandas blocks with no schema.
        if BlockAccessor.for_block(block).num_rows():
            assert isinstance(block, pa.Table)
            assert block.schema == schema
            assert not block.schema.metadata
    expected = {
        0: Decimal(4 * value),
        1: None,
        2: Decimal(4 * value) if ignore_nulls else None,
    }

    def check(rows):
        for name in ["sum(v)", "total"]:
            assert {row["g"]: row[name] for row in rows} == expected
        assert {row["g"]: row["count(v)"] for row in rows} == {0: 4, 1: 0, 2: 4}
        assert {row["g"]: row["unique(g)"] for row in rows} == {0: [0], 1: [1], 2: [2]}

    check(result.take_all())
    batches = list(result.iter_batches(batch_format="pyarrow", batch_size=100))
    assert all(batch.schema == schema for batch in batches)
    check([row for batch in batches for row in batch.to_pylist()])
    check(result.sort("g").take_all())
    check(result.repartition(1).take_all())
    result.write_parquet(str(tmp_path), min_rows_per_file=1000, concurrency=1)
    check(ray.data.read_parquet(str(tmp_path)).take_all())


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("unsigned", [False, True])
@pytest.mark.parametrize("dictionary", [False, True])
def test_integer_sum_schema_is_stable_across_reducers(
    ray_start_regular_shared_2_cpus, fallback, unsigned, dictionary
):
    dtype = pa.uint64() if unsigned else pa.int64()
    value = 2**63 if unsigned else 2**62
    aggs = (Sum("v"), Sum("v"), Sum("v", alias_name="total"), Mean("v"))
    if fallback:
        aggs += (Unique("v"),)
    map_builder = (
        _fallback_aggregating_transformer
        if fallback
        else _make_vectorized_aggregating_transformer
    )
    reduce_builder = (
        _fallback_aggregating_reduce_fn
        if fallback
        else _make_vectorized_aggregating_reduce_fn
    )
    transform = map_builder(("g",), aggs)
    reduce_fn = reduce_builder(("g",), aggs)
    assert transform is not None and reduce_fn is not None
    groups = [[1, 2], [value, value], [None, None]]
    if not unsigned:
        groups += [[-value, -value], [value, -value]]
    outputs = []
    for g, values in enumerate(groups):
        table = pa.table({"g": [g] * len(values), "v": pa.array(values, type=dtype)})
        if dictionary:
            table = table.set_column(1, "v", table["v"].dictionary_encode())
        partial = _serialize_partial(transform(table))
        output = list(reduce_fn(g, [[partial]]))[0]
        assert isinstance(output, pa.Table)
        assert not output.schema.metadata
        row = output.to_pylist()[0]
        expected = sum(v for v in values if v is not None) if g != 2 else None
        for name in ["sum(v)", "sum(v)_2", "total"]:
            assert output[name].type == pa.decimal128(38, 0)
            assert row[name] == expected
            assert row[name] is None or isinstance(row[name], Decimal)
        assert row["mean(v)"] == (
            expected / len(values) if expected is not None else None
        )
        outputs.append(output)
    # Fallback Mean keeps its existing null type for an all-null partition.
    # Integer sum fields must agree even when sibling fields need promotion.
    sums = [output.select(["g", "sum(v)", "sum(v)_2", "total"]) for output in outputs]
    assert pa.unify_schemas([output.schema for output in sums]) == sums[0].schema
    assert pa.concat_tables(sums).num_rows == len(groups)


@pytest.mark.parametrize("input_format", ["arrow", "pandas"])
@pytest.mark.parametrize("scale", [0, 2])
def test_fallback_integer_sum_compaction_preserves_provenance(
    ray_start_regular_shared_2_cpus, input_format, scale
):
    class CustomSum(Sum):
        def _arrow_agg_spec(self):
            return None

    aggs = (
        Sum("v"),
        Sum("d"),
        Mean("v"),
        Count("v"),
        Min("v"),
        Max("v"),
        CustomSum("small", alias_name="custom"),
    )
    table = pa.table(
        {
            "g": [0, 0, 1],
            "v": pa.array([2**62, 2**62, 1]),
            "d": pa.array([Decimal(2)] * 3, type=pa.decimal128(20, scale)),
            "small": [1, 2, 3],
        }
    )
    source = table if input_format == "arrow" else table.to_pandas()
    accessor = BlockAccessor.for_block(source)
    partial = _serialize_partial(accessor._aggregate(SortKey("g"), aggs))
    compacted, metadata = BlockAccessor.for_block(partial)._combine_aggregated_blocks(
        [partial, partial], SortKey("g"), aggs, finalize=False
    )
    assert compacted.schema.metadata == partial.schema.metadata
    assert metadata.schema == compacted.schema
    final, metadata = BlockAccessor.for_block(compacted)._combine_aggregated_blocks(
        [_serialize_partial(compacted), partial], SortKey("g"), aggs
    )
    assert isinstance(final, pa.Table)
    assert not final.schema.metadata
    assert metadata.schema == final.schema
    assert final["sum(v)"].type == pa.decimal128(38, 0)
    assert final["sum(d)"].type == pa.decimal128(2 + scale, scale)
    assert final["custom"].type == pa.int64()
    rows = {row["g"]: row for row in final.to_pylist()}
    assert rows[0]["sum(v)"] == 3 * 2**63
    assert rows[1]["sum(v)"] == 3
    assert rows[0]["sum(d)"] == Decimal(12)
    assert rows[0]["mean(v)"] == float(2**62)
    assert rows[0]["count(v)"] == 6
    assert rows[0]["min(v)"] == rows[0]["max(v)"] == 2**62
    assert rows[0]["custom"] == 9


@pytest.mark.parametrize("fallback", [False, True])
def test_distributed_integer_sum_schema_and_parquet(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    tmp_path,
    fallback,
):
    # Many groups and explicit hash partitions force small and oversized sums
    # into different output blocks. Row collection alone misses this failure.
    values = [2**62 if g == 0 else g + 1 for g in range(16)] * 2
    table = pa.table({"g": list(range(16)) * 2, "v": pa.array(values)})
    aggs = (Sum("v"), Mean("v"), Max("v"))
    if fallback:
        aggs += (Unique("v"),)
    result = (
        ray.data.from_arrow([table] * 4)
        .groupby("g", num_partitions=8)
        .aggregate(*aggs)
        .materialize()
    )
    # Legacy hash shuffle also emits empty blocks with an empty schema.
    blocks = [block for block in ray.get(result.to_arrow_refs()) if block.num_rows > 0]
    assert len(blocks) > 1
    schema = result.schema().base_schema
    for block in blocks:
        assert block.schema == schema
        assert not block.schema.metadata
    assert schema.field("sum(v)").type == pa.decimal128(38, 0)
    expected = {g: 8 * (2**62 if g == 0 else g + 1) for g in range(16)}

    def check_rows(rows):
        assert {row["g"]: row["sum(v)"] for row in rows} == expected
        assert all(isinstance(row["sum(v)"], Decimal) for row in rows)
        assert all(row["mean(v)"] == row["max(v)"] for row in rows)

    check_rows(result.take_all())
    batches = list(result.iter_batches(batch_size=100, batch_format="pyarrow"))
    assert all(batch.schema == schema for batch in batches)
    check_rows([row for batch in batches for row in batch.to_pylist()])
    check_rows(result.sort("g").take_all())
    check_rows(result.repartition(1).take_all())
    result.write_parquet(str(tmp_path), min_rows_per_file=1000, concurrency=1)
    check_rows(ray.data.read_parquet(str(tmp_path)).take_all())


def _vectorized_result(tables, aggs):
    transform = _make_vectorized_aggregating_transformer(("g",), aggs)
    reduce_fn = _make_vectorized_aggregating_reduce_fn(("g",), aggs)
    assert transform is not None and reduce_fn is not None
    partials = [transform(table) for table in tables]
    # Serialization must retain the provenance used by the reducer.
    serialized = []
    for partial in partials:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, partial.schema) as writer:
            writer.write_table(partial)
        serialized.append(pa.ipc.open_stream(sink.getvalue()).read_all())
    blocks = list(reduce_fn(0, [serialized]))
    assert len(blocks) == 1
    assert isinstance(blocks[0], pa.Table)
    assert not blocks[0].schema.metadata
    return blocks[0]


@pytest.mark.parametrize(
    "dtype",
    [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
    ],
)
def test_integer_sum_preserves_native_type(dtype):
    column = pa.array([1, 2, None], type=dtype)
    accessor = ArrowBlockColumnAccessor(column)
    expected_type = pc.call_function("sum", [column]).type
    assert accessor.sum(ignore_nulls=True) == 3
    scalar = accessor.sum(ignore_nulls=True, as_py=False)
    assert scalar is not None and scalar.type == expected_type
    null = accessor.sum(ignore_nulls=False, as_py=False)
    assert null is not None and not null.is_valid and null.type == expected_type
    table = pa.table({"g": [0, 0, 0], "v": column})
    result = _vectorized_result([table], (Sum("v"), Mean("v"), Max("v")))
    assert result["sum(v)"].type == pa.decimal128(38, 0)
    assert result.to_pylist() == [
        {"g": 0, "sum(v)": Decimal(3), "mean(v)": 1.5, "max(v)": 2}
    ]


@pytest.mark.parametrize(
    "dtype", [pa.int64(), pa.uint64(), pa.dictionary(pa.int8(), pa.int64())]
)
def test_integer_sum_output_field_is_unresolved(dtype):
    assert Sum("v").output_field(pa.schema([pa.field("v", dtype)])) is None


@pytest.mark.parametrize(
    "dtype", [pa.float64(), pa.decimal128(20, 0), pa.decimal128(20, 2)]
)
def test_noninteger_sum_output_field_preserves_kernel_type(dtype):
    expected_type = pc.call_function("sum", [pa.array([], type=dtype)]).type
    assert Sum("v").output_field(pa.schema([pa.field("v", dtype)])) == pa.field(
        "sum(v)", expected_type
    )


@pytest.mark.parametrize("unsigned", [False, True])
@pytest.mark.parametrize("dictionary", [False, True])
def test_overflow_and_cancellation_across_partials(unsigned, dictionary):
    dtype = pa.uint64() if unsigned else pa.int64()
    value = 2**63 if unsigned else 2**62
    column = pa.array([value] * 4, type=dtype)
    if dictionary:
        column = column.dictionary_encode()
    table = pa.table({"g": [0] * 4, "v": column, "__d0_sumsrc": [7] * 4})
    # Arrow's max kernel does not support dictionary inputs. Exercise sum and
    # mean there; ordinary integer inputs also check sibling max preservation.
    aggs = (Sum("v"), Mean("v")) if dictionary else (Sum("v"), Mean("v"), Max("v"))
    result = _vectorized_result([table, table], aggs)
    expected = {"g": 0, "sum(v)": Decimal(value * 8), "mean(v)": float(value)}
    if not dictionary:
        expected["max(v)"] = value
        assert result["max(v)"].type == dtype
    assert result.to_pylist() == [expected]
    assert result["sum(v)"].type == pa.decimal128(38, 0)
    if not unsigned:
        negative = pa.table({"g": [0] * 4, "v": pa.array([-value] * 4, type=dtype)})
        result = _vectorized_result([table, negative], (Sum("v"), Mean("v")))
        assert result.to_pylist() == [{"g": 0, "sum(v)": 0, "mean(v)": 0.0}]
        assert result["sum(v)"].type == pa.decimal128(38, 0)


@pytest.mark.parametrize(
    "dtype", [pa.decimal128(20, 0), pa.decimal128(20, 2), pa.decimal256(50, 2)]
)
def test_decimal_sum_keeps_decimal_type_and_scale(dtype):
    column = pa.array([Decimal("2"), Decimal("3"), None], type=dtype)
    expected = pc.call_function("sum", [column])
    accessor = ArrowBlockColumnAccessor(column)
    assert accessor.sum(ignore_nulls=True) == expected.as_py()
    assert isinstance(accessor.sum(ignore_nulls=True), Decimal)
    scalar = accessor.sum(ignore_nulls=True, as_py=False)
    assert scalar is not None and scalar.type == expected.type
    assert isinstance(Sum("v").aggregate_block(pa.table({"v": column})), Decimal)
    table = pa.table({"g": [0, 0, 0], "v": column})
    result = _vectorized_result([table, table], (Sum("v"), Mean("v")))
    assert result["sum(v)"].type == expected.type
    assert result.to_pylist() == [{"g": 0, "sum(v)": Decimal("10"), "mean(v)": 2.5}]


@pytest.mark.parametrize("dtype", ["int64", "uint64", "Int64", "UInt64"])
def test_pandas_integer_accessor_overflow(dtype):
    value = 2**63 if "uint" in dtype.lower() else 2**62
    series = pd.Series([value] * 8, dtype=dtype)
    assert PandasBlockColumnAccessor(series).sum(ignore_nulls=True) == value * 8


@pytest.mark.parametrize(
    "dtype,values", [(pa.float64(), [1.5, 2.5]), (pa.bool_(), [True, False])]
)
def test_noninteger_sum_unchanged(dtype, values):
    column = pa.array(values, type=dtype)
    result = ArrowBlockColumnAccessor(column).sum(ignore_nulls=True, as_py=False)
    assert result == pc.call_function("sum", [column])


@pytest.mark.parametrize("ignore_nulls", [False, True])
def test_integer_null_groups(ignore_nulls):
    table = pa.table(
        {"g": [0, 0, 1, 1], "v": pa.array([None, None, 2**62, None], type=pa.int64())}
    )
    result = _vectorized_result(
        [table, table], (Sum("v", ignore_nulls), Mean("v", ignore_nulls))
    )
    rows = {row["g"]: row for row in result.to_pylist()}
    assert rows[0] == {"g": 0, "sum(v)": None, "mean(v)": None}
    assert rows[1] == {
        "g": 1,
        "sum(v)": Decimal(2**63) if ignore_nulls else None,
        "mean(v)": float(2**62) if ignore_nulls else None,
    }


def test_reduce_combines_native_and_widened_integer_partials():
    large = pa.table({"g": [0, 0], "v": pa.array([2**62, 2**62], type=pa.int64())})
    small = pa.table({"g": [0, 0], "v": pa.array([1, 2], type=pa.int64())})
    result = _vectorized_result([small, large], (Sum("v"), Mean("v")))
    assert result.to_pylist() == [
        {"g": 0, "sum(v)": Decimal(2**63 + 3), "mean(v)": float((2**63 + 3) / 4)}
    ]
    # Every map sum fits, but their combined sum does not.
    result = _vectorized_result([large.slice(0, 1)] * 4, (Sum("v"), Mean("v")))
    assert result.to_pylist() == [
        {"g": 0, "sum(v)": Decimal(2**64), "mean(v)": float(2**62)}
    ]


def test_mean_preserves_decimal_fallback_and_exact_integer_cancellation():
    mean = Mean("v")
    block = pa.table(
        {"v": pa.array([Decimal("2.00"), Decimal("3.00")], type=pa.decimal128(20, 2))}
    )
    accumulator = mean.aggregate_block(block)
    assert accumulator is not None
    result = mean.finalize(accumulator)
    assert result == Decimal("2.50") and isinstance(result, Decimal)
    # These integer partials fit decimal128 but exceed the default Decimal
    # context's 28 digits. Their difference must not round away before division.
    large = 10**35
    result = mean.finalize(
        mean.combine([Decimal(large), 1, 1], [Decimal(-large + 1), 1, 1])
    )
    assert result == 0.5 and type(result) is float
    # Divide exact integers before converting to float, preserving Python's
    # rounding. Converting the numerator first introduces another rounding step.
    total, count = 19253989466460215758, 9405
    assert mean.finalize([Decimal(total), count, 1]) == total / count


@pytest.mark.parametrize("input_format", ["arrow", "pandas"])
def test_distributed_integer_aggregations(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    input_format,
):
    # One group overflows in every partial; another stays within int64. Their
    # reducers can emit different physical types, which must remain compatible.
    blocks = []
    for _ in range(4):
        table = pa.table({"g": [0, 0, 1], "v": pa.array([2**62, 2**62, 1])})
        blocks.append(table if input_format == "arrow" else table.to_pandas())
    ds = (
        ray.data.from_arrow(blocks)
        if input_format == "arrow"
        else ray.data.from_pandas(blocks)
    )
    assert ds.sum("v") == 2**65 + 4
    assert type(ds.sum("v")) is int
    assert ds.mean("v") == pytest.approx((2**65 + 4) / 12)
    result = ds.groupby("g").aggregate(Sum("v"), Mean("v"), Max("v"))
    rows = {row["g"]: row for row in result.take_all()}
    assert int(rows[0]["sum(v)"]) == 2**65
    assert rows[0]["mean(v)"] == float(2**62)
    assert rows[0]["max(v)"] == 2**62
    assert int(rows[1]["sum(v)"]) == 4
    assert rows[1]["mean(v)"] == 1.0
    # Mixed collection/reduction takes the Python fallback in SHUFFLE_V2.
    fallback = ds.groupby("g").aggregate(Sum("v"), Mean("v"), Unique("v"))
    rows = {row["g"]: row for row in fallback.take_all()}
    assert int(rows[0]["sum(v)"]) == 2**65
    assert rows[0]["mean(v)"] == float(2**62)


@pytest.mark.parametrize("scale", [0, 2])
def test_distributed_decimal_sum_preserves_type(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    scale,
):
    table = pa.table(
        {
            "g": [0, 0],
            "d": pa.array([Decimal("2"), Decimal("3")], type=pa.decimal128(20, scale)),
            "i": pa.array([2**62, 2**62], type=pa.int64()),
        }
    )
    ds = ray.data.from_arrow([table, table])
    sums = ds.sum(["d", "i", "i"])
    assert sums == {"sum(d)": Decimal("10"), "sum(i)": 2**64, "sum(i)_2": 2**64}
    assert isinstance(sums["sum(d)"], Decimal)
    assert type(sums["sum(i)"]) is int
    assert type(sums["sum(i)_2"]) is int
    decimal_mean = ds.mean("d")
    assert decimal_mean == Decimal("2.5") and isinstance(decimal_mean, Decimal)
    result = ds.groupby("g").sum("d")
    rows = result.take_all()
    assert rows == [{"g": 0, "sum(d)": Decimal("10")}]
    assert isinstance(rows[0]["sum(d)"], Decimal)
    dtype = result.schema().base_schema.field("sum(d)").type
    assert pa.types.is_decimal(dtype) and dtype.scale == scale


_VALUE = 2**62
_EXPECTED = float(_VALUE)


@pytest.mark.parametrize("decimal", [False, True])
def test_sum_does_not_execute_lazy_map_again(ray_start_regular_shared_2_cpus, decimal):
    class Calls:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

    calls: Any = ray.remote(num_cpus=0)(Calls).remote()

    def convert(batch):
        ray.get(calls.increment.remote())
        value = Decimal("2.00") if decimal else 2**62
        dtype = pa.decimal128(20, 2) if decimal else pa.int64()
        return pa.table({"v": pa.array([value] * len(batch), type=dtype)})

    ds = ray.data.range(8, override_num_blocks=1).map_batches(
        convert, batch_format="pyarrow"
    )
    assert ds.schema(fetch_if_missing=False) is None
    result = ds.sum(["v", "v"])
    expected = Decimal("16.00") if decimal else 2**65
    assert result == {"sum(v)": expected, "sum(v)_2": expected}
    assert all(
        type(value) is (Decimal if decimal else int) for value in result.values()
    )
    assert ray.get(calls.get.remote()) == 1


def test_dataset_sum_int64_does_not_wrap(ray_start_regular_shared_2_cpus):
    # One block: the block sum itself is outside int64.
    one_block = ray.data.from_items([{"v": 2**62}] * 8, override_num_blocks=1)
    total = one_block.sum("v")
    assert total == 2**65
    assert type(total) is int

    # Several blocks: each partial is 2 * 2**62 = 2**63, which is itself
    # outside signed int64, and the combined total is 2**65.
    several = ray.data.from_items([{"v": 2**62}] * 8, override_num_blocks=4)
    assert several.sum("v") == 2**65

    negative = ray.data.from_items([{"v": -(2**62)}] * 8, override_num_blocks=4)
    assert negative.sum("v") == -(2**65)

    with_null = ray.data.from_items([{"v": 2**62}, {"v": None}, {"v": 2**62}])
    assert with_null.sum("v") == 2**63
    assert with_null.sum("v", ignore_nulls=False) is None

    columns = ray.data.from_items([{"a": 2**62, "b": 1}] * 8)
    summed = columns.sum(["a", "b"])
    assert summed["sum(a)"] == 2**65
    assert type(summed["sum(a)"]) is int
    assert summed["sum(b)"] == 8

    # Values that fit in int64 stay a Python int, including the documented
    # example and an empty dataset.
    assert ray.data.range(100).sum("id") == 4950
    assert ray.data.range(100).mean("id") == 49.5
    assert ray.data.range(0).sum("id") is None


def test_groupby_sum_int64_does_not_wrap(ray_start_regular_shared_2_cpus):
    ds = ray.data.from_items([{"g": i % 2, "v": 2**62} for i in range(8)])
    rows = ds.groupby("g").sum("v").take_all()
    assert {row["g"]: int(row["sum(v)"]) for row in rows} == {0: 2**64, 1: 2**64}


def _grouped_means(tables, *, ignore_nulls=True, on="v", keys=("g",)):
    aggs = (Mean(on=on, ignore_nulls=ignore_nulls),)
    transform = _make_vectorized_aggregating_transformer(keys, aggs)
    reduce_fn = _make_vectorized_aggregating_reduce_fn(keys, aggs)
    assert transform is not None and reduce_fn is not None
    partials = [transform(table) for table in tables]
    blocks = list(reduce_fn(0, [partials]))
    assert len(blocks) == 1
    assert isinstance(blocks[0], pa.Table)
    rows = blocks[0].to_pylist()
    return {row[keys[0]]: row[f"mean({on})"] for row in rows}


def test_arrow_grouped_mean_int64_does_not_wrap():
    table = pa.table(
        {
            "g": [0] * 8,
            "v": pa.array([_VALUE] * 8, type=pa.int64()),
        }
    )
    assert _grouped_means([table]) == {0: _EXPECTED}

    # 3 * 2**62 wraps to -2**62 in int64. The mean must stay positive.
    three = pa.table(
        {
            "g": [0, 0, 0],
            "v": pa.array([_VALUE] * 3, type=pa.int64()),
        }
    )
    assert _grouped_means([three]) == {0: _EXPECTED}

    # Each partial is 4 * 2**62 = 2**64, which itself does not fit in int64.
    # The reduce has to sum those decimal partials, then divide.
    partial = pa.table(
        {
            "g": [0] * 4,
            "v": pa.array([_VALUE] * 4, type=pa.int64()),
        }
    )
    assert _grouped_means([partial, partial]) == {0: _EXPECTED}

    negative = pa.table(
        {
            "g": [0] * 8,
            "v": pa.array([-_VALUE] * 8, type=pa.int64()),
        }
    )
    assert _grouped_means([negative]) == {0: -_EXPECTED}

    small = pa.table({"g": [0] * 10, "v": pa.array(list(range(10)), type=pa.int64())})
    assert _grouped_means([small]) == {0: 4.5}

    floats = pa.table({"g": [0, 0], "v": pa.array([1.5, 2.5])})
    assert _grouped_means([floats]) == {0: 2.0}

    with_null = pa.table(
        {
            "g": [0, 0, 0],
            "v": pa.array([_VALUE, None, _VALUE], type=pa.int64()),
        }
    )
    assert _grouped_means([with_null]) == {0: _EXPECTED}
    assert _grouped_means([with_null], ignore_nulls=False) == {0: None}

    other_group = pa.table(
        {
            "g": [0] * 8 + [1] * 4,
            "v": pa.array([_VALUE] * 8 + [10] * 4, type=pa.int64()),
        }
    )
    assert _grouped_means([other_group]) == {0: _EXPECTED, 1: 10.0}


def test_mean_aggregate_block_int64_does_not_wrap():
    # Keyless mean uses this Python accumulator. Eight copies wrap to 0
    # before the divide; the partial itself is already outside int64.
    block = pa.table({"v": pa.array([_VALUE] * 8, type=pa.int64())})
    mean = Mean(on="v")
    accumulator = mean.aggregate_block(block)
    assert accumulator is not None
    assert mean.finalize(accumulator) == _EXPECTED
    assert type(mean.finalize(accumulator)) is float

    left = pa.table({"v": pa.array([_VALUE] * 4, type=pa.int64())})
    right = pa.table({"v": pa.array([_VALUE] * 4, type=pa.int64())})
    left_acc = mean.aggregate_block(left)
    right_acc = mean.aggregate_block(right)
    assert left_acc is not None and right_acc is not None
    combined = mean.combine(left_acc, right_acc)
    assert mean.finalize(combined) == _EXPECTED

    negative = Mean(on="v")
    neg = pa.table({"v": pa.array([-_VALUE] * 8, type=pa.int64())})
    accumulator = negative.aggregate_block(neg)
    assert accumulator is not None
    assert negative.finalize(accumulator) == -_EXPECTED

    small = Mean(on="v")
    small_block = pa.table({"v": pa.array([1, 2, 3, 4], type=pa.int64())})
    accumulator = small.aggregate_block(small_block)
    assert accumulator is not None
    assert small.finalize(accumulator) == 2.5

    floats = Mean(on="v")
    float_block = pa.table({"v": pa.array([1.5, 2.5])})
    accumulator = floats.aggregate_block(float_block)
    assert accumulator is not None
    assert floats.finalize(accumulator) == 2.0

    with_null = Mean(on="v")
    null_block = pa.table({"v": pa.array([_VALUE, None, _VALUE], type=pa.int64())})
    accumulator = with_null.aggregate_block(null_block)
    assert accumulator is not None
    assert with_null.finalize(accumulator) == _EXPECTED
    skipped = Mean(on="v", ignore_nulls=False)
    assert skipped.aggregate_block(null_block) is None


def test_dataset_mean_int64_does_not_wrap(ray_start_regular_shared_2_cpus):
    one_block = ray.data.from_items([{"v": _VALUE}] * 8, override_num_blocks=1)
    assert one_block.mean("v") == _EXPECTED
    assert type(one_block.mean("v")) is float

    several = ray.data.from_items([{"v": _VALUE}] * 8, override_num_blocks=4)
    assert several.mean("v") == _EXPECTED

    negative = ray.data.from_items([{"v": -_VALUE}] * 8, override_num_blocks=4)
    assert negative.mean("v") == -_EXPECTED

    with_null = ray.data.from_items([{"v": _VALUE}, {"v": None}, {"v": _VALUE}])
    assert with_null.mean("v") == _EXPECTED
    assert with_null.mean("v", ignore_nulls=False) is None

    assert ray.data.range(100).mean("id") == 49.5
    assert ray.data.range(0).mean("id") is None


def test_groupby_mean_int64_does_not_wrap(ray_start_regular_shared_2_cpus):
    rows = [{"g": 0, "v": _VALUE}] * 8
    grouped = ray.data.from_items(rows).groupby("g").mean("v").take_all()
    assert {row["g"]: row["mean(v)"] for row in grouped} == {0: _EXPECTED}

    # Split so each map partial overflows int64 on its own (2 * 2**62 = 2**63).
    split = (
        ray.data.from_items([{"g": 0, "v": _VALUE}] * 8, override_num_blocks=4)
        .groupby("g")
        .mean("v")
        .take_all()
    )
    assert {row["g"]: row["mean(v)"] for row in split} == {0: _EXPECTED}

    two = ray.data.from_items(
        [{"g": i % 2, "v": _VALUE if i % 2 == 0 else 10} for i in range(8)]
    )
    by_group = {
        row["g"]: row["mean(v)"] for row in two.groupby("g").mean("v").take_all()
    }
    assert by_group[0] == _EXPECTED
    assert by_group[1] == 10.0

    signed = ray.data.from_items([{"g": 0, "v": -_VALUE}] * 8).groupby("g").mean("v")
    assert {row["g"]: row["mean(v)"] for row in signed.take_all()} == {0: -_EXPECTED}

    # Widening the mean input must not change a sibling aggregation's column.
    both = (
        ray.data.from_items(rows)
        .groupby("g")
        .aggregate(Mean(on="v"), Max(on="v"))
        .take_all()
    )
    assert {row["g"]: (row["mean(v)"], row["max(v)"]) for row in both} == {
        0: (_EXPECTED, _VALUE)
    }

    small = ray.data.from_items([{"g": 0, "v": i} for i in range(10)])
    small_rows = small.groupby("g").mean("v").take_all()
    assert small_rows == [{"g": 0, "mean(v)": 4.5}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
