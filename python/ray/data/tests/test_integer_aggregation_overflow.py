from decimal import Decimal
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data._internal.arrow_block import ArrowBlockColumnAccessor
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _make_vectorized_aggregating_reduce_fn,
    _make_vectorized_aggregating_transformer,
)
from ray.data._internal.pandas_block import PandasBlockColumnAccessor
from ray.data.aggregate import Max, Mean, Sum, Unique
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


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
    assert result["sum(v)"].type == expected_type
    assert result.to_pylist() == [{"g": 0, "sum(v)": 3, "mean(v)": 1.5, "max(v)": 2}]


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
        assert result["sum(v)"].type == pa.int64()


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
