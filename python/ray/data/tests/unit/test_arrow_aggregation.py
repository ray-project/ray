import sys
from decimal import Decimal

import pyarrow as pa
import pytest

from ray.data._internal.arrow_aggregation import arrow_agg_options, mean_spec, sum_spec


def _aggregate(table, agg_spec, source_col):
    options = arrow_agg_options(ignore_nulls=True)
    prepared = agg_spec.prep(0, source_col, table)
    partial = prepared.group_by(["g"]).aggregate(
        agg_spec.raw_agg_specs(0, source_col, options)
    )
    return partial.rename_columns(["g", *agg_spec.components])


def test_integer_sum_uses_widened_accumulator():
    agg = sum_spec()
    partial = _aggregate(pa.table({"g": [0] * 8, "v": [2**62] * 8}), agg, "v")

    merged = partial.group_by(["g"]).aggregate(
        agg.merge_specs(tuple(agg.components), arrow_agg_options(True))
    )
    merged = merged.rename_columns(["g", *agg.components])
    result = agg.finalize(merged, tuple(agg.components))

    assert result.to_pylist() == [2**65]


def test_integer_sum_does_not_overflow_when_merging_partials():
    agg = sum_spec()
    partials = [
        _aggregate(pa.table({"g": [0] * 4, "v": [2**60] * 4}), agg, "v"),
        _aggregate(pa.table({"g": [0] * 4, "v": [2**60] * 4}), agg, "v"),
    ]
    merged = (
        pa.concat_tables(partials)
        .group_by(["g"])
        .aggregate(agg.merge_specs(tuple(agg.components), arrow_agg_options(True)))
    )
    merged = merged.rename_columns(["g", *agg.components])

    result = agg.finalize(merged, tuple(agg.components))

    assert result.to_pylist() == [2**63]


def test_unsigned_integer_sum_does_not_overflow():
    agg = sum_spec()
    partial = _aggregate(
        pa.table(
            {
                "g": [0, 0],
                "v": pa.array([2**64 - 1, 1], type=pa.uint64()),
            }
        ),
        agg,
        "v",
    )

    result = agg.finalize(partial, tuple(agg.components))

    assert result.to_pylist() == [2**64]


def test_unsigned_integer_sum_preserves_uint64_when_it_fits():
    agg = sum_spec()
    partial = _aggregate(
        pa.table({"g": [0], "v": pa.array([2**62], type=pa.uint64())}),
        agg,
        "v",
    )

    result = agg.finalize(partial, tuple(agg.components))

    assert result.type == pa.uint64()
    assert result.to_pylist() == [2**62]


def test_decimal_sum_is_not_widened():
    agg = sum_spec()
    partial = _aggregate(
        pa.table(
            {
                "g": [0, 0],
                "v": pa.array(
                    [Decimal("1.20"), Decimal("2.30")],
                    type=pa.decimal128(10, 2),
                ),
            }
        ),
        agg,
        "v",
    )

    assert partial["widened"].to_pylist() == [0]
    assert agg.finalize(partial, tuple(agg.components)).type == pa.decimal128(38, 2)


def test_integer_mean_uses_widened_sum():
    agg = mean_spec()
    partial = _aggregate(pa.table({"g": [0] * 8, "v": [2**62] * 8}), agg, "v")

    result = agg.finalize(partial, tuple(agg.components))

    assert result.to_pylist() == [float(2**62)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
