import itertools
import sys

import pyarrow
import pytest

from pyarrow import compute as pac
from ray.data.aggregate import Min, Max, Sum, Mean, Std, Quantile, AbsMax, Unique, Count


@pytest.mark.parametrize(
    "agg_cls",
    [
        Count,
        Min,
        Max,
        Sum,
        Mean,
        Std,
        Quantile,
        AbsMax,
        Unique,
    ],
)
@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_null_safe_aggregation_protocol(agg_cls, ignore_nulls):
    """This test verifies that all aggregation implementations
    properly implement aggregation protocol
    """

    col = pyarrow.array([0, 1, 2, None])
    t = pyarrow.table([col], names=["A"])

    pac_method = _map_to_pa_compute_method(agg_cls)

    expected = pac_method(col, ignore_nulls)

    agg_kwargs = {} if agg_cls is Unique else {"ignore_nulls": ignore_nulls}

    agg = agg_cls(on="A", **agg_kwargs)

    # Step 1: Initialize accumulator
    # Step 2: Partially aggregate individual rows
    accumulators = [
        agg.accumulate_block(agg.init(None), t.slice(i, 1)) for i in range(t.num_rows)
    ]

    # NOTE: This test intentionally permutes all accumulators to verify
    #       that combination is associative
    for permuted_accumulators in itertools.permutations(accumulators):
        # Step 3: Combine accumulators holding partial aggregations
        #         into final result
        cur = permuted_accumulators[0]
        for new in permuted_accumulators[1:]:
            cur = agg.merge(cur, new)

        res = agg.finalize(cur)

        # Assert that combining aggregations is an associative operation,
        # ie invariant of the order of combining partial aggregations
        assert res == expected, permuted_accumulators


def _map_to_pa_compute_method(agg_cls: type):
    _map = {
        Count: lambda col, ignore_nulls: pac.count(
            col, mode=("only_valid" if ignore_nulls else "all")
        ).as_py(),
        Min: lambda col, ignore_nulls: pac.min(col, skip_nulls=ignore_nulls).as_py(),
        Max: lambda col, ignore_nulls: pac.max(col, skip_nulls=ignore_nulls).as_py(),
        Sum: lambda col, ignore_nulls: pac.sum(col, skip_nulls=ignore_nulls).as_py(),
        Mean: lambda col, ignore_nulls: pac.mean(col, skip_nulls=ignore_nulls).as_py(),
        Std: lambda col, ignore_nulls: pac.stddev(
            col, ddof=1, skip_nulls=ignore_nulls
        ).as_py(),
        Quantile: lambda col, ignore_nulls: pac.quantile(
            col, q=0.5, skip_nulls=ignore_nulls
        )[0].as_py(),
        AbsMax: lambda col, ignore_nulls: pac.max(
            pac.abs(col), skip_nulls=ignore_nulls
        ).as_py(),
        Unique: lambda col, ignore_nulls: set(pac.unique(col).to_pylist()),
    }

    return _map[agg_cls]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
