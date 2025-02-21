import itertools

import pyarrow
import pytest

from pyarrow import compute as pac
from ray.data.aggregate import Min, Max, Sum, Mean, Std, Quantile
from ray.data._internal.util import is_nan


@pytest.mark.parametrize(
    "agg_cls,pac_method",
    [
        (Min, pac.min),
        (Max, pac.max),
        (Sum, pac.sum),
        (Mean, pac.mean),
        (Std, lambda col, **kwargs: pac.stddev(col, ddof=1, **kwargs)),
        (Quantile, lambda col, **kwargs: pac.quantile(col, q=0.5, **kwargs)[0]),
    ],
)
@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_null_safe_aggregation_protocol(agg_cls, pac_method, ignore_nulls):
    """This test verifies that all aggregation implementations
    properly implement aggregation protocol
    """

    col = pyarrow.array([0, 1, 2, None])
    t = pyarrow.table([col], names=["A"])

    expected = pac_method(col, skip_nulls=ignore_nulls).as_py()

    agg = agg_cls(on="A", ignore_nulls=ignore_nulls)

    # Step 1: Initialize accumulator
    init_val = agg.init(None)
    # Step 2: Partially aggregate individual rows
    accumulators = [
        agg.accumulate_block(init_val, t.slice(i, 1)) for i in range(t.num_rows)
    ]

    # NOTE: This test intentionally permutes all accumulators to verify
    #       that combination is associative
    for permuted_accumulators in itertools.permutations(accumulators):
        # Step 3: Combine accumulators holding partial aggregations
        #         into final result
        cur = init_val
        for new in permuted_accumulators:
            cur = agg.merge(cur, new)

        res = agg.finalize(cur)

        # Assert that combining aggregations is an associative operation,
        # ie invariant of the order of combining partial aggregations
        assert res == expected or (
            is_nan(res) and is_nan(expected)
        ), permuted_accumulators
