import pytest
from ray.rllib.utils.metrics.stats import Stats
from ray.rllib.utils.test_utils import check


def test_reduce_per_index_on_merge():
    stats_default = Stats(reduce="mean", window=3, reduce_per_index_on_merge=False)
    stats1 = Stats(reduce="mean", window=3)
    stats1.push(10)
    stats1.push(20)
    stats1.push(30)

    stats2 = Stats(reduce="mean", window=3)
    stats2.push(100)
    stats2.push(200)
    stats2.push(300)

    stats_default.merge_in_parallel(stats1, stats2)
    # Default behavior repeats the mean at each index:
    # Last index: mean([30, 300]) = 165 -> [165, 165]
    # Second to last: mean([20, 200]) = 110 -> [165, 165, 110, 110]
    # Take only last 3: [110, 165, 165]
    check(stats_default.values, [110, 165, 165])
    check(stats_default.peek(), (110 + 165 + 165) / 3)

    stats_per_index = Stats(reduce="mean", window=3, reduce_per_index_on_merge=True)
    stats1 = Stats(reduce="mean", window=3)
    stats1.push(10)
    stats1.push(20)
    stats1.push(30)

    stats2 = Stats(reduce="mean", window=3)
    stats2.push(100)
    stats2.push(200)
    stats2.push(300)

    stats_per_index.merge_in_parallel(stats1, stats2)
    # Per-index behavior:
    # Last index: mean([30, 300]) = 165 -> [165]
    # Second to last: mean([20, 200]) = 110 -> [165, 110]
    # First index: mean([10, 100]) = 55 -> [165, 110, 55]
    # Reversed: [55, 110, 165]
    check(stats_per_index.values, [55, 110, 165])
    check(stats_per_index.peek(), (55 + 110 + 165) / 3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
