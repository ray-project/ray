import random
import time
from typing import List, Tuple

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.aggregate import Percentile


@pytest.mark.parametrize("num_parts", [1, 10])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_percentile(ray_start_regular_shared, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_percentile with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global percentile aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    
    # Test single percentile
    assert ds.percentile(50, "A") == 49.5
    
    # Test multiple percentiles
    percentiles = ds.percentile([25, 50, 75], "A")
    assert len(percentiles) == 3
    assert percentiles[0] == 24.75  # 25th percentile
    assert percentiles[1] == 49.5   # 50th percentile
    assert percentiles[2] == 74.25  # 75th percentile
    
    # Test different interpolation methods
    assert ds.percentile(50, "A", interpolation="linear") == 49.5
    assert ds.percentile(50, "A", interpolation="lower") == 49
    assert ds.percentile(50, "A", interpolation="higher") == 50
    assert ds.percentile(50, "A", interpolation="nearest") == 50
    assert ds.percentile(50, "A", interpolation="midpoint") == 49.5
    
    # Test empty dataset
    empty_ds = ds.filter(lambda r: r["A"] > 100)
    assert empty_ds.percentile(50, "A") is None
    
    # Test dataset with nulls
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    
    # By default, nulls should be ignored
    assert nan_ds.percentile(50, "A") == 49.5
    
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.percentile(50, "A", ignore_nulls=False))
    
    # Test all nulls
    all_null_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        all_null_ds = _to_pandas(all_null_ds)
    assert pd.isnull(all_null_ds.percentile(50, "A"))


@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_describe(ray_start_regular_shared, ds_format):
    xs = list(range(100))
    
    # Create dataset with known values for easy testing
    ds = ray.data.from_items([{
        "A": x,
        "B": x ** 2
    } for x in xs])
    
    if ds_format == "pandas":
        ds = ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")
    
    # Test describe without specifying columns (should use all columns)
    stats = ds.describe()
    
    # Verify we get stats for all columns
    assert "A" in stats
    assert "B" in stats
    
    # Verify A column stats
    assert stats["A"]["count"] == 100
    assert stats["A"]["mean"] == 49.5
    assert abs(stats["A"]["std"] - 28.86607004772212) < 1e-10
    assert stats["A"]["min"] == 0
    assert stats["A"]["max"] == 99
    assert stats["A"]["25%"] == 24.75
    assert stats["A"]["50%"] == 49.5
    assert stats["A"]["75%"] == 74.25
    
    # Verify B column stats - should be squares
    assert stats["B"]["min"] == 0
    assert stats["B"]["max"] == 99**2
    assert stats["B"]["mean"] == sum(x**2 for x in range(100)) / 100
    
    # Test describe with specific columns
    stats_a = ds.describe("A")
    assert "A" in stats_a
    assert "B" not in stats_a
    
    # Test with custom percentiles
    custom_stats = ds.describe("A", percentiles=[10, 90])
    assert "10%" in custom_stats["A"]
    assert "90%" in custom_stats["A"]
    assert "25%" not in custom_stats["A"]
    assert "75%" not in custom_stats["A"]
    
    # Test with dataset containing nulls
    null_ds = ray.data.from_items([{
        "A": None if i % 10 == 0 else i,
        "B": i ** 2
    } for i in range(100)])
    
    if ds_format == "pandas":
        null_ds = null_ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")
    
    null_stats = null_ds.describe()
    
    # Should still work with ignore_nulls=True (default)
    assert null_stats["A"]["count"] == 100  # we count all rows, even with nulls
    assert null_stats["A"]["mean"] != 49.5  # mean should be different with nulls ignored
    
    # Test empty dataset
    empty_ds = ds.filter(lambda r: r["A"] > 100)
    empty_stats = empty_ds.describe()
    assert empty_stats["A"]["count"] == 0


def test_percentile_aggregation():
    """Test the Percentile aggregation class directly."""
    # Test single percentile
    p = Percentile("col", q=50)
    assert p._qs == [0.5]  # Should convert 50 to 0.5
    
    # Test multiple percentiles
    p = Percentile("col", q=[25, 50, 75])
    assert p._qs == [0.25, 0.5, 0.75]  # Should convert all to 0-1 range
    
    # Test different interpolation methods
    p1 = Percentile("col", q=50, interpolation="linear")
    p2 = Percentile("col", q=50, interpolation="lower")
    p3 = Percentile("col", q=50, interpolation="higher")
    p4 = Percentile("col", q=50, interpolation="nearest")
    p5 = Percentile("col", q=50, interpolation="midpoint")
    
    assert p1._interpolation == "linear"
    assert p2._interpolation == "lower"
    assert p3._interpolation == "higher"
    assert p4._interpolation == "nearest"
    assert p5._interpolation == "midpoint"
