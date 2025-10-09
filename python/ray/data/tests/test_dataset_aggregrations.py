import math
import random
import sys
import time

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
)
from ray.tests.conftest import *  # noqa


def test_count(ray_start_regular):
    ds = ray.data.range(100, override_num_blocks=10)
    # We do not kick off the read task by default.
    assert not ds._plan.has_started_execution
    assert ds.count() == 100
    # Getting number of rows should not trigger execution of any read tasks
    # for ray.data.range(), as the number of rows is known beforehand.
    assert not ds._plan.has_started_execution

    assert_core_execution_metrics_equals(CoreExecutionMetrics(task_count={}))


def test_count_edge_case(ray_start_regular):
    # Test this edge case: https://github.com/ray-project/ray/issues/44509.
    ds = ray.data.range(10)
    ds.count()

    actual_count = ds.filter(lambda row: row["id"] % 2 == 0).count()

    assert actual_count == 5


def test_count_after_caching_after_execution(ray_start_regular):
    SCALE_FACTOR = 5
    FILE_ROW_COUNT = 150
    DS_ROW_COUNT = FILE_ROW_COUNT * SCALE_FACTOR
    paths = ["example://iris.csv"] * SCALE_FACTOR
    ds = ray.data.read_csv(paths)
    # Row count should be unknown before execution.
    assert "num_rows=?" in str(ds)
    # After iterating over bundles and completing execution, row count should be known.
    list(ds.iter_internal_ref_bundles())
    assert f"num_rows={DS_ROW_COUNT}" in str(ds)
    assert ds.count() == DS_ROW_COUNT
    assert ds._plan._snapshot_metadata_schema.metadata.num_rows == DS_ROW_COUNT


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_min(ray_start_regular_shared_2_cpus, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global min aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.min("A") == 0

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).min("id") is None

    # Test built-in global min aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.min("A") == 0
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.min("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.min("A"))
    assert pd.isnull(nan_ds.min("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_max(ray_start_regular_shared_2_cpus, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.max("A") == 99

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).max("id") is None

    # Test built-in global max aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.max("A") == 99
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.max("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.max("A"))
    assert pd.isnull(nan_ds.max("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_mean(ray_start_regular_shared_2_cpus, ds_format, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global mean aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.mean("A") == 49.5

    # Test empty dataset
    # Note: we explicitly set parallelism here to ensure there are no empty
    # input blocks.
    ds = ray.data.range(10, override_num_blocks=10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).mean("id") is None

    # Test built-in global mean aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.mean("A") == 49.5
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.mean("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.mean("A"))
    assert pd.isnull(nan_ds.mean("A", ignore_nulls=False))


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_std(ray_start_regular_shared_2_cpus, ds_format, num_parts):
    # NOTE: Do not change the seed
    seed = 1740035705

    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_arrow(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global max aggregation
    df = pd.DataFrame({"A": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert math.isclose(ds.std("A"), df["A"].std())
    assert math.isclose(ds.std("A", ddof=0), df["A"].std(ddof=0))

    # Test empty dataset
    ds = ray.data.from_pandas(pd.DataFrame({"A": []}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert pd.isnull(ds.std("A"))
    # Test edge cases
    ds = ray.data.from_pandas(pd.DataFrame({"A": [3]}))
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    assert np.isnan(ds.std("A"))

    # Test built-in global std aggregation with nans
    nan_df = pd.DataFrame({"A": xs + [None]})
    nan_ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        nan_ds = _to_arrow(nan_ds)
    assert math.isclose(nan_ds.std("A"), nan_df["A"].std())
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.std("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert pd.isnull(nan_ds.std("A"))
    assert pd.isnull(nan_ds.std("A", ignore_nulls=False))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
