import pytest
import re

import ray
from ray.tests.conftest import *  # noqa


def canonicalize(stats: str) -> str:
    # Time expressions.
    s1 = re.sub("[0-9\.]+(ms|us|s)", "T", stats)
    # Handle zero values specially so we can check for missing values.
    s2 = re.sub(" [0]+(\.[0]+)?", " Z", s1)
    # Other numerics.
    s3 = re.sub("[0-9]+(\.[0-9]+)?", "N", s2)
    return s3


def test_dataset_stats_basic(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.map_batches(lambda x: x)
    ds = ds.map(lambda x: x)
    for batch in ds.iter_batches():
        pass
    stats = canonicalize(ds.stats())
    assert (
        stats
        == """Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
    )


def test_dataset_stats_shuffle(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.random_shuffle().repartition(1, shuffle=True)
    stats = canonicalize(ds.stats())
    assert (
        stats
        == """Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N random_shuffle_map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N random_shuffle_reduce: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N repartition_map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N repartition_reduce: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
    )


def test_dataset_stats_repartition(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.repartition(1, shuffle=False)
    stats = ds.stats()
    assert "repartition" in stats, stats


def test_dataset_stats_union(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.union(ds)
    stats = ds.stats()
    assert "union" in stats, stats


def test_dataset_stats_zip(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.zip(ds)
    stats = ds.stats()
    assert "zip" in stats, stats


def test_dataset_stats_sort(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.sort()
    stats = ds.stats()
    assert "sort_map" in stats, stats
    assert "sort_merge" in stats, stats


def test_dataset_stats_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(range(10))
    stats = ds.stats()
    assert "from_items" in stats, stats


def test_dataset_stats_read_parquet(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(1000, parallelism=10)
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path)).map(lambda x: x)
    stats = canonicalize(ds.stats())
    print(stats)
    assert (
        stats
        == """Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
    )


def test_dataset_pipeline_stats_basic(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.map_batches(lambda x: x)
    pipe = ds.repeat(5)
    pipe = pipe.map(lambda x: x)
    for batch in pipe.iter_batches():
        pass
    stats = canonicalize(pipe.stats())
    assert (
        stats
        == """== Pipeline Window N ==
Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T

== Pipeline Window N ==
Stage Z read: [execution cached]
Stage N map_batches: [execution cached]
Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T

== Pipeline Window N ==
Stage Z read: [execution cached]
Stage N map_batches: [execution cached]
Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total
* Time in dataset iterator: T
* Time in user code: T
* Total time: T
"""
    )


def test_dataset_pipeline_split_stats_basic(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    pipe = ds.repeat(2)

    @ray.remote
    def consume(split):
        for batch in split.iter_batches():
            pass
        return split.stats()

    s0, s1 = pipe.split(2)
    stats = ray.get([consume.remote(s0), consume.remote(s1)])
    assert (
        canonicalize(stats[0])
        == """== Pipeline Window Z ==
Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T

== Pipeline Window N ==
Stage Z read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In format_batch(): T
* In user code: T
* Total time: T

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total
* Time in dataset iterator: T
* Time in user code: T
* Total time: T
"""
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
