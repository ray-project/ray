from collections import Counter
import re
import numpy as np

import pytest

import ray
from ray.data._internal.stats import DatasetStats
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data.context import DatasetContext
from ray.tests.conftest import *  # noqa

from unittest.mock import patch


def canonicalize(stats: str) -> str:
    # Time expressions.
    s1 = re.sub("[0-9\.]+(ms|us|s)", "T", stats)
    # Handle zero values specially so we can check for missing values.
    s2 = re.sub(" [0]+(\.[0]+)?", " Z", s1)
    # Other numerics.
    s3 = re.sub("[0-9]+(\.[0-9]+)?", "N", s2)
    # Replace tabs with spaces.
    s4 = re.sub("\t", "    ", s3)
    # Drop the extra metrics line.
    s5 = "\n".join(x for x in s4.split("\n") if "Extra metrics: " not in x)
    return s5


def test_dataset_stats_basic(ray_start_regular_shared, enable_auto_log_stats):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True

    if context.new_execution_backend:
        logger = DatasetLogger("ray.data._internal.execution.bulk_executor").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )
    else:
        logger = DatasetLogger("ray.data._internal.plan").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )
    with patch.object(logger, "info") as mock_logger:
        ds = ray.data.range(1000, parallelism=10)
        ds = ds.map_batches(lambda x: x).fully_executed()

        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        ds = ds.map(lambda x: x).fully_executed()
        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )
    for batch in ds.iter_batches():
        pass
    stats = canonicalize(ds.stats())

    if context.new_execution_backend:
        assert (
            stats
            == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
        )
    else:
        assert (
            stats
            == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Dataset iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
        )


def test_dataset_stats_shuffle(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.random_shuffle().repartition(1, shuffle=True)
    stats = canonicalize(ds.stats())
    assert (
        stats
        == """Stage N read->random_shuffle: executed in T

    Substage Z read->random_shuffle_map: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

    Substage N random_shuffle_reduce: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

Stage N repartition: executed in T

    Substage Z repartition_map: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

    Substage N repartition_reduce: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
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
    assert "sort_reduce" in stats, stats


def test_dataset_stats_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(range(10))
    stats = ds.stats()
    assert "from_items" in stats, stats


def test_dataset_stats_read_parquet(ray_start_regular_shared, tmp_path):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(1000, parallelism=10)
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path)).map(lambda x: x)
    stats = canonicalize(ds.stats())
    if context.new_execution_backend:
        assert (
            stats
            == """Stage N read->map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
        )
    else:
        assert (
            stats
            == """Stage N read->map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
        )


def test_dataset_split_stats(ray_start_regular_shared, tmp_path):
    context = DatasetContext.get_current()
    ds = ray.data.range(100, parallelism=10).map(lambda x: x + 1)
    dses = ds.split_at_indices([49])
    dses = [ds.map(lambda x: x + 1) for ds in dses]
    for ds_ in dses:
        stats = canonicalize(ds_.stats())

        if context.new_execution_backend:
            assert (
                stats
                == """Stage N read->map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

Stage N split: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
            )
        else:
            assert (
                stats
                == """Stage N read->map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N split: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
            )


def test_dataset_pipeline_stats_basic(ray_start_regular_shared, enable_auto_log_stats):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True

    if context.new_execution_backend:
        logger = DatasetLogger("ray.data._internal.execution.bulk_executor").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )
    else:
        logger = DatasetLogger("ray.data._internal.plan").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )

    with patch.object(logger, "info") as mock_logger:
        ds = ray.data.range(1000, parallelism=10)
        ds = ds.map_batches(lambda x: x).fully_executed()

        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        pipe = ds.repeat(5)
        pipe = pipe.map(lambda x: x)
        if enable_auto_log_stats:
            # Stats only include first stage, and not for pipelined map
            logger_args, logger_kwargs = mock_logger.call_args
            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        stats = canonicalize(pipe.stats())
        assert "No stats available" in stats, stats
        for batch in pipe.iter_batches():
            pass

        if enable_auto_log_stats:
            # Now stats include the pipelined map stage
            logger_args, logger_kwargs = mock_logger.call_args
            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        stats = canonicalize(pipe.stats())
        if context.new_execution_backend:
            assert (
                stats
                == """== Pipeline Window N ==
Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N read->map_batches: [execution cached]

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N read->map_batches: [execution cached]

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next dataset: T
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
            )
        else:
            assert (
                stats
                == """== Pipeline Window N ==
Stage N read->map_batches: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N read->map_batches: [execution cached]

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N read->map_batches: [execution cached]

Stage N map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next dataset: T
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
            )


def test_dataset_pipeline_cache_cases(ray_start_regular_shared):
    # NOT CACHED (lazy read stage).
    ds = ray.data.range(10).repeat(2).map_batches(lambda x: x)
    ds.take(999)
    stats = ds.stats()
    assert "[execution cached]" not in stats

    # CACHED (called fully_executed()).
    ds = ray.data.range(10).fully_executed().repeat(2).map_batches(lambda x: x)
    ds.take(999)
    stats = ds.stats()
    assert "[execution cached]" in stats

    # CACHED (eager map stage).
    ds = ray.data.range(10).map_batches(lambda x: x).repeat(2)
    ds.take(999)
    stats = ds.stats()
    assert "[execution cached]" in stats
    assert "read->map_batches" in stats


def test_dataset_pipeline_split_stats_basic(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(1000, parallelism=10)
    pipe = ds.repeat(2)

    @ray.remote
    def consume(split):
        for batch in split.iter_batches():
            pass
        return split.stats()

    s0, s1 = pipe.split(2)
    stats = ray.get([consume.remote(s0), consume.remote(s1)])
    if context.new_execution_backend:
        assert (
            canonicalize(stats[0])
            == """== Pipeline Window Z ==
Stage N read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': Z, \
'obj_store_mem_peak': N}

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next dataset: T
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
        )
    else:
        assert (
            canonicalize(stats[0])
            == """== Pipeline Window Z ==
Stage N read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next dataset: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next dataset: T
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
        )


def test_calculate_blocks_stats(ray_start_regular_shared, stage_two_block):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatasetStats(
        stages={"read": block_meta_list},
        parent=None,
    )
    calculated_stats = stats.to_summary().stages_stats[0]

    assert calculated_stats.output_num_rows == {
        "min": min(block_params["num_rows"]),
        "max": max(block_params["num_rows"]),
        "mean": np.mean(block_params["num_rows"]),
        "sum": sum(block_params["num_rows"]),
    }
    assert calculated_stats.output_size_bytes == {
        "min": min(block_params["size_bytes"]),
        "max": max(block_params["size_bytes"]),
        "mean": np.mean(block_params["size_bytes"]),
        "sum": sum(block_params["size_bytes"]),
    }
    assert calculated_stats.wall_time == {
        "min": min(block_params["wall_time"]),
        "max": max(block_params["wall_time"]),
        "mean": np.mean(block_params["wall_time"]),
        "sum": sum(block_params["wall_time"]),
    }
    assert calculated_stats.cpu_time == {
        "min": min(block_params["cpu_time"]),
        "max": max(block_params["cpu_time"]),
        "mean": np.mean(block_params["cpu_time"]),
        "sum": sum(block_params["cpu_time"]),
    }

    node_counts = Counter(block_params["node_id"])
    assert calculated_stats.node_count == {
        "min": min(node_counts.values()),
        "max": max(node_counts.values()),
        "mean": np.mean(list(node_counts.values())),
        "count": len(node_counts),
    }


def test_summarize_blocks(ray_start_regular_shared, stage_two_block):
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatasetStats(
        stages={"read": block_meta_list},
        parent=None,
    )
    stats.dataset_uuid = "test-uuid"

    calculated_stats = stats.to_summary()
    summarized_lines = calculated_stats.to_string().split("\n")

    assert (
        "Stage 0 read: 2/2 blocks executed in {}s".format(
            max(round(stats.time_total_s, 2), 0)
        )
        == summarized_lines[0]
    )
    assert (
        "* Remote wall time: {}s min, {}s max, {}s mean, {}s total".format(
            min(block_params["wall_time"]),
            max(block_params["wall_time"]),
            np.mean(block_params["wall_time"]),
            sum(block_params["wall_time"]),
        )
        == summarized_lines[1]
    )
    assert (
        "* Remote cpu time: {}s min, {}s max, {}s mean, {}s total".format(
            min(block_params["cpu_time"]),
            max(block_params["cpu_time"]),
            np.mean(block_params["cpu_time"]),
            sum(block_params["cpu_time"]),
        )
        == summarized_lines[2]
    )
    assert (
        "* Peak heap memory usage (MiB): {} min, {} max, {} mean".format(
            min(block_params["max_rss_bytes"]) / (1024 * 1024),
            max(block_params["max_rss_bytes"]) / (1024 * 1024),
            int(np.mean(block_params["max_rss_bytes"]) / (1024 * 1024)),
        )
        == summarized_lines[3]
    )
    assert (
        "* Output num rows: {} min, {} max, {} mean, {} total".format(
            min(block_params["num_rows"]),
            max(block_params["num_rows"]),
            int(np.mean(block_params["num_rows"])),
            sum(block_params["num_rows"]),
        )
        == summarized_lines[4]
    )
    assert (
        "* Output size bytes: {} min, {} max, {} mean, {} total".format(
            min(block_params["size_bytes"]),
            max(block_params["size_bytes"]),
            int(np.mean(block_params["size_bytes"])),
            sum(block_params["size_bytes"]),
        )
        == summarized_lines[5]
    )

    node_counts = Counter(block_params["node_id"])
    assert (
        "* Tasks per node: {} min, {} max, {} mean; {} nodes used".format(
            min(node_counts.values()),
            max(node_counts.values()),
            int(np.mean(list(node_counts.values()))),
            len(node_counts),
        )
        == summarized_lines[6]
    )


def test_get_total_stats(ray_start_regular_shared, stage_two_block):
    """Tests a set of similar getter methods which pull aggregated
    statistics values after calculating stage-level stats:
    `DatasetStats.get_max_wall_time()`,
    `DatasetStats.get_total_cpu_time()`,
    `DatasetStats.get_max_heap_memory()`."""
    context = DatasetContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatasetStats(
        stages={"read": block_meta_list},
        parent=None,
    )

    dataset_stats_summary = stats.to_summary()
    stage_stats = dataset_stats_summary.stages_stats[0]
    wall_time_stats = stage_stats.wall_time
    assert dataset_stats_summary.get_total_wall_time() == wall_time_stats.get("max")

    cpu_time_stats = stage_stats.cpu_time
    assert dataset_stats_summary.get_total_cpu_time() == cpu_time_stats.get("sum")

    peak_memory_stats = stage_stats.memory
    assert dataset_stats_summary.get_max_heap_memory() == peak_memory_stats.get("max")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
