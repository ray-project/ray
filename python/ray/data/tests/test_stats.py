from collections import Counter
import re
import numpy as np

import pytest

import ray
from ray.data._internal.stats import _StatsActor, DatastreamStats
from ray.data._internal.datastream_logger import DatastreamLogger
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.tests.util import column_udf
from ray.tests.conftest import *  # noqa

from unittest.mock import patch


def canonicalize(stats: str) -> str:
    # Dataset UUID expression.
    s0 = re.sub("([a-f\d]{32})", "U", stats)
    # Time expressions.
    s1 = re.sub("[0-9\.]+(ms|us|s)", "T", s0)
    # Handle zero values specially so we can check for missing values.
    s2 = re.sub(" [0]+(\.[0]+)?", " Z", s1)
    # Other numerics.
    s3 = re.sub("[0-9]+(\.[0-9]+)?", "N", s2)
    # Replace tabs with spaces.
    s4 = re.sub("\t", "    ", s3)
    return s4


def dummy_map_batches(x):
    """Dummy function used in calls to map_batches below."""
    return x


def test_dataset_stats_basic(ray_start_regular_shared, enable_auto_log_stats):
    context = DataContext.get_current()
    context.optimize_fuse_stages = True

    if context.new_execution_backend:
        if context.use_streaming_executor:
            logger = DatastreamLogger(
                "ray.data._internal.execution.streaming_executor"
            ).get_logger(
                log_to_stdout=enable_auto_log_stats,
            )
        else:
            logger = DatastreamLogger(
                "ray.data._internal.execution.bulk_executor"
            ).get_logger(
                log_to_stdout=enable_auto_log_stats,
            )
    else:
        logger = DatastreamLogger("ray.data._internal.plan").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )
    with patch.object(logger, "info") as mock_logger:
        ds = ray.data.range(1000, parallelism=10)
        ds = ds.map_batches(dummy_map_batches).materialize()

        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Read->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        ds = ds.map(dummy_map_batches).materialize()
        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Map: N/N blocks executed in T
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
    stats = canonicalize(ds.materialize().stats())

    if context.new_execution_backend:
        if context.use_streaming_executor:
            assert (
                stats
                == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Datastream iterator time breakdown:
* Total time user code is blocked: T
* Total time in user code: T
* Total time overall: T
* Num blocks local: Z
* Num blocks remote: Z
* Num blocks unknown location: N
* Batch iteration time breakdown (summed across prefetch threads):
    * In ray.get(): T min, T max, T avg, T total
    * In batch creation: T min, T max, T avg, T total
    * In batch formatting: T min, T max, T avg, T total
"""
            )
        else:
            assert (
                stats
                == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Datastream iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* Num blocks local: Z
* Num blocks remote: Z
* Num blocks unknown location: N
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
            )
    else:
        if context.use_streaming_executor:
            assert (
                stats
                == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Datastream iterator time breakdown:
* Total time user code is blocked: T
* Total time in user code: T
* Total time overall: T
* Num blocks local: Z
* Num blocks remote: Z
* Num blocks unknown location: N
* Batch iteration time breakdown (summed across prefetch threads):
    * In ray.get(): T min, T max, T avg, T total
    * In batch creation: T min, T max, T avg, T total
    * In batch formatting: T min, T max, T avg, T total
"""
            )
        else:
            assert (
                stats
                == """Stage N Read->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Datastream iterator time breakdown:
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
            )


def test_dataset__repr__(ray_start_regular_shared):
    n = 100
    ds = ray.data.range(n)
    assert len(ds.take_all()) == n
    ds = ds.materialize()

    assert canonicalize(repr(ds._plan.stats().to_summary())) == (
        "DatastreamStatsSummary(\n"
        "   datastream_uuid=U,\n"
        "   base_name=None,\n"
        "   number=N,\n"
        "   extra_metrics={},\n"
        "   stage_stats=[\n"
        "      StageStatsSummary(\n"
        "         stage_name='Read',\n"
        "         is_substage=False,\n"
        "         time_total_s=T,\n"
        "         block_execution_summary_str=N/N blocks executed in T\n"
        "         wall_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         cpu_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         memory={'min': 'T', 'max': 'T', 'mean': 'T'},\n"
        "         output_num_rows={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         output_size_bytes={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"  # noqa: E501
        "         node_count={'min': 'T', 'max': 'T', 'mean': 'T', 'count': 'T'},\n"
        "      ),\n"
        "   ],\n"
        "   iter_stats=IterStatsSummary(\n"
        "      wait_time=T,\n"
        "      get_time=T,\n"
        "      iter_blocks_local=None,\n"
        "      iter_blocks_remote=None,\n"
        "      iter_unknown_location=None,\n"
        "      next_time=T,\n"
        "      format_time=T,\n"
        "      user_time=T,\n"
        "      total_time=T,\n"
        "   ),\n"
        "   parents=[],\n"
        ")"
    )

    ds2 = ds.map_batches(lambda x: x).materialize()
    assert len(ds2.take_all()) == n
    assert canonicalize(repr(ds2._plan.stats().to_summary())) == (
        "DatastreamStatsSummary(\n"
        "   datastream_uuid=U,\n"
        "   base_name=MapBatches(<lambda>),\n"
        "   number=N,\n"
        "   extra_metrics={\n"
        "      obj_store_mem_alloc: N,\n"
        "      obj_store_mem_freed: N,\n"
        "      obj_store_mem_peak: N,\n"
        "   },\n"
        "   stage_stats=[\n"
        "      StageStatsSummary(\n"
        "         stage_name='MapBatches(<lambda>)',\n"
        "         is_substage=False,\n"
        "         time_total_s=T,\n"
        "         block_execution_summary_str=N/N blocks executed in T\n"
        "         wall_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         cpu_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         memory={'min': 'T', 'max': 'T', 'mean': 'T'},\n"
        "         output_num_rows={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "         output_size_bytes={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"  # noqa: E501
        "         node_count={'min': 'T', 'max': 'T', 'mean': 'T', 'count': 'T'},\n"
        "      ),\n"
        "   ],\n"
        "   iter_stats=IterStatsSummary(\n"
        "      wait_time=T,\n"
        "      get_time=T,\n"
        "      iter_blocks_local=None,\n"
        "      iter_blocks_remote=None,\n"
        "      iter_unknown_location=N,\n"
        "      next_time=T,\n"
        "      format_time=T,\n"
        "      user_time=T,\n"
        "      total_time=T,\n"
        "   ),\n"
        "   parents=[\n"
        "      DatastreamStatsSummary(\n"
        "         datastream_uuid=U,\n"
        "         base_name=None,\n"
        "         number=N,\n"
        "         extra_metrics={},\n"
        "         stage_stats=[\n"
        "            StageStatsSummary(\n"
        "               stage_name='Read',\n"
        "               is_substage=False,\n"
        "               time_total_s=T,\n"
        "               block_execution_summary_str=N/N blocks executed in T\n"
        "               wall_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "               cpu_time={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"
        "               memory={'min': 'T', 'max': 'T', 'mean': 'T'},\n"
        "               output_num_rows={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"  # noqa: E501
        "               output_size_bytes={'min': 'T', 'max': 'T', 'mean': 'T', 'sum': 'T'},\n"  # noqa: E501
        "               node_count={'min': 'T', 'max': 'T', 'mean': 'T', 'count': 'T'},\n"  # noqa: E501
        "            ),\n"
        "         ],\n"
        "         iter_stats=IterStatsSummary(\n"
        "            wait_time=T,\n"
        "            get_time=T,\n"
        "            iter_blocks_local=None,\n"
        "            iter_blocks_remote=None,\n"
        "            iter_unknown_location=None,\n"
        "            next_time=T,\n"
        "            format_time=T,\n"
        "            user_time=T,\n"
        "            total_time=T,\n"
        "         ),\n"
        "         parents=[],\n"
        "      ),\n"
        "   ],\n"
        ")"
    )


def test_dataset_stats_shuffle(ray_start_regular_shared):
    context = DataContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.random_shuffle().repartition(1, shuffle=True)
    stats = canonicalize(ds.materialize().stats())
    assert (
        stats
        == """Stage N ReadRange->RandomShuffle: executed in T

    Substage Z ReadRange->RandomShuffleMap: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

    Substage N RandomShuffleReduce: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

Stage N Repartition: executed in T

    Substage Z RepartitionMap: N/N blocks executed
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Peak heap memory usage (MiB): N min, N max, N mean
    * Output num rows: N min, N max, N mean, N total
    * Output size bytes: N min, N max, N mean, N total
    * Tasks per node: N min, N max, N mean; N nodes used

    Substage N RepartitionReduce: N/N blocks executed
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
    stats = ds.materialize().stats()
    assert "Repartition" in stats, stats


def test_dataset_stats_union(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.union(ds)
    stats = ds.materialize().stats()
    assert "Union" in stats, stats


def test_dataset_stats_zip(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.zip(ds)
    stats = ds.materialize().stats()
    assert "Zip" in stats, stats


def test_dataset_stats_sort(ray_start_regular_shared):
    ds = ray.data.range(1000, parallelism=10)
    ds = ds.sort("id")
    stats = ds.materialize().stats()
    assert "SortMap" in stats, stats
    assert "SortReduce" in stats, stats


def test_dataset_stats_from_items(ray_start_regular_shared):
    ds = ray.data.from_items(range(10))
    stats = ds.materialize().stats()
    assert "FromItems" in stats, stats


def test_dataset_stats_read_parquet(ray_start_regular_shared, tmp_path):
    context = DataContext.get_current()
    context.optimize_fuse_stages = True
    ds = ray.data.range(1000, parallelism=10)
    ds.write_parquet(str(tmp_path))
    ds = ray.data.read_parquet(str(tmp_path)).map(lambda x: x)
    stats = canonicalize(ds.materialize().stats())
    if context.new_execution_backend:
        assert (
            stats
            == """Stage N ReadParquet->Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
        )
    else:
        assert (
            stats
            == """Stage N Read->Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
        )


def test_dataset_split_stats(ray_start_regular_shared, tmp_path):
    context = DataContext.get_current()
    ds = ray.data.range(100, parallelism=10).map(column_udf("id", lambda x: x + 1))
    dses = ds.split_at_indices([49])
    dses = [ds.map(column_udf("id", lambda x: x + 1)) for ds in dses]
    for ds_ in dses:
        stats = canonicalize(ds_.materialize().stats())

        if context.new_execution_backend:
            assert (
                stats
                == """Stage N ReadRange->Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Split: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
            )
        else:
            assert (
                stats
                == """Stage N Read->Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N Split: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
            )


def test_dataset_pipeline_stats_basic(ray_start_regular_shared, enable_auto_log_stats):
    context = DataContext.get_current()
    context.optimize_fuse_stages = True

    if context.new_execution_backend:
        if context.use_streaming_executor:
            logger = DatastreamLogger(
                "ray.data._internal.execution.streaming_executor"
            ).get_logger(
                log_to_stdout=enable_auto_log_stats,
            )
        else:
            logger = DatastreamLogger(
                "ray.data._internal.execution.bulk_executor"
            ).get_logger(
                log_to_stdout=enable_auto_log_stats,
            )
    else:
        logger = DatastreamLogger("ray.data._internal.plan").get_logger(
            log_to_stdout=enable_auto_log_stats,
        )

    with patch.object(logger, "info") as mock_logger:
        ds = ray.data.range(1000, parallelism=10)
        ds = ds.map_batches(dummy_map_batches).materialize()

        if enable_auto_log_stats:
            logger_args, logger_kwargs = mock_logger.call_args

            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Read->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
"""
                )

        pipe = ds.repeat(5)
        pipe = pipe.map(dummy_map_batches)
        if enable_auto_log_stats:
            # Stats only include first stage, and not for pipelined map
            logger_args, logger_kwargs = mock_logger.call_args
            if context.new_execution_backend:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Read->MapBatches(dummy_map_batches): N/N blocks executed in T
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
                    == """Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}
"""
                )
            else:
                assert (
                    canonicalize(logger_args[0])
                    == """Stage N Map: N/N blocks executed in T
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
Stage N ReadRange->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N ReadRange->MapBatches(dummy_map_batches): [execution cached]
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N ReadRange->MapBatches(dummy_map_batches): [execution cached]
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next datastream: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next datastream: T
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
Stage N Read->MapBatches(dummy_map_batches): N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N Read->MapBatches(dummy_map_batches): [execution cached]

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N Read->MapBatches(dummy_map_batches): [execution cached]

Stage N Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next datastream: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next datastream: T
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

    # CACHED (called cache()).
    ds = ray.data.range(10).materialize().repeat(2).map_batches(lambda x: x)
    ds.take(999)
    stats = ds.stats()
    assert "[execution cached]" in stats

    # CACHED (eager map stage).
    ds = ray.data.range(10).map_batches(dummy_map_batches).repeat(2)
    ds.take(999)
    stats = ds.stats()
    assert "[execution cached]" in stats
    assert "ReadRange->MapBatches(dummy_map_batches)" in stats


def test_dataset_pipeline_split_stats_basic(ray_start_regular_shared):
    context = DataContext.get_current()
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
        print("XXX stats:", canonicalize(stats[0]))
        assert (
            canonicalize(stats[0])
            == """== Pipeline Window Z ==
Stage N ReadRange: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

== Pipeline Window N ==
Stage N ReadRange: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: {'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, \
'obj_store_mem_peak': N}

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next datastream: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next datastream: T
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
Stage N Read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

== Pipeline Window N ==
Stage N Read: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used

##### Overall Pipeline Time Breakdown #####
* Time stalled waiting for next datastream: T min, T max, T mean, T total

DatasetPipeline iterator time breakdown:
* Waiting for next datastream: T
* In ray.wait(): T
* In ray.get(): T
* In next_batch(): T
* In format_batch(): T
* In user code: T
* Total time: T
"""
        )


def test_calculate_blocks_stats(ray_start_regular_shared, stage_two_block):
    context = DataContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatastreamStats(
        stages={"Read": block_meta_list},
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
    context = DataContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatastreamStats(
        stages={"Read": block_meta_list},
        parent=None,
    )
    stats.datastream_uuid = "test-uuid"

    calculated_stats = stats.to_summary()
    summarized_lines = calculated_stats.to_string().split("\n")

    assert (
        "Stage 0 Read: 2/2 blocks executed in {}s".format(
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
    `DatastreamStats.get_max_wall_time()`,
    `DatastreamStats.get_total_cpu_time()`,
    `DatastreamStats.get_max_heap_memory()`."""
    context = DataContext.get_current()
    context.optimize_fuse_stages = True

    block_params, block_meta_list = stage_two_block
    stats = DatastreamStats(
        stages={"Read": block_meta_list},
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


def test_streaming_stats_full(ray_start_regular_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    ds = ray.data.range(5, parallelism=5).map(column_udf("id", lambda x: x + 1))
    ds.take_all()
    stats = canonicalize(ds.stats())
    assert (
        stats
        == """Stage N ReadRange->Map: N/N blocks executed in T
* Remote wall time: T min, T max, T mean, T total
* Remote cpu time: T min, T max, T mean, T total
* Peak heap memory usage (MiB): N min, N max, N mean
* Output num rows: N min, N max, N mean, N total
* Output size bytes: N min, N max, N mean, N total
* Tasks per node: N min, N max, N mean; N nodes used
* Extra metrics: \
{'obj_store_mem_alloc': N, 'obj_store_mem_freed': N, 'obj_store_mem_peak': N}

Datastream iterator time breakdown:
* Total time user code is blocked: T
* Total time in user code: T
* Total time overall: T
* Num blocks local: Z
* Num blocks remote: Z
* Num blocks unknown location: N
* Batch iteration time breakdown (summed across prefetch threads):
    * In ray.get(): T min, T max, T avg, T total
    * In batch creation: T min, T max, T avg, T total
    * In batch formatting: T min, T max, T avg, T total
"""
    )


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


def test_stats_actor_cap_num_stats(ray_start_cluster):
    actor = _StatsActor.remote(3)
    metadatas = []
    task_idx = 0
    for uuid in range(3):
        metadatas.append(
            BlockMetadata(
                num_rows=uuid,
                size_bytes=None,
                schema=None,
                input_files=None,
                exec_stats=None,
            )
        )
        num_stats = uuid + 1
        actor.record_start.remote(uuid)
        assert ray.get(actor._get_stats_dict_size.remote()) == (
            num_stats,
            num_stats - 1,
            num_stats - 1,
        )
        actor.record_task.remote(uuid, task_idx, [metadatas[-1]])
        assert ray.get(actor._get_stats_dict_size.remote()) == (
            num_stats,
            num_stats,
            num_stats,
        )
    for uuid in range(3):
        assert ray.get(actor.get.remote(uuid))[0][task_idx] == [metadatas[uuid]]
    # Add the fourth stats to exceed the limit.
    actor.record_start.remote(3)
    # The first stats (with uuid=0) should have been purged.
    assert ray.get(actor.get.remote(0))[0] == {}
    # The start_time has 3 entries because we just added it above with record_start().
    assert ray.get(actor._get_stats_dict_size.remote()) == (3, 2, 2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
