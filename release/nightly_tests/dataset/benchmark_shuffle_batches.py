"""Compare shuffle_batches with a map_batches/PyArrow take UDF.

Run on a single CPU node with online range_tensor input (4 KiB per row), parallel
NumPy consumers, and no model computation. The exact-window baseline constructs the
same consecutive windows before calling PyArrow take. Each measured execution
excludes iterator warmup; a separate untimed execution validates every payload.
Set RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE before starting to measure its effect on
the same batch-shuffle pipelines. The native baselines do not use that flag.
"""

import argparse
import os
import statistics
import time

import numpy as np
import pyarrow as pa
from benchmark import Benchmark

import ray
from ray.data import TaskPoolStrategy
from ray.data._internal.tensor_extensions.chunked_tensor_take import (
    ENABLE_CHUNKED_TENSOR_TAKE,
)

BASELINES = {
    "map_batches_native_take": "map_batches_native_take",
    "shuffle_batches": "map_batches_native_take",
    "exact_windows_native_take": "exact_windows_native_take",
    "exact_windows_shuffle_batches": "exact_windows_native_take",
}


def native_shuffle(batch: pa.Table) -> pa.Table:
    return batch.take(np.random.permutation(len(batch)))


@ray.remote(num_cpus=1)
class Consumer:
    def consume(self, shard, args, validate_payload):
        iterator = iter(
            shard.iter_batches(
                batch_size=args.batch_size, batch_format="numpy", prefetch_batches=1
            )
        )
        row_ids = []
        for step in range(args.warmup_batches + args.measured_batches):
            if step == args.warmup_batches:
                started = time.perf_counter()
            values = next(iterator)["data"]
            finished = time.perf_counter()
            assert values.shape == (args.batch_size, 512)
            assert values.dtype == np.int64
            ids = values[:, 0].copy()
            np.testing.assert_array_equal(values[:, -1], ids)
            if validate_payload:
                assert np.all(values == ids[:, None])
            row_ids.append(ids)
        assert next(iterator, None) is None
        return {
            "started": started,
            "finished": finished,
            "ids": np.concatenate(row_ids),
            "stats": shard.stats(),
        }


def run_pipeline(args, variant, *, validate_payload=False):
    rows = (
        args.workers * (args.warmup_batches + args.measured_batches) * args.batch_size
    )
    dataset = ray.data.range_tensor(rows, shape=(512,), override_num_blocks=args.blocks)
    compute = TaskPoolStrategy(size=args.concurrency)
    exact = variant in ("exact_windows_shuffle_batches", "exact_windows_native_take")
    if exact:
        dataset = dataset.repartition(
            target_num_rows_per_block=args.shuffle_batch_size, strict=True
        )
    batch_size = None if exact else args.shuffle_batch_size
    if variant in ("shuffle_batches", "exact_windows_shuffle_batches"):
        dataset = dataset.shuffle_batches(
            batch_size=batch_size,
            seed=42,
            compute=compute,
            num_cpus=1,
        )
    else:
        dataset = dataset.map_batches(
            native_shuffle,
            batch_size=batch_size,
            batch_format="pyarrow",
            zero_copy_batch=True,
            compute=compute,
            num_cpus=1,
        )
    consumers = [Consumer.remote() for _ in range(args.workers)]
    try:
        shards = dataset.streaming_split(args.workers, equal=True)
        results = ray.get(
            [
                consumer.consume.remote(shard, args, validate_payload)
                for consumer, shard in zip(consumers, shards)
            ]
        )
        np.testing.assert_array_equal(
            np.sort(np.concatenate([result["ids"] for result in results])),
            np.arange(rows),
        )
        # All consumers share the node's monotonic clock. Include the entire
        # measured interval, including any skew between consumer warmups.
        elapsed = max(r["finished"] for r in results) - min(
            r["started"] for r in results
        )
        return elapsed, [r["stats"] for r in results]
    finally:
        for consumer in consumers:
            ray.kill(consumer)


def measure(args, variant):
    durations = []
    for _ in range(args.repeats):
        elapsed, stats = run_pipeline(args, variant)
        durations.append(elapsed)
    median = statistics.median(durations)
    return {
        "parameters": vars(args),
        "cluster_resources": ray.cluster_resources(),
        "native_ray_commit": ray.__commit__,
        "pyarrow": pa.__version__,
        "numpy": np.__version__,
        "chunked_tensor_take_enabled": ENABLE_CHUNKED_TENSOR_TAKE,
        "baseline": BASELINES[variant],
        "iteration_times_s": durations,
        "median_iteration_s": median,
        "stdev_iteration_s": statistics.stdev(durations) if len(durations) > 1 else 0,
        "rows_per_s": args.workers * args.measured_batches * args.batch_size / median,
        "last_iteration_stats": stats,
        "scope": "single-node CPU ingestion; online 4-KiB tensors; excludes training",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--blocks", type=int, default=160)
    parser.add_argument("--shuffle-batch-size", type=int, default=262144)
    parser.add_argument("--warmup-batches", type=int, default=100)
    parser.add_argument("--measured-batches", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--variant", choices=BASELINES)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--num-cpus", type=int)
    args = parser.parse_args()
    if (
        min(
            args.workers,
            args.batch_size,
            args.blocks,
            args.shuffle_batch_size,
            args.warmup_batches,
            args.measured_batches,
            args.repeats,
        )
        <= 0
    ):
        parser.error("all sizes, batch counts, and repeats must be positive")
    if args.concurrency is not None and args.concurrency <= 0:
        parser.error("--concurrency must be positive")

    ray.init(
        num_cpus=args.num_cpus,
        runtime_env={
            "env_vars": {
                "RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE": os.environ.get(
                    "RAY_DATA_ENABLE_CHUNKED_TENSOR_TAKE", "1"
                )
            }
        },
    )
    benchmark = Benchmark()
    try:
        if len([node for node in ray.nodes() if node["Alive"]]) != 1:
            raise ValueError("This benchmark requires a single-node cluster")
        if ray.cluster_resources().get("CPU", 0) < args.workers + 1:
            raise ValueError(
                "Reserve at least one CPU for Data tasks besides consumers"
            )
        ray.data.DataContext.get_current().enable_progress_bars = False
        names = (
            [args.variant]
            if args.variant
            else np.random.default_rng(0).permutation(list(BASELINES))
        )
        for name in names:
            _, warmup_stats = run_pipeline(args, name, validate_payload=True)
            benchmark.run_fn(name, measure, args, name)
            benchmark.result[name]["warmup_stats"] = warmup_stats
        for result in benchmark.result.values():
            baseline = benchmark.result.get(result["baseline"])
            if baseline is not None:
                result["speedup_vs_native_baseline"] = (
                    result["rows_per_s"] / baseline["rows_per_s"]
                )
    finally:
        benchmark.write_result()
        ray.shutdown()


if __name__ == "__main__":
    main()
