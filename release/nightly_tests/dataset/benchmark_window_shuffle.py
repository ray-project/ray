"""Compare complete single-node window-shuffle pipelines.

The Parquet source includes reading, shuffling, and streaming consumption. The
cached source isolates shuffle execution. Input generation and full payload
validation are untimed. Model computation is excluded.
"""

import argparse
import gc
from pathlib import Path
import statistics
import tempfile
import time
from typing import Callable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from benchmark import Benchmark

import ray
from ray.data import TaskPoolStrategy
from ray.data._internal.arrow_ops.transform_pyarrow import take_table
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext

VARIANTS = (
    "map_batches_native_take",
    "strict_windows_native_take",
    "map_batches_ray_take",
    "strict_windows_ray_take",
    "window_shuffle_default",
    "window_shuffle_strict",
)


def native_shuffle(batch: pa.Table) -> pa.Table:
    # Match the documented UDF recipe. The process RNG advances between batches,
    # unlike a fresh task-only seed, which repeats equal-sized permutations.
    return batch.take(np.random.permutation(len(batch)))


def ray_shuffle(batch: pa.Table) -> pa.Table:
    return take_table(batch, np.random.permutation(len(batch)))


def make_table(start: int, end: int, tensor_kind: str) -> pa.Table:
    ids = np.arange(start, end)
    table = pa.table({"id": ids})
    if tensor_kind == "scalar":
        return table.append_column("value", pa.array(ids * 2))
    if tensor_kind == "variable":
        data = np.empty(len(ids), dtype=object)
        for j, row_id in enumerate(ids):
            data[j] = np.full(
                (64 if row_id % 2 == 0 else 128, 64), row_id, dtype=np.float32
            )
    else:
        data = np.broadcast_to(
            ids.astype(np.float32)[:, None, None], (len(ids), 64, 64)
        ).copy()
    return table.append_column("tensor", ArrowTensorArray.from_numpy(data))


def consume(
    dataset: ray.data.Dataset,
    *,
    validate_payload: bool = False,
    tensor_kind: str = "fixed",
) -> list[int]:
    ids = []
    for batch in dataset.iter_batches(batch_format="pyarrow", prefetch_batches=0):
        batch_ids = batch["id"].to_pylist()
        ids.extend(batch_ids)
        if validate_payload:
            if "tensor" in batch.column_names:
                values = batch["tensor"].combine_chunks().to_numpy(zero_copy_only=False)
                for row_id, tensor in zip(batch_ids, values):
                    rows = 128 if tensor_kind == "variable" and row_id % 2 else 64
                    assert tensor.shape == (rows, 64)
                    assert tensor.dtype == np.float32
                    assert np.all(tensor == row_id)
            else:
                assert batch["value"].to_pylist() == [
                    row_id * 2 for row_id in batch_ids
                ]
    return ids


def measure(build: Callable[[], ray.data.Dataset], args: argparse.Namespace) -> dict:
    durations = []
    for _ in range(args.repeats):
        gc.collect()
        started = time.perf_counter()
        ids = consume(build())
        durations.append(time.perf_counter() - started)
        np.testing.assert_array_equal(np.sort(ids), np.arange(args.rows))
    median = statistics.median(durations)
    return {
        "parameters": vars(args),
        "native_ray_commit": ray.__commit__,
        "pyarrow": pa.__version__,
        "numpy": np.__version__,
        "iteration_times_s": durations,
        "median_iteration_s": median,
        "stdev_iteration_s": statistics.stdev(durations) if len(durations) > 1 else 0,
        "rows_per_s": args.rows / median,
        "scope": "single-node pipeline; cached or Parquet input; excludes training",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--window", type=int, default=2048)
    parser.add_argument("--block", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--tensor-kind", choices=("scalar", "fixed", "variable"), default="fixed"
    )
    parser.add_argument("--source", choices=("cached", "parquet"), default="parquet")
    parser.add_argument(
        "--legacy-reader",
        action="store_true",
        help="Include legacy Parquet read/map fusion.",
    )
    parser.add_argument("--variant", choices=VARIANTS)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--num-cpus", type=int)
    parser.add_argument("--object-store-memory", type=int)
    args = parser.parse_args()
    if min(args.rows, args.window, args.block, args.repeats) <= 0:
        parser.error("all sizes and repeats must be positive")
    if args.concurrency is not None and args.concurrency <= 0:
        parser.error("--concurrency must be positive")

    ray.init(num_cpus=args.num_cpus, object_store_memory=args.object_store_memory)
    benchmark = Benchmark()
    try:
        ctx = DataContext.get_current()
        ctx.enable_progress_bars = False
        ctx.execution_options.preserve_order = True
        if args.legacy_reader:
            ctx.use_datasource_v2 = False
        with tempfile.TemporaryDirectory() as input_dir:
            tables = []
            for start in range(0, args.rows, args.block):
                table = make_table(
                    start, min(start + args.block, args.rows), args.tensor_kind
                )
                if args.source == "cached":
                    tables.append(table)
                else:
                    pq.write_table(table, Path(input_dir) / f"{start:012d}.parquet")
            if args.source == "cached":
                source = ray.data.from_arrow(tables).materialize()

            def read() -> ray.data.Dataset:
                if args.source == "cached":
                    return source
                return ray.data.read_parquet(input_dir)

            del tables, table
            gc.collect()
            compute = TaskPoolStrategy(size=args.concurrency)
            map_options = dict(
                batch_format="pyarrow",
                zero_copy_batch=True,
                compute=compute,
                num_cpus=1,
            )
            variants = {
                "map_batches_native_take": lambda: read().map_batches(
                    native_shuffle, batch_size=args.window, **map_options
                ),
                "strict_windows_native_take": lambda: read()
                .repartition(target_num_rows_per_block=args.window, strict=True)
                .map_batches(native_shuffle, batch_size=None, **map_options),
                "map_batches_ray_take": lambda: read().map_batches(
                    ray_shuffle, batch_size=args.window, **map_options
                ),
                "strict_windows_ray_take": lambda: read()
                .repartition(target_num_rows_per_block=args.window, strict=True)
                .map_batches(ray_shuffle, batch_size=None, **map_options),
                "window_shuffle_default": lambda: read().window_shuffle(
                    args.window, seed=42, compute=compute, num_cpus=1
                ),
                "window_shuffle_strict": lambda: read().window_shuffle(
                    args.window,
                    strict=True,
                    seed=42,
                    compute=compute,
                    num_cpus=1,
                ),
            }
            names = (
                [args.variant]
                if args.variant
                else np.random.default_rng(0).permutation(VARIANTS)
            )
            for name in names:
                build = variants[name]
                warmup = build()
                ids = consume(
                    warmup, validate_payload=True, tensor_kind=args.tensor_kind
                )
                np.testing.assert_array_equal(np.sort(ids), np.arange(args.rows))
                benchmark.run_fn(name, measure, build, args)
                benchmark.result[name]["warmup_stats"] = warmup.stats()
                benchmark.result[name]["window_contract"] = (
                    "strict consecutive windows"
                    if name.startswith("strict_windows")
                    or name == "window_shuffle_strict"
                    else "target batch size; task tails may be smaller"
                )
    finally:
        benchmark.write_result()
        ray.shutdown()


if __name__ == "__main__":
    main()
