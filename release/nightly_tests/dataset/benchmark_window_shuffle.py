"""Compare window shuffle and UDF baselines, including execution costs.

Run in a virtual environment containing a compatible build of this checkout:
    python release/nightly_tests/dataset/benchmark_window_shuffle.py
"""

import argparse
import gc
import json
import statistics
import time

import numpy as np
import pyarrow as pa
import ray
from ray.data import TaskPoolStrategy
from ray.data._internal.arrow_ops.transform_pyarrow import take_table
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray
from ray.data.context import DataContext


def native_shuffle(batch):
    rng = np.random.default_rng((TaskContext.get_current().task_idx, 42))
    return batch.take(rng.permutation(len(batch)))


def ray_shuffle(batch):
    rng = np.random.default_rng((TaskContext.get_current().task_idx, 42))
    return take_table(batch, rng.permutation(len(batch)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--window", type=int, default=2048)
    parser.add_argument("--block", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--tensor-kind", choices=("fixed", "variable"), default="fixed")
    args = parser.parse_args()
    if min(args.rows, args.window, args.block, args.repeats) <= 0:
        parser.error("all sizes and repeats must be positive")

    ray.init(num_cpus=4, include_dashboard=False, object_store_memory=1024 * 2**20)
    try:
        ctx = DataContext.get_current()
        ctx.enable_progress_bars = False
        ctx.execution_options.preserve_order = True
        tables = []
        for start in range(0, args.rows, args.block):
            ids = np.arange(start, min(start + args.block, args.rows))
            if args.tensor_kind == "variable":
                data = np.empty(len(ids), dtype=object)
                for j, row_id in enumerate(ids):
                    data[j] = np.full(
                        (64 if row_id % 2 == 0 else 128, 64), row_id, dtype=np.float32
                    )
            else:
                data = np.broadcast_to(
                    ids.astype(np.float32)[:, None, None], (len(ids), 64, 64)
                ).copy()
            tables.append(
                pa.Table.from_arrays(
                    [pa.array(ids), ArrowTensorArray.from_numpy(data)],
                    names=["id", "tensor"],
                )
            )
        source = ray.data.from_arrow(tables).materialize()
        del tables, data
        gc.collect()
        map_options = dict(
            batch_format="pyarrow",
            zero_copy_batch=True,
            compute=TaskPoolStrategy(size=2),
            num_cpus=1,
        )
        variants = {
            "map_batches_native_take": lambda: source.map_batches(
                native_shuffle, batch_size=args.window, **map_options
            ),
            "strict_windows_native_take": lambda: source.repartition(
                target_num_rows_per_block=args.window, strict=True
            ).map_batches(native_shuffle, batch_size=None, **map_options),
            "map_batches_ray_take": lambda: source.map_batches(
                ray_shuffle, batch_size=args.window, **map_options
            ),
            "strict_windows_ray_take": lambda: source.repartition(
                target_num_rows_per_block=args.window, strict=True
            ).map_batches(ray_shuffle, batch_size=None, **map_options),
            "window_shuffle_default": lambda: source.window_shuffle(
                args.window, seed=42, compute=TaskPoolStrategy(size=2), num_cpus=1
            ),
            "window_shuffle_strict": lambda: source.window_shuffle(
                args.window,
                strict=True,
                seed=42,
                compute=TaskPoolStrategy(size=2),
                num_cpus=1,
            ),
        }
        results = {name: [] for name in variants}
        trial_rng = np.random.default_rng(0)
        # Rotate execution order to reduce bias from cache/worker warm-up.
        for iteration in range(args.repeats + 1):
            for name in trial_rng.permutation(list(variants)):
                build = variants[name]
                started = time.perf_counter()
                output = build().materialize()
                elapsed = time.perf_counter() - started
                assert output.count() == args.rows
                if iteration > 0:
                    results[name].append(elapsed)
                del output
                gc.collect()
        print(
            json.dumps(
                {
                    "ray": ray.__version__,
                    "ray_commit": ray.__commit__,
                    "pyarrow": pa.__version__,
                    "numpy": np.__version__,
                    "parameters": vars(args),
                    "seconds": results,
                    "median_seconds": {
                        k: statistics.median(v) for k, v in results.items()
                    },
                    "scope": "local CPU pipeline, materialized input; excludes training",
                    "window_contract": {
                        name: (
                            "strict consecutive windows"
                            if name.startswith("strict_windows")
                            or name == "window_shuffle_strict"
                            else "target batch size; task tails may be smaller"
                        )
                        for name in variants
                    },
                },
                indent=2,
            )
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
