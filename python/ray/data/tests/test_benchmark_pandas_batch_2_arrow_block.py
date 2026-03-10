"""
Benchmark for Pandas batch handling in Ray Data.

Compares performance with RAY_DATA_DEFAULT_BATCH_TO_BLOCK_ARROW_FORMAT=0 vs 1.

Usage:
    python bench_pandas_batch_to_arrow.py
"""

import time

import numpy as np
import pandas as pd
from tabulate import tabulate

import ray
from ray.data.block import BlockAccessor


def create_dataset(num_rows: int):
    """Create dataset with 10 columns: 4 int, 3 float, 3 string."""
    string_pool = [f"str_{i}" for i in range(1000)]
    df = pd.DataFrame(
        {
            "int_0": np.random.randint(0, 1000000, num_rows),
            "int_1": np.random.randint(0, 1000000, num_rows),
            "int_2": np.random.randint(0, 1000000, num_rows),
            "int_3": np.random.randint(0, 1000000, num_rows),
            "float_0": np.random.randn(num_rows),
            "float_1": np.random.randn(num_rows),
            "float_2": np.random.randn(num_rows),
            "str_0": np.random.choice(string_pool, num_rows),
            "str_1": np.random.choice(string_pool, num_rows),
            "str_2": np.random.choice(string_pool, num_rows),
        }
    )
    return ray.data.from_pandas(df).materialize()


def run_benchmark(num_rows, num_map_batches, iter_batch_size):
    """Run a single benchmark iteration."""
    ds = create_dataset(num_rows)

    # Chain multiple map_batches that don't fuse
    start = time.perf_counter()
    result_ds = ds
    for i in range(num_map_batches):
        result_ds = result_ds.map_batches(
            lambda x: x,
            batch_format="pandas",
            batch_size=iter_batch_size * (i + 1),
        )
    result_ds = result_ds.materialize()
    map_batches_time = time.perf_counter() - start

    # iter_batches (on result of map_batches)
    start = time.perf_counter()
    for _ in result_ds.iter_batches(batch_size=iter_batch_size, batch_format="pandas"):
        pass
    iter_time = time.perf_counter() - start

    return map_batches_time, iter_time


def main():
    ray.init(ignore_reinit_error=True)

    row_counts = [10_000, 1_000_000, 10_000_000]
    num_map_batches_list = [1, 2, 3, 5, 10]
    iter_batch_size = 1000
    results = []

    for num_rows in row_counts:
        for num_map_batches in num_map_batches_list:
            print(f"\n=== {num_rows:,} rows, {num_map_batches} maps_batches ===")

            # Run with arrow_format=False (default)
            BlockAccessor._DEFAULT_BATCH_TO_BLOCK_ARROW_FORMAT = False
            map_batches_time_0, iter_time_0 = run_benchmark(
                num_rows, num_map_batches, iter_batch_size
            )
            print(
                f"  fmt=0: map_batches={map_batches_time_0:.3f}s iter={iter_time_0:.3f}s"
            )

            # Run with arrow_format=True
            BlockAccessor._DEFAULT_BATCH_TO_BLOCK_ARROW_FORMAT = True
            map_batches_time_1, iter_time_1 = run_benchmark(
                num_rows, num_map_batches, iter_batch_size
            )
            print(
                f"  fmt=1: map_batches={map_batches_time_1:.3f}s iter={iter_time_1:.3f}s"
            )

            # Calculate speedup (positive = arrow_format=1 is faster)
            map_speedup = (
                (map_batches_time_0 / map_batches_time_1 - 1) * 100
                if map_batches_time_1 > 0
                else 0
            )
            iter_speedup = (
                (iter_time_0 / iter_time_1 - 1) * 100 if iter_time_1 > 0 else 0
            )

            results.append(
                {
                    "num_rows": f"{num_rows:,}",
                    "num_map_batches": num_map_batches,
                    "map_batches(pandas block) (s)": f"{map_batches_time_0:.3f}",
                    "map_batches(arrow block) (s)": f"{map_batches_time_1:.3f}",
                    "map_batches Δ%": f"{map_speedup:+.1f}%",
                    "iter_batches(pandas block) (s)": f"{iter_time_0:.3f}",
                    "iter_batches(arrow block) (s)": f"{iter_time_1:.3f}",
                    "iter_batches Δ%": f"{iter_speedup:+.1f}%",
                }
            )

    # Reset to default
    BlockAccessor._DEFAULT_BATCH_TO_BLOCK_ARROW_FORMAT = False

    print("\n" + tabulate(results, headers="keys", tablefmt="grid"))
    print("\nNote: Δ% = speedup when arrow_format=1 (positive = faster)")
    ray.shutdown()


if __name__ == "__main__":
    main()
