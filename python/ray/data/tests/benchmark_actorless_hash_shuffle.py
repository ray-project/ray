"""Performance benchmark: Actorless Hash Shuffle vs Original (Actor-based) Hash Shuffle.

Runs a simple repartition on TPC-H lineitem table and compares wall-clock time.

Usage:
    # Local with generated data (no S3 needed):
    python benchmark_actorless_hash_shuffle.py

    # With TPC-H from S3:
    python benchmark_actorless_hash_shuffle.py --sf 1

    # Custom num partitions:
    python benchmark_actorless_hash_shuffle.py --sf 1 --num-partitions 50
"""

import argparse
import time

import ray
from ray.data.context import DataContext, ShuffleStrategy


def generate_local_dataset(num_rows: int = 1_000_000, num_blocks: int = 100):
    """Generate a synthetic dataset for benchmarking without S3 access."""
    import numpy as np
    import pyarrow as pa

    rows_per_block = num_rows // num_blocks
    blocks = []
    for i in range(num_blocks):
        t = pa.table(
            {
                "l_orderkey": np.random.randint(0, 1_000_000, rows_per_block),
                "l_partkey": np.random.randint(0, 200_000, rows_per_block),
                "l_suppkey": np.random.randint(0, 10_000, rows_per_block),
                "l_linenumber": np.random.randint(1, 8, rows_per_block),
                "l_quantity": np.random.uniform(1, 50, rows_per_block),
                "l_extendedprice": np.random.uniform(900, 105000, rows_per_block),
                "l_discount": np.random.uniform(0, 0.1, rows_per_block),
                "l_tax": np.random.uniform(0, 0.08, rows_per_block),
            }
        )
        blocks.append(t)

    return ray.data.from_arrow(blocks)


def load_tpch_lineitem(sf: int):
    """Load TPC-H lineitem table from S3."""
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    ds = ray.data.read_parquet(path)

    column_mapping = {
        "column00": "l_orderkey",
        "column01": "l_partkey",
        "column02": "l_suppkey",
        "column03": "l_linenumber",
        "column04": "l_quantity",
        "column05": "l_extendedprice",
        "column06": "l_discount",
        "column07": "l_tax",
        "column08": "l_returnflag",
        "column09": "l_linestatus",
        "column10": "l_shipdate",
        "column11": "l_commitdate",
        "column12": "l_receiptdate",
        "column13": "l_shipinstruct",
        "column14": "l_shipmode",
        "column15": "l_comment",
    }
    ds = ds.rename_columns(column_mapping)
    return ds


def run_repartition_benchmark(ds, strategy, num_partitions, key_columns, label):
    """Run a single repartition benchmark and return elapsed time."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = strategy

    # Warm up / force materialization of input.
    ds = ds.materialize()

    start = time.perf_counter()
    result = ds.repartition(num_partitions, keys=key_columns).materialize()
    elapsed = time.perf_counter() - start

    num_rows = result.count()
    num_blocks = result.num_blocks()
    print(
        f"  [{label}] {elapsed:.2f}s | "
        f"{num_rows:,} rows, {num_blocks} blocks, "
        f"{num_partitions} partitions"
    )
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Shuffle Benchmark")
    parser.add_argument(
        "--sf",
        type=int,
        default=0,
        help="TPC-H scale factor (0 = use local generated data)",
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=1_000_000,
        help="Number of rows for local generated data (ignored if --sf > 0)",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=20,
        help="Number of output partitions for repartition",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=3,
        help="Number of runs per strategy",
    )
    parser.add_argument(
        "--key-columns",
        nargs="+",
        default=["l_orderkey"],
        help="Key columns for hash partitioning",
    )
    args = parser.parse_args()

    ray.init()

    # Load data.
    if args.sf > 0:
        print(f"Loading TPC-H lineitem (sf={args.sf}) from S3...")
        ds = load_tpch_lineitem(args.sf)
    else:
        print(f"Generating local dataset ({args.num_rows:,} rows)...")
        ds = generate_local_dataset(num_rows=args.num_rows)

    ds = ds.materialize()
    print(f"Dataset: {ds.count():,} rows, {ds.num_blocks()} blocks\n")

    strategies = [
        (ShuffleStrategy.HASH_SHUFFLE, "actor-based"),
        (ShuffleStrategy.ACTORLESS_HASH_SHUFFLE, "actorless"),
    ]

    results = {label: [] for _, label in strategies}

    for run in range(1, args.num_runs + 1):
        print(f"--- Run {run}/{args.num_runs} ---")
        for strategy, label in strategies:
            elapsed = run_repartition_benchmark(
                ds, strategy, args.num_partitions, args.key_columns, label
            )
            results[label].append(elapsed)
        print()

    # Summary.
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for _, label in strategies:
        times = results[label]
        avg = sum(times) / len(times)
        best = min(times)
        print(f"  {label:15s}: avg={avg:.2f}s, best={best:.2f}s, runs={times}")

    actor_avg = sum(results["actor-based"]) / len(results["actor-based"])
    actorless_avg = sum(results["actorless"]) / len(results["actorless"])
    if actor_avg > 0:
        speedup = actor_avg / actorless_avg
        print(f"\n  Actorless vs Actor-based: {speedup:.2f}x")


if __name__ == "__main__":
    main()
