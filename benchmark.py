"""
Benchmark for Ray object spilling throughput.

Measures spill and restore performance to compare the C++ threadpool
spilling implementation against the legacy Python IO worker implementation.

Usage:
    # Run with defaults (good for quick comparison):
    python benchmark.py

    # Custom parameters:
    python benchmark.py --object-size-mb 50 --num-objects 40 --store-memory-mb 200

    # Run with more IO workers:
    python benchmark.py --max-io-workers 8

To compare old vs new implementation, run this script on each branch
and compare the output.
"""

import argparse
import json
import os
import shutil
import tempfile
import time

import numpy as np

import ray
from ray._private.internal_api import memory_summary


def parse_spill_stats(address: str) -> dict:
    """Extract spill/restore stats from Ray's memory summary."""
    summary = memory_summary(address=address, stats_only=True)
    stats = {
        "spilled_mib": 0.0,
        "spilled_objects": 0,
        "restored_mib": 0.0,
        "restored_objects": 0,
    }
    for line in summary.split("\n"):
        if "Spilled" in line and "MiB" in line:
            parts = line.split()
            for i, part in enumerate(parts):
                if part == "MiB," and i > 0:
                    try:
                        stats["spilled_mib"] = float(parts[i - 1])
                    except ValueError:
                        pass
                if part == "objects," and i > 0:
                    try:
                        stats["spilled_objects"] = int(parts[i - 1])
                    except ValueError:
                        pass
        if "Restored" in line and "MiB" in line:
            parts = line.split()
            for i, part in enumerate(parts):
                if part == "MiB," and i > 0:
                    try:
                        stats["restored_mib"] = float(parts[i - 1])
                    except ValueError:
                        pass
                if part == "objects," and i > 0:
                    try:
                        stats["restored_objects"] = int(parts[i - 1])
                    except ValueError:
                        pass
    return stats


def run_spill_benchmark(
    object_size_mb: float,
    num_objects: int,
    store_memory_mb: int,
    max_io_workers: int,
    spill_dir: str,
) -> dict:
    """Run a single spill/restore benchmark and return timing results."""

    object_size_bytes = int(object_size_mb * 1024 * 1024)
    num_floats = object_size_bytes // 8  # np.float64 is 8 bytes
    total_data_mb = object_size_mb * num_objects

    spill_config = json.dumps(
        {"type": "filesystem", "params": {"directory_path": spill_dir}}
    )

    address = ray.init(
        num_cpus=1,
        object_store_memory=store_memory_mb * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "max_io_workers": max_io_workers,
            "object_store_full_delay_ms": 50,
            "object_spilling_config": spill_config,
            "min_spilling_size": 0,
        },
    )

    ray_address = address["address"]

    # Phase 1: Spill - create objects that exceed object store memory.
    refs = []
    t_spill_start = time.monotonic()
    for i in range(num_objects):
        arr = np.random.rand(num_floats)
        ref = ray.put(arr)
        refs.append(ref)
    t_spill_end = time.monotonic()

    # Wait for spilling to complete by checking stats.
    # Objects are spilled asynchronously, so poll until all are spilled.
    deadline = time.monotonic() + 120  # 2 minute timeout
    while time.monotonic() < deadline:
        stats = parse_spill_stats(ray_address)
        if stats["spilled_objects"] >= num_objects - (
            store_memory_mb // int(object_size_mb + 1)
        ):
            break
        time.sleep(0.1)
    t_spill_settled = time.monotonic()

    spill_stats = parse_spill_stats(ray_address)

    # Phase 2: Restore - read back all spilled objects.
    # Delete local references first to ensure objects are read from spill.
    t_restore_start = time.monotonic()
    for ref in refs:
        result = ray.get(ref)
        assert (
            result.shape[0] == num_floats
        ), f"Data corruption: expected {num_floats} floats, got {result.shape[0]}"
    t_restore_end = time.monotonic()

    restore_stats = parse_spill_stats(ray_address)

    # Phase 3: Delete - release all references and wait for cleanup.
    t_delete_start = time.monotonic()
    del refs
    del result
    # Give time for async deletion.
    time.sleep(2)
    t_delete_end = time.monotonic()

    ray.shutdown()

    # Calculate results.
    spill_wall_time = t_spill_settled - t_spill_start
    restore_wall_time = t_restore_end - t_restore_start
    spill_throughput = (
        spill_stats["spilled_mib"] / spill_wall_time if spill_wall_time > 0 else 0
    )
    restore_throughput = (
        restore_stats["restored_mib"] / restore_wall_time
        if restore_wall_time > 0
        else 0
    )

    return {
        "object_size_mb": object_size_mb,
        "num_objects": num_objects,
        "total_data_mb": total_data_mb,
        "store_memory_mb": store_memory_mb,
        "max_io_workers": max_io_workers,
        "spill_wall_time_s": spill_wall_time,
        "spill_throughput_mib_s": spill_throughput,
        "spilled_mib": spill_stats["spilled_mib"],
        "spilled_objects": spill_stats["spilled_objects"],
        "restore_wall_time_s": restore_wall_time,
        "restore_throughput_mib_s": restore_throughput,
        "restored_mib": restore_stats["restored_mib"],
        "restored_objects": restore_stats["restored_objects"],
        "put_time_s": t_spill_end - t_spill_start,
    }


def print_results(results: list[dict]):
    """Print benchmark results in a readable table format."""

    print("\n" + "=" * 80)
    print("OBJECT SPILLING BENCHMARK RESULTS")
    print("=" * 80)

    for r in results:
        print(
            f"\n--- Object size: {r['object_size_mb']:.1f} MiB, "
            f"Count: {r['num_objects']}, "
            f"Workers: {r['max_io_workers']} ---"
        )
        print(f"  Object store memory:  {r['store_memory_mb']} MiB")
        print(f"  Total data:           {r['total_data_mb']:.1f} MiB")
        print()
        print("  Spill:")
        print(f"    Objects spilled:    {r['spilled_objects']}")
        print(f"    Data spilled:       {r['spilled_mib']:.1f} MiB")
        print(f"    Wall time:          {r['spill_wall_time_s']:.3f} s")
        print(f"    Throughput:         {r['spill_throughput_mib_s']:.1f} MiB/s")
        print(f"    ray.put() time:     {r['put_time_s']:.3f} s")
        print()
        print("  Restore:")
        print(f"    Objects restored:   {r['restored_objects']}")
        print(f"    Data restored:      {r['restored_mib']:.1f} MiB")
        print(f"    Wall time:          {r['restore_wall_time_s']:.3f} s")
        print(f"    Throughput:         {r['restore_throughput_mib_s']:.1f} MiB/s")

    print("\n" + "=" * 80)
    print("SUMMARY (for copy-paste comparison)")
    print("=" * 80)
    print(
        f"{'ObjSize':>8} {'Count':>6} {'Workers':>8} "
        f"{'SpillMiB/s':>11} {'RestoreMiB/s':>13} "
        f"{'SpillTime':>10} {'RestoreTime':>12}"
    )
    print("-" * 80)
    for r in results:
        print(
            f"{r['object_size_mb']:>7.1f}M {r['num_objects']:>6} {r['max_io_workers']:>8} "
            f"{r['spill_throughput_mib_s']:>11.1f} {r['restore_throughput_mib_s']:>13.1f} "
            f"{r['spill_wall_time_s']:>9.3f}s {r['restore_wall_time_s']:>11.3f}s"
        )
    print()


def main():
    parser = argparse.ArgumentParser(description="Ray object spilling benchmark")
    parser.add_argument(
        "--object-size-mb",
        type=float,
        nargs="+",
        default=[10, 50],
        help="Object sizes in MiB to test (default: 10 50)",
    )
    parser.add_argument(
        "--num-objects",
        type=int,
        default=20,
        help="Number of objects to spill per test (default: 20)",
    )
    parser.add_argument(
        "--store-memory-mb",
        type=int,
        default=200,
        help="Object store memory in MiB (default: 200)",
    )
    parser.add_argument(
        "--max-io-workers",
        type=int,
        nargs="+",
        default=[4],
        help="Number of IO workers to test (default: 4)",
    )
    parser.add_argument(
        "--spill-dir",
        type=str,
        default=None,
        help="Directory for spilled objects (default: temp dir)",
    )
    args = parser.parse_args()

    spill_dir = args.spill_dir or tempfile.mkdtemp(prefix="ray_spill_bench_")
    os.makedirs(spill_dir, exist_ok=True)

    print(f"Spill directory: {spill_dir}")
    print(f"Object sizes: {args.object_size_mb} MiB")
    print(f"Num objects: {args.num_objects}")
    print(f"Store memory: {args.store_memory_mb} MiB")
    print(f"IO workers: {args.max_io_workers}")

    all_results = []
    for workers in args.max_io_workers:
        for size_mb in args.object_size_mb:
            # Clean spill dir between runs.
            if os.path.exists(spill_dir):
                shutil.rmtree(spill_dir)
            os.makedirs(spill_dir, exist_ok=True)

            print(
                f"\nRunning: {size_mb} MiB x {args.num_objects} objects, "
                f"{workers} workers..."
            )
            result = run_spill_benchmark(
                object_size_mb=size_mb,
                num_objects=args.num_objects,
                store_memory_mb=args.store_memory_mb,
                max_io_workers=workers,
                spill_dir=spill_dir,
            )
            all_results.append(result)

    print_results(all_results)

    # Cleanup.
    if args.spill_dir is None and os.path.exists(spill_dir):
        shutil.rmtree(spill_dir)


if __name__ == "__main__":
    main()
