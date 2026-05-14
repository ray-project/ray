#!/usr/bin/env python3
"""Minimal repro for test_parquet_read_spread CI slowness/timeouts.

Shows that the same read completes in <1s on one node but stalls ~90s on
a multi-node cluster with max_direct_call_object_size=0 (all objects via
plasma) and SPREAD scheduling — even after all blocks are consumed.

This matches test_parquet_read_spread (python/ray/data/tests/datasource/
test_parquet.py) without the location assertions.

Usage:
  PYTHONPATH=python python demo_spread_streaming_hang.py

Optional:
  PYTHONPATH=python python demo_spread_streaming_hang.py --multi-node-only
  PYTHONPATH=python python demo_spread_streaming_hang.py --timeout-s 180
"""

import argparse
import os
import sys
import tempfile
import threading
import time

import pandas as pd

import ray
from ray.cluster_utils import Cluster


def _raylet_size() -> int:
    import ray._raylet as raylet

    return os.path.getsize(raylet.__file__)


def _make_cluster(*, multi_node: bool) -> Cluster:
    ray.shutdown()
    cluster = Cluster()
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        object_store_memory=2 * 1024 * 1024 * 1024,
        _system_config={"max_direct_call_object_size": 0},
    )
    if multi_node:
        cluster.add_node(
            resources={"bar:2": 100},
            num_cpus=10,
            object_store_memory=2 * 1024 * 1024 * 1024,
        )
        cluster.add_node(resources={"bar:3": 100}, num_cpus=0)
    ray.init(cluster.address)
    return cluster


def _write_parquet(data_dir: str) -> None:
    # Same shape as test_parquet_read_spread: 200 rows across 2 files.
    pd.DataFrame({"one": range(100), "two": range(100, 200)}).to_parquet(
        os.path.join(data_dir, "a.parquet")
    )
    pd.DataFrame({"one": range(300, 400), "two": range(400, 500)}).to_parquet(
        os.path.join(data_dir, "b.parquet")
    )


def _run_read(data_dir: str) -> tuple[float, int]:
    """Return (elapsed_seconds, num_blocks)."""
    ctx = ray.data.DataContext.get_current()
    ctx.target_max_block_size = 1  # 200 single-row blocks, 1 read task.

    ds = ray.data.read_parquet(data_dir)
    t0 = time.perf_counter()
    block_count = sum(len(b.block_refs) for b in ds.iter_internal_ref_bundles())
    return time.perf_counter() - t0, block_count


def _run_read_with_timeout(data_dir: str, timeout_s: float) -> tuple[float, int]:
    """Run _run_read, raising TimeoutError if it exceeds timeout_s."""
    result: dict = {}
    error: dict = {}

    def _target() -> None:
        try:
            result["value"] = _run_read(data_dir)
        except BaseException as e:
            error["exc"] = e

    thread = threading.Thread(target=_target, daemon=True)
    thread.start()
    thread.join(timeout=timeout_s)
    if thread.is_alive():
        raise TimeoutError(
            f"read did not finish within {timeout_s:.1f}s "
            "(matches pytest timeout behavior on multi-node)"
        )
    if "exc" in error:
        raise error["exc"]
    return result["value"]


def _run_case(
    label: str, *, multi_node: bool, data_dir: str, timeout_s: float
) -> tuple[float | None, bool]:
    """Return (elapsed_seconds, timed_out). elapsed is None on timeout."""
    cluster = _make_cluster(multi_node=multi_node)
    nodes = "3 nodes (SPREAD)" if multi_node else "1 node"
    try:
        elapsed, blocks = _run_read_with_timeout(data_dir, timeout_s)
        print(f"  [{label}] {nodes}: {blocks} blocks in {elapsed:.2f}s")
        return elapsed, False
    except TimeoutError as e:
        print(f"  [{label}] {nodes}: TIMEOUT after {timeout_s:.1f}s ({e})")
        return None, True
    finally:
        ray.shutdown()
        cluster.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--multi-node-only",
        action="store_true",
        help="Skip the fast single-node baseline.",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=180.0,
        help=(
            "Max seconds to wait for each case before declaring TIMEOUT "
            "(default: 180, same as pytest default)."
        ),
    )
    parser.add_argument(
        "--slow-threshold-s",
        type=float,
        default=10.0,
        help="Elapsed time above this on multi-node is treated as repro (default: 10).",
    )
    args = parser.parse_args()

    print("=== test_parquet_read_spread hang demo ===")
    print(f"_raylet size: {_raylet_size():,} bytes")
    print(f"timeout: {args.timeout_s:.1f}s per case")
    print(
        "Config: target_max_block_size=1, max_direct_call_object_size=0, "
        "read_parquet -> iter_internal_ref_bundles()"
    )
    print()

    with tempfile.TemporaryDirectory(prefix="spread_hang_demo_") as data_dir:
        _write_parquet(data_dir)

        single_s = None
        single_timed_out = False
        if not args.multi_node_only:
            print("1) Single-node baseline (should be fast):")
            single_s, single_timed_out = _run_case(
                "single", multi_node=False, data_dir=data_dir, timeout_s=args.timeout_s
            )
            print()

        print("2) Multi-node (matches CI test cluster layout):")
        multi_s, multi_timed_out = _run_case(
            "multi", multi_node=True, data_dir=data_dir, timeout_s=args.timeout_s
        )
        print()

    print("=== Interpretation ===")
    if single_s is not None:
        print(f"  single-node: {single_s:.2f}s")
    elif single_timed_out:
        print(f"  single-node: TIMEOUT (>{args.timeout_s:.1f}s)")
    if multi_s is not None:
        print(f"  multi-node:  {multi_s:.2f}s")
    elif multi_timed_out:
        print(f"  multi-node:  TIMEOUT (>{args.timeout_s:.1f}s)")
    print()
    print(
        "If multi-node is ~90s while single-node is <1s, the stall is in "
        "streaming-generator completion when the task's completion ref (or "
        "EOF marker) is on a remote node — not in parquet or block count."
    )
    print(
        "pytest uses a similar timeout (default 180s): multi-node often finishes "
        "in ~90-100s but can exceed the limit under load or with a stale _raylet."
    )
    print()
    print("Mitigations for developers:")
    print("  - Rebuild Ray so driver/worker share a current _raylet.so (~44 MB)")
    print("  - PYTHONPATH=python when running tests from a source checkout")

    if multi_timed_out or single_timed_out:
        print()
        print(f"RESULT: TIMEOUT (case exceeded --timeout-s {args.timeout_s:.1f})")
        return 1

    if multi_s is not None and multi_s > args.slow_threshold_s:
        print()
        print(f"RESULT: REPRODUCED (multi-node > {args.slow_threshold_s}s)")
        return 1

    print()
    print("RESULT: multi-node completed quickly (build may include the fix)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
