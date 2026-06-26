#!/usr/bin/env python3
"""Debug script for test_parquet_read_spread liveness hang."""

import argparse
import faulthandler
import os
import sys
import tempfile
import threading
import time
import traceback
from typing import List, Optional

import pandas as pd

import ray
from ray.cluster_utils import Cluster


def print_ray_build_info() -> None:
    import ray._raylet as raylet

    print("=== Ray build ===")
    print(f"ray.__file__       = {ray.__file__}")
    print(f"ray._raylet.__file__ = {raylet.__file__}")
    print(f"_raylet size        = {os.path.getsize(raylet.__file__):,} bytes")
    print()


def setup_cluster(single_node: bool) -> Cluster:
    ray.shutdown()
    cluster = Cluster()

    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        object_store_memory=2 * 1024 * 1024 * 1024,
        _system_config={"max_direct_call_object_size": 0},
    )
    if not single_node:
        cluster.add_node(
            resources={"bar:2": 100},
            num_cpus=10,
            object_store_memory=2 * 1024 * 1024 * 1024,
        )
        cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

    ray.init(cluster.address)
    return cluster


def write_parquet_files(data_dir: str) -> None:
    df1 = pd.DataFrame({"one": list(range(100)), "two": list(range(100, 200))})
    df1.to_parquet(os.path.join(data_dir, "test1.parquet"))
    df2 = pd.DataFrame({"one": list(range(300, 400)), "two": list(range(400, 500))})
    df2.to_parquet(os.path.join(data_dir, "test2.parquet"))


def get_node_ids(single_node: bool) -> tuple[str, Optional[str]]:
    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    node1_id = ray.get(get_node_id.options(resources={"bar:1": 1}).remote())
    if single_node:
        return node1_id, None

    node2_id = ray.get(get_node_id.options(resources={"bar:2": 1}).remote())
    return node1_id, node2_id


def print_task_state() -> None:
    print("=== Ray task state (best effort) ===")
    try:
        from ray.util.state import list_tasks

        tasks = list_tasks(filters=[("type", "=", "NORMAL_TASK")], limit=20)
        for t in tasks[-10:]:
            print(
                f"  task_id={t.get('task_id', '?')[:16]} "
                f"state={t.get('state')} name={t.get('name')}"
            )
    except Exception as e:
        print(f"  (could not list tasks: {e})")
    print()


def print_session_logs_hint() -> None:
    session_dir = ray._private.worker.global_worker.node._session_dir
    print("=== Log files ===")
    print(f"session_dir = {session_dir}")
    print(f"ray-data.log = {os.path.join(session_dir, 'logs', 'ray-data.log')}")
    print("grep hints:")
    print(
        "  grep -E 'orphan|output stream|IdleDetector|backpressure' "
        f"{os.path.join(session_dir, 'logs', 'ray-data.log')}"
    )
    print()


def consume_dataset(
    data_path: str,
    block_size: int,
    hang_timeout_s: float,
    result: dict,
) -> None:
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.target_max_block_size = block_size
        ctx.execution_options.verbose_progress = True

        ds = ray.data.read_parquet(data_path)
        bundles = ds.iter_internal_ref_bundles()

        bundle_count = 0
        block_refs: List[ray.ObjectRef] = []

        t0 = time.time()
        for bundle in bundles:
            bundle_count += 1
            block_refs.extend(bundle.block_refs)
            if bundle_count % 50 == 0:
                elapsed = time.time() - t0
                print(
                    f"[consumer] bundles={bundle_count} blocks={len(block_refs)} "
                    f"elapsed={elapsed:.1f}s",
                    flush=True,
                )

        result["bundle_count"] = bundle_count
        result["block_refs"] = block_refs
        result["error"] = None
        print(
            f"[consumer] DONE bundles={bundle_count} blocks={len(block_refs)}",
            flush=True,
        )
    except Exception as e:
        result["error"] = e
        result["traceback"] = traceback.format_exc()
        print(f"[consumer] ERROR: {e}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--single-node", action="store_true")
    parser.add_argument(
        "--block-size",
        type=int,
        default=1,
        help="target_max_block_size in bytes (default: 1, like the test)",
    )
    parser.add_argument(
        "--block-size-mib",
        type=int,
        default=None,
        help="target_max_block_size in MiB (overrides --block-size)",
    )
    parser.add_argument(
        "--hang-timeout-s",
        type=float,
        default=120.0,
        help="Seconds before declaring a hang and dumping diagnostics",
    )
    args = parser.parse_args()

    block_size = args.block_size
    if args.block_size_mib is not None:
        block_size = args.block_size_mib * 1024 * 1024

    print_ray_build_info()

    cluster = None
    tmpdir = tempfile.mkdtemp(prefix="parquet_spread_debug_")
    try:
        print("=== Setup ===")
        print(f"tmpdir      = {tmpdir}")
        print(f"single_node = {args.single_node}")
        print(f"block_size  = {block_size}")
        print(f"timeout     = {args.hang_timeout_s}s")
        print()

        cluster = setup_cluster(args.single_node)
        write_parquet_files(tmpdir)

        node1_id, node2_id = get_node_ids(args.single_node)
        print(f"node1_id = {node1_id}")
        if args.single_node:
            print("node2_id = (skipped, single-node mode)")
        else:
            print(f"node2_id = {node2_id}")
        print()

        result: dict = {}
        consumer = threading.Thread(
            target=consume_dataset,
            args=(tmpdir, block_size, args.hang_timeout_s, result),
            daemon=True,
        )

        print("=== Starting consumer thread ===")
        t0 = time.time()
        consumer.start()

        while consumer.is_alive():
            elapsed = time.time() - t0
            if elapsed > args.hang_timeout_s:
                print()
                print(f"!!! HANG detected after {elapsed:.1f}s !!!")
                print()
                faulthandler.dump_traceback(file=sys.stdout, all_threads=True)
                print()
                print_task_state()
                print_session_logs_hint()
                return 1
            time.sleep(1.0)

        consumer.join()
        elapsed = time.time() - t0

        if result.get("error") is not None:
            print(result.get("traceback", ""))
            print(f"FAILED with error after {elapsed:.1f}s")
            return 1

        block_refs = result["block_refs"]
        print(f"=== Verifying object locations ({len(block_refs)} blocks) ===")
        ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)
        location_data = ray.experimental.get_object_locations(block_refs)
        locations = []
        for block in block_refs:
            locations.extend(location_data[block]["node_ids"])

        unique_locations = set(locations)
        print(f"unique node_ids = {unique_locations}")

        if args.single_node:
            if unique_locations == {node1_id}:
                print(f"PASS (single-node) in {elapsed:.1f}s")
                return 0
            print(
                f"FAIL (single-node): expected all blocks on {node1_id}, "
                f"got {unique_locations}"
            )
            return 1

        expected = {node1_id, node2_id}
        if unique_locations == expected:
            print(f"PASS in {elapsed:.1f}s")
            return 0

        print(f"FAIL: expected locations {expected}, got {unique_locations}")
        return 1

    finally:
        ray.shutdown()
        if cluster is not None:
            cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())
