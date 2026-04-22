"""
Benchmark: Ray object store vs. Arrow Flight store for pyarrow.Table transfers.

Tests same-node and cross-node transfers at varying table sizes.
Requires RAY_USE_FLIGHT_STORE=1 for the Flight store path.

Usage:
    # Ray object store baseline (no env var needed):
    python proto/flight_store/bench_flight_store.py --mode ray

    # Flight store (cross-node via Flight RPC):
    RAY_USE_FLIGHT_STORE=1 python proto/flight_store/bench_flight_store.py --mode flight

    # Flight store (same-node via process_vm_writev):
    RAY_USE_FLIGHT_STORE=1 python proto/flight_store/bench_flight_store.py --mode flight-vm

    # All modes:
    RAY_USE_FLIGHT_STORE=1 python proto/flight_store/bench_flight_store.py --mode all
"""

import argparse
import os
import time

import numpy as np
import pyarrow as pa

import ray

# ---------------------------------------------------------------------------
# Table generation
# ---------------------------------------------------------------------------

def make_table(size_mb):
    """Create an Arrow table of approximately size_mb megabytes."""
    n_rows = max(1, size_mb * 1024 * 1024 // 8)
    return pa.table({"data": np.random.randn(n_rows)})


# ---------------------------------------------------------------------------
# Ray object store path (baseline)
# ---------------------------------------------------------------------------

@ray.remote
def ray_produce(size_mb):
    return make_table(size_mb)


@ray.remote
def ray_consume(table):
    assert isinstance(table, pa.Table)
    return table.num_rows


@ray.remote(resources={"node2": 0.01})
def ray_produce_node2(size_mb):
    return make_table(size_mb)


@ray.remote(resources={"node2": 0.01})
def ray_consume_node2(table):
    assert isinstance(table, pa.Table)
    return table.num_rows


# ---------------------------------------------------------------------------
# Flight store path (uses RAY_USE_FLIGHT_STORE=1 integration in core)
# When the env var is set, returning a pa.Table from a task automatically
# stores it in the Flight store. ray.get() fetches via Flight/VM.
# The same @ray.remote functions work — the interception is transparent.
# ---------------------------------------------------------------------------

# We reuse ray_produce / ray_consume — the Flight store intercept is
# automatic when RAY_USE_FLIGHT_STORE=1.


# ---------------------------------------------------------------------------
# Benchmark harness
# ---------------------------------------------------------------------------

def bench_throughput(produce_fn, consume_fn, size_mb, n_iters, label):
    """Measure round-trip throughput: produce → transfer → consume."""
    # Warmup.
    ref = produce_fn.remote(size_mb)
    ray.get(consume_fn.remote(ref))

    refs = []
    t0 = time.perf_counter()
    for _ in range(n_iters):
        ref = produce_fn.remote(size_mb)
        refs.append(consume_fn.remote(ref))
    results = ray.get(refs)
    elapsed = time.perf_counter() - t0

    total_mb = size_mb * n_iters
    throughput = total_mb / elapsed
    per_iter_ms = elapsed / n_iters * 1000
    print(
        f"  {label:30s}  {size_mb:4d}MB x {n_iters:3d}  "
        f"{per_iter_ms:7.1f}ms/iter  {throughput:7.1f} MB/s"
    )
    return throughput


def bench_latency(produce_fn, consume_fn, size_mb, n_iters, label):
    """Measure per-object latency: produce, wait, consume, wait."""
    # Warmup.
    ref = produce_fn.remote(size_mb)
    ray.get(consume_fn.remote(ref))

    latencies = []
    for _ in range(n_iters):
        t0 = time.perf_counter()
        ref = produce_fn.remote(size_mb)
        result = ray.get(consume_fn.remote(ref))
        latencies.append(time.perf_counter() - t0)

    avg_ms = sum(latencies) / len(latencies) * 1000
    p50_ms = sorted(latencies)[len(latencies) // 2] * 1000
    p99_ms = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
    print(
        f"  {label:30s}  {size_mb:4d}MB x {n_iters:3d}  "
        f"avg={avg_ms:7.1f}ms  p50={p50_ms:7.1f}ms  p99={p99_ms:7.1f}ms"
    )


def run_ray_baseline(sizes_mb, n_iters):
    """Benchmark Ray object store (same-node only with local cluster)."""
    print("\n--- Ray Object Store (baseline) ---")
    print("  Throughput:")
    for size_mb in sizes_mb:
        bench_throughput(ray_produce, ray_consume, size_mb, n_iters, "ray-object-store")
    print("  Latency:")
    for size_mb in sizes_mb:
        bench_latency(ray_produce, ray_consume, size_mb, min(n_iters, 20), "ray-object-store")


def run_flight(sizes_mb, n_iters):
    """Benchmark Flight store (Flight RPC for cross-node, or VM for same-node).

    Which path is used depends on whether producer and consumer are on the
    same node — the core integration handles this automatically.
    """
    if not os.environ.get("RAY_USE_FLIGHT_STORE"):
        print("\n--- Flight Store: SKIPPED (set RAY_USE_FLIGHT_STORE=1) ---")
        return

    print("\n--- Flight Store ---")
    print("  Throughput:")
    for size_mb in sizes_mb:
        bench_throughput(ray_produce, ray_consume, size_mb, n_iters, "flight-store")
    print("  Latency:")
    for size_mb in sizes_mb:
        bench_latency(ray_produce, ray_consume, size_mb, min(n_iters, 20), "flight-store")


def run_cross_node(sizes_mb, n_iters):
    """Benchmark cross-node transfers (producer on node2, consumer on head).

    Requires a 2-node cluster (e.g., via cluster_utils.Cluster).
    """
    nodes = ray.nodes()
    alive = [n for n in nodes if n["Alive"]]
    if len(alive) < 2:
        print("\n--- Cross-node: SKIPPED (need 2+ nodes) ---")
        return

    print("\n--- Cross-node transfers ---")

    # Ray baseline: produce on node2, consume on head.
    print("  Ray Object Store:")
    for size_mb in sizes_mb:
        bench_throughput(
            ray_produce_node2, ray_consume, size_mb, n_iters, "ray-cross-node"
        )

    if os.environ.get("RAY_USE_FLIGHT_STORE"):
        print("  Flight Store:")
        for size_mb in sizes_mb:
            bench_throughput(
                ray_produce_node2, ray_consume, size_mb, n_iters, "flight-cross-node"
            )


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Ray object store vs Flight store"
    )
    parser.add_argument(
        "--mode",
        choices=["ray", "flight", "cross-node", "all"],
        default="all",
        help="Which benchmark to run (default: all)",
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default="1,10,50,200",
        help="Comma-separated table sizes in MB (default: 1,10,50,200)",
    )
    parser.add_argument(
        "--iters",
        type=int,
        default=50,
        help="Number of iterations per size (default: 50)",
    )
    parser.add_argument(
        "--num-cpus",
        type=int,
        default=4,
        help="Number of CPUs for ray.init (default: 4)",
    )
    args = parser.parse_args()

    sizes_mb = [int(s) for s in args.sizes.split(",")]

    ray.init(num_cpus=args.num_cpus)

    flight_status = "ON" if os.environ.get("RAY_USE_FLIGHT_STORE") else "OFF"
    print(f"Flight store: {flight_status}")
    print(f"Sizes: {sizes_mb} MB")
    print(f"Iterations: {args.iters}")
    print(f"Nodes: {len([n for n in ray.nodes() if n['Alive']])}")

    if args.mode in ("ray", "all"):
        run_ray_baseline(sizes_mb, args.iters)
    if args.mode in ("flight", "all"):
        run_flight(sizes_mb, args.iters)
    if args.mode in ("cross-node", "all"):
        run_cross_node(sizes_mb, args.iters)

    print("\nDone.")
    ray.shutdown()


if __name__ == "__main__":
    main()
