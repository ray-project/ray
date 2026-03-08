#!/usr/bin/env python3
"""
Rust CoreWorker-based distributed task driver.

Creates a Rust CoreWorker driver that distributes tasks across multiple
Rust CoreWorker workers running on different nodes. Demonstrates:
- All-Rust backend (GCS + Raylet + CoreWorker)
- Multi-node task execution
- Task distribution across 3 AWS nodes

Usage:
    python3 driver.py --gcs-address <gcs_ip>:<gcs_port> \
        --worker <ip>:<port>:<worker_id_hex> \
        --worker <ip>:<port>:<worker_id_hex> \
        --worker <ip>:<port>:<worker_id_hex>
"""
import argparse
import pickle
import socket
import time
import sys
from collections import Counter


def get_private_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="Rust CoreWorker distributed driver")
    parser.add_argument("--gcs-address", required=True, help="GCS server address (ip:port)")
    parser.add_argument("--worker", action="append", required=True,
                        help="Worker address as ip:port:worker_id_hex (repeat for each worker)")
    args = parser.parse_args()

    from _raylet import PyCoreWorker, PyWorkerType

    node_ip = get_private_ip()
    print(f"{'='*60}")
    print(f"  Ray Rust Backend - Multi-Node Distributed Task Demo")
    print(f"{'='*60}")
    print(f"Driver node IP: {node_ip}")
    print(f"GCS address: {args.gcs_address}")

    # Parse worker addresses
    workers = []
    for w in args.worker:
        parts = w.split(":")
        if len(parts) != 3:
            print(f"ERROR: Invalid worker format '{w}', expected ip:port:worker_id_hex")
            sys.exit(1)
        workers.append((parts[0], int(parts[1]), parts[2]))

    print(f"Workers ({len(workers)}):")
    for ip, port, wid in workers:
        print(f"  - {ip}:{port} (id={wid[:16]}...)")

    # Create CoreWorker in driver mode
    # Constructor: (worker_type: int, node_ip_address: str, gcs_address: str, job_id_int: int)
    driver = PyCoreWorker(
        int(PyWorkerType.Driver),
        node_ip,
        args.gcs_address,
        1,  # job_id
    )
    print(f"Driver worker ID: {driver.worker_id()}")

    # Start driver gRPC server (needed for receiving return objects)
    driver_port = driver.start_grpc_server("0.0.0.0", 0)
    print(f"Driver gRPC server on port {driver_port}")

    # Set up multi-worker dispatch (round-robin across all workers)
    worker_tuples = [(ip, port, wid) for ip, port, wid in workers]
    driver.setup_multi_worker_dispatch(worker_tuples)
    print(f"\nMulti-worker dispatch configured (round-robin across {len(workers)} workers)")

    # ─── Test 1: get_node_ip ───────────────────────────────────────────
    print(f"\n{'─'*60}")
    print("Test 1: get_node_ip() - 30 tasks across 3 nodes")
    print(f"{'─'*60}")

    refs = []
    for _ in range(30):
        ref_list = driver.submit_task("get_node_ip", [])
        refs.append(ref_list[0])

    results = []
    for ref in refs:
        data = driver.get([ref.binary()], 10000)
        if data and data[0] is not None:
            data_bytes, metadata_bytes = data[0]
            result = pickle.loads(bytes(data_bytes))
            results.append(result)
        else:
            results.append("TIMEOUT")

    ip_counts = Counter(results)
    print(f"Task distribution across nodes:")
    for ip, count in sorted(ip_counts.items()):
        print(f"  {ip}: {count} tasks")
    print(f"Total tasks: {len(results)}")
    print(f"Unique nodes: {len(ip_counts)}")

    # ─── Test 2: compute_square ────────────────────────────────────────
    print(f"\n{'─'*60}")
    print("Test 2: compute_square(x) - 20 compute tasks")
    print(f"{'─'*60}")

    refs2 = []
    for i in range(20):
        arg = pickle.dumps(i)
        ref_list = driver.submit_task("compute_square", [arg])
        refs2.append(ref_list[0])

    results2 = []
    for ref in refs2:
        data = driver.get([ref.binary()], 10000)
        if data and data[0] is not None:
            data_bytes, metadata_bytes = data[0]
            result = pickle.loads(bytes(data_bytes))
            results2.append(result)
        else:
            results2.append(None)

    node_ips2 = Counter()
    for r in results2:
        if r and isinstance(r, dict):
            node_ips2[r["node_ip"]] += 1
            print(f"  {r['result']:>4} = square({results2.index(r)}) on {r['node_ip']}")

    print(f"\nCompute task distribution:")
    for ip, count in sorted(node_ips2.items()):
        print(f"  {ip}: {count} tasks")

    # ─── Test 3: sum_range ─────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print("Test 3: sum_range(n) - 9 CPU-intensive tasks")
    print(f"{'─'*60}")

    refs3 = []
    for n in [100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000]:
        arg = pickle.dumps(n)
        ref_list = driver.submit_task("sum_range", [arg])
        refs3.append((n, ref_list[0]))

    for n, ref in refs3:
        data = driver.get([ref.binary()], 10000)
        if data and data[0] is not None:
            data_bytes, metadata_bytes = data[0]
            result = pickle.loads(bytes(data_bytes))
            print(f"  sum(0..{n:>7}) = {result['sum']:>15,} on {result['node_ip']}")

    # ─── Test 4: process_text ──────────────────────────────────────────
    print(f"\n{'─'*60}")
    print("Test 4: process_text() - 6 string processing tasks")
    print(f"{'─'*60}")

    texts = ["hello world", "ray rust backend", "distributed computing",
             "multi node cluster", "python tasks", "three aws nodes"]
    refs4 = []
    for text in texts:
        arg = pickle.dumps(text)
        ref_list = driver.submit_task("process_text", [arg])
        refs4.append(ref_list[0])

    for ref in refs4:
        data = driver.get([ref.binary()], 10000)
        if data and data[0] is not None:
            data_bytes, metadata_bytes = data[0]
            result = pickle.loads(bytes(data_bytes))
            print(f"  '{result['original']}' -> '{result['upper']}' (len={result['length']}) on {result['node_ip']}")

    # ─── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  EXPERIMENT SUMMARY")
    print(f"{'='*60}")
    print(f"Backend: 100% Rust (GCS + Raylet + CoreWorker)")
    print(f"Nodes: {len(ip_counts)} (from {len(workers)} workers)")
    print(f"Total tasks executed: {len(results) + len(results2) + len(refs3) + len(refs4)}")
    print(f"All tasks used Rust components:")
    print(f"  - Rust GCS server")
    print(f"  - Rust Raylet (per-node)")
    print(f"  - Rust CoreWorker (driver + workers)")
    print(f"  - gRPC task dispatch (PushTask)")
    print(f"Task distribution: {dict(ip_counts)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
