"""
Distributed Ray program to test the Rust backend on a 3-node cluster.

Each task returns the IP address of the node it runs on, demonstrating
that work is distributed across all nodes.
"""

import ray
import socket
import time
import os
from collections import Counter

ray.init(address="auto")

print("=" * 60)
print("Ray Cluster Info (Rust Backend)")
print("=" * 60)
print(f"Ray version: {ray.__version__}")
print(f"Cluster resources: {ray.cluster_resources()}")
print(f"Available resources: {ray.available_resources()}")
nodes = ray.nodes()
print(f"Number of nodes: {len(nodes)}")
for node in nodes:
    alive = node.get("Alive", False)
    ip = node.get("NodeManagerAddress", "unknown")
    resources = node.get("Resources", {})
    print(f"  Node {ip}: alive={alive}, resources={resources}")
print()


# --- Task 1: Simple node IP reporter ---
@ray.remote
def get_node_ip():
    """Return the IP address of the node this task runs on."""
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


# --- Task 2: Compute task with node tracking ---
@ray.remote
def compute_square(x):
    """Compute x^2 and report which node did the work."""
    ip = socket.gethostbyname(socket.gethostname())
    return {"input": x, "result": x * x, "node_ip": ip}


# --- Task 3: CPU-bound task ---
@ray.remote
def sum_range(n):
    """Sum integers from 0 to n-1, report the node."""
    ip = socket.gethostbyname(socket.gethostname())
    total = sum(range(n))
    return {"n": n, "sum": total, "node_ip": ip}


# --- Task 4: String processing task ---
@ray.remote
def process_text(text):
    """Count words and characters, report the node."""
    ip = socket.gethostbyname(socket.gethostname())
    return {
        "text_preview": text[:30] + "..." if len(text) > 30 else text,
        "word_count": len(text.split()),
        "char_count": len(text),
        "node_ip": ip,
    }


# --- Task 5: Nested task (task calling another task) ---
@ray.remote
def coordinator(values):
    """Distribute sub-computations and collect results."""
    ip = socket.gethostbyname(socket.gethostname())
    futures = [compute_square.remote(v) for v in values]
    results = ray.get(futures)
    total = sum(r["result"] for r in results)
    return {
        "coordinator_ip": ip,
        "sub_results": results,
        "total": total,
    }


# ============================================================
# Run Task 1: get_node_ip — 30 tasks to ensure distribution
# ============================================================
print("Task 1: get_node_ip() x 30")
print("-" * 40)
start = time.time()
futures = [get_node_ip.remote() for _ in range(30)]
ips = ray.get(futures)
elapsed = time.time() - start
dist = Counter(ips)
print(f"  Completed in {elapsed:.2f}s")
print(f"  Node distribution: {dict(dist)}")
print(f"  Unique nodes used: {len(dist)}")
print()

# ============================================================
# Run Task 2: compute_square — 20 tasks
# ============================================================
print("Task 2: compute_square() x 20")
print("-" * 40)
start = time.time()
futures = [compute_square.remote(i) for i in range(20)]
results = ray.get(futures)
elapsed = time.time() - start
node_dist = Counter(r["node_ip"] for r in results)
print(f"  Completed in {elapsed:.2f}s")
print(f"  Node distribution: {dict(node_dist)}")
for r in results[:5]:
    print(f"    {r['input']}^2 = {r['result']} (on {r['node_ip']})")
print(f"    ... ({len(results) - 5} more)")
print()

# ============================================================
# Run Task 3: sum_range — 15 tasks with varying sizes
# ============================================================
print("Task 3: sum_range() x 15")
print("-" * 40)
start = time.time()
sizes = [10_000 * (i + 1) for i in range(15)]
futures = [sum_range.remote(n) for n in sizes]
results = ray.get(futures)
elapsed = time.time() - start
node_dist = Counter(r["node_ip"] for r in results)
print(f"  Completed in {elapsed:.2f}s")
print(f"  Node distribution: {dict(node_dist)}")
for r in results[:3]:
    print(f"    sum(0..{r['n']}) = {r['sum']} (on {r['node_ip']})")
print(f"    ... ({len(results) - 3} more)")
print()

# ============================================================
# Run Task 4: process_text — 10 tasks
# ============================================================
print("Task 4: process_text() x 10")
print("-" * 40)
texts = [
    "Ray is a distributed computing framework for Python",
    "The Rust backend replaces C++ components with safe Rust code",
    "This cluster has three nodes running the Rust GCS and Raylet",
    "Distributed task scheduling spreads work across all available nodes",
    "Each task returns the IP address of the node where it executes",
    "The Global Control Store manages cluster metadata and state",
    "Raylets handle local resource management and task dispatch",
    "Workers are spawned by raylets to execute user-defined functions",
    "gRPC is used for inter-node communication in the Ray cluster",
    "This experiment validates the Rust backend on a real multi-node setup",
]
start = time.time()
futures = [process_text.remote(t) for t in texts]
results = ray.get(futures)
elapsed = time.time() - start
node_dist = Counter(r["node_ip"] for r in results)
print(f"  Completed in {elapsed:.2f}s")
print(f"  Node distribution: {dict(node_dist)}")
for r in results[:3]:
    print(f"    '{r['text_preview']}' -> {r['word_count']} words (on {r['node_ip']})")
print(f"    ... ({len(results) - 3} more)")
print()

# ============================================================
# Run Task 5: coordinator (nested tasks) — 3 batches
# ============================================================
print("Task 5: coordinator() with nested tasks x 3")
print("-" * 40)
start = time.time()
batches = [[1, 2, 3, 4, 5], [10, 20, 30], [100, 200]]
futures = [coordinator.remote(batch) for batch in batches]
results = ray.get(futures)
elapsed = time.time() - start
print(f"  Completed in {elapsed:.2f}s")
for i, r in enumerate(results):
    sub_nodes = [s["node_ip"] for s in r["sub_results"]]
    print(f"  Batch {i+1}: coordinator on {r['coordinator_ip']}, "
          f"sub-tasks on {sub_nodes}, total={r['total']}")
print()

# ============================================================
# Summary
# ============================================================
print("=" * 60)
print("SUMMARY")
print("=" * 60)
all_ips = set()
all_ips.update(ips)
all_ips.update(r["node_ip"] for r in ray.get([compute_square.remote(0)]))
print(f"Total unique nodes used: {len(dist)}")
print(f"Node IPs: {sorted(dist.keys())}")
print(f"Total tasks executed: {30 + 20 + 15 + 10 + 3} top-level + nested")
print(f"Backend: Rust GCS + Rust Raylet")
print(f"All tasks completed successfully!")
print("=" * 60)

ray.shutdown()
