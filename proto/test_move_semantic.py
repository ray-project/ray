"""
Demonstrates move semantics for the Ray object store.

Without move semantics: the primary copy stays pinned on the producing node
even after the consumer on another node has received it.

With move semantics: the primary copy is freed after push completes,
releasing memory on the producing node.

Usage:
    python proto/test_move_semantic.py
"""

import time

import numpy as np

import ray
from ray.cluster_utils import Cluster

OBJECT_SIZE_MB = 200
NUM_OBJECTS = 5


def get_node_object_store_usage():
    """Return {node_ip: (used_bytes, total_bytes)} for each node."""
    usage = {}
    for node in ray.nodes():
        if not node["Alive"]:
            continue
        node_id = node["NodeID"]
        ip = node["NodeManagerAddress"]
        resources = node["Resources"]
        total = resources.get("object_store_memory", 0)
        available = ray.available_resources().get("object_store_memory", 0)
        usage[ip] = {"node_id": node_id, "total": total}
    return usage


def get_plasma_usage_per_node():
    """Get per-node plasma memory usage from memory_summary."""
    from ray._private.internal_api import memory_summary

    summary = memory_summary(stats_only=True)
    return summary


@ray.remote(num_cpus=1, resources={"producer": 1})
def produce(size_mb):
    """Create a large object on the producer node."""
    data = np.zeros(size_mb * 1024 * 1024 // 8, dtype=np.float64)
    return data


@ray.remote(num_cpus=1, resources={"consumer": 1})
def consume(data):
    """Receive the object on the consumer node (forces a pull)."""
    return len(data)


def main():
    cluster = Cluster()

    # Node 1: producer (head)
    head = cluster.add_node(
        num_cpus=2,
        resources={"producer": 10},
        object_store_memory=500 * 1024 * 1024,
    )

    # Node 2: consumer
    cluster.add_node(
        num_cpus=2,
        resources={"consumer": 10},
        object_store_memory=500 * 1024 * 1024,
    )

    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    print("=" * 60)
    print("Move Semantics Demo")
    print("=" * 60)
    print(f"Creating {NUM_OBJECTS} objects of {OBJECT_SIZE_MB}MB each")
    print(f"Producer node has 500MB object store")
    print(f"Consumer node has 500MB object store")
    print()

    # Phase 1: Produce objects on the producer node.
    print("--- Phase 1: Producing objects on producer node ---")
    refs = []
    for i in range(NUM_OBJECTS):
        ref = produce.remote(OBJECT_SIZE_MB)
        refs.append(ref)
        # Wait for each to be created before submitting next.
        ray.wait([ref], fetch_local=False)
    print(f"  Created {NUM_OBJECTS} objects")
    time.sleep(1)

    print()
    print("--- Memory after production (before transfer) ---")
    print(get_plasma_usage_per_node())

    # Phase 2: Consume objects on the consumer node.
    # This forces a pull from producer → consumer.
    print()
    print("--- Phase 2: Consuming objects on consumer node ---")
    consume_refs = []
    for i, ref in enumerate(refs):
        consume_ref = consume.remote(ref)
        consume_refs.append(consume_ref)

    # Wait for all to complete.
    results = ray.get(consume_refs)
    print(f"  All {NUM_OBJECTS} objects consumed (lengths: {results})")
    time.sleep(2)

    print()
    print("--- Memory after consumption (after transfer) ---")
    print(get_plasma_usage_per_node())
    print()

    # Phase 3: Check if we can produce MORE objects on the producer node.
    # Without move semantics: producer's object store is full (~1000MB used
    #   for 5x200MB), can't produce more.
    # With move semantics: producer freed objects after push, has space.
    print("--- Phase 3: Can we produce more on the producer? ---")
    try:
        extra_refs = []
        for i in range(NUM_OBJECTS):
            ref = produce.remote(OBJECT_SIZE_MB)
            extra_refs.append(ref)
        # Wait with timeout.
        ready, not_ready = ray.wait(extra_refs, num_returns=NUM_OBJECTS, timeout=15)
        if len(ready) == NUM_OBJECTS:
            print(f"  SUCCESS: Produced {NUM_OBJECTS} more objects on producer!")
            print("  (Move semantics freed space after push)")
        else:
            print(f"  Only {len(ready)}/{NUM_OBJECTS} produced within timeout")
            print("  (Primary copies still occupying space — no move semantics)")
    except Exception as e:
        print(f"  FAILED: {e}")
        print("  (Primary copies still occupying space — no move semantics)")

    print()
    print("--- Final memory state ---")
    print(get_plasma_usage_per_node())

    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
