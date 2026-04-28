"""FreeObjects broadcast fan-out repro, two rounds.

Spawns 1 producer + 2 consumers on 3 worker nodes of an N-node cluster
(N-3 nodes stay idle).

Round 1 -- producer-owned objects:
  Both consumers pull into their local plasma via ray.wait(fetch_local=True).
  The driver then clears the producer's refs, driving the owner refcount
  to 0 and triggering the cluster-wide FreeObjects broadcast. Cluster
  plasma usage is polled until it drains back to the pre-produce baseline.

Round 2 -- consumer-owned objects:
  Each consumer then ray.puts its own batch of equally-sized objects.
  The driver drops the consumer refs to trigger a second broadcast free,
  and the drain is polled again. This exercises the lazy-free path on
  copies that land off the owner's node.

Repro, not a benchmark -- no timings printed. Measure externally.

Objects must exceed RAY_max_direct_call_object_size (default 100 KiB) or
Ray inlines them in task specs and nothing reaches plasma.
"""

import argparse
import sys
import time
from typing import Dict, List

import ray
import ray.autoscaler.sdk
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--num-nodes", type=int, default=1000)
    p.add_argument("--num-objects", type=int, default=10_000)
    p.add_argument("--object-size-bytes", type=int, default=160 * 1024)
    p.add_argument("--pull-batch-size", type=int, default=1000)
    p.add_argument("--ramp-timeout-s", type=int, default=2700)
    p.add_argument("--drain-timeout-s", type=int, default=300)
    p.add_argument("--drain-poll-interval-s", type=float, default=1.0)
    p.add_argument("--drain-tolerance-bytes", type=int, default=50 * 1024 * 1024)
    return p.parse_args()


@ray.remote(num_cpus=1)
class Producer:
    def __init__(self, payload_size: int):
        self._payload = b"\0" * payload_size
        self._refs: List[ray.ObjectRef] = []

    def produce(self, n: int) -> int:
        self._refs = [ray.put(self._payload) for _ in range(n)]
        return len(self._refs)

    def get_refs(self) -> List[ray.ObjectRef]:
        return self._refs

    def clear_refs(self) -> int:
        n = len(self._refs)
        self._refs = []
        return n


@ray.remote(num_cpus=1)
class Consumer:
    def __init__(self, payload_size: int):
        self._payload = b"\0" * payload_size
        self._refs: List[ray.ObjectRef] = []

    def pull(self, producer, batch_size: int) -> int:
        refs = ray.get(producer.get_refs.remote())
        for i in range(0, len(refs), batch_size):
            chunk = refs[i : i + batch_size]
            ray.wait(chunk, num_returns=len(chunk), fetch_local=True, timeout=120)
        return len(refs)

    def produce(self, n: int) -> int:
        self._refs = [ray.put(self._payload) for _ in range(n)]
        return len(self._refs)

    def clear_refs(self) -> int:
        n = len(self._refs)
        self._refs = []
        return n


def worker_nodes() -> List[Dict]:
    # Alive, non-head, and actually schedulable for a 1-CPU actor. Filtering
    # just on IsHead/head-marker lets through system nodes with CPU=0 that
    # NodeAffinitySchedulingStrategy(soft=False) will reject as infeasible.
    return [
        n for n in ray.nodes()
        if n.get("Alive")
        and "node:__internal_head__" not in n.get("Resources", {})
        and n.get("Resources", {}).get("CPU", 0) >= 1
    ]


def ramp_cluster(num_nodes: int, timeout_s: int) -> List[Dict]:
    print(f"[ramp] requesting {num_nodes} workers")
    ray.autoscaler.sdk.request_resources(bundles=[{"CPU": 4}] * num_nodes)
    deadline = time.time() + timeout_s
    last_log = 0.0
    nodes: List[Dict] = []
    while time.time() < deadline:
        nodes = worker_nodes()
        if len(nodes) >= num_nodes:
            print(f"[ramp] {len(nodes)} workers ready")
            return nodes[:num_nodes]
        if time.time() - last_log > 10:
            print(f"[ramp] {len(nodes)}/{num_nodes} workers ready")
            last_log = time.time()
        time.sleep(2)
    raise TimeoutError(f"only {len(nodes)}/{num_nodes} workers after {timeout_s}s")


def cluster_plasma_used_bytes() -> int:
    total = ray.cluster_resources().get("object_store_memory", 0.0)
    avail = ray.available_resources().get("object_store_memory", 0.0)
    return int(max(0.0, total - avail))


def wait_for_drain(baseline: int, tolerance: int, timeout_s: int, poll_s: float) -> None:
    target = baseline + tolerance
    print(f"[drain] waiting for plasma <= {target / 1e9:.3f} GB "
          f"(baseline={baseline / 1e9:.3f} GB)")
    deadline = time.time() + timeout_s
    last_log = 0.0
    while time.time() < deadline:
        used = cluster_plasma_used_bytes()
        if used <= target:
            print(f"[drain] reached {used / 1e9:.3f} GB")
            return
        if time.time() - last_log > 5:
            print(f"[drain]   used={used / 1e9:.3f} GB")
            last_log = time.time()
        time.sleep(poll_s)
    used = cluster_plasma_used_bytes()
    raise TimeoutError(f"plasma stuck at {used / 1e9:.3f} GB after {timeout_s}s")


def pin_on(node_id: str) -> NodeAffinitySchedulingStrategy:
    return NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)


def main():
    args = parse_args()
    if args.num_nodes < 3:
        sys.exit(f"--num-nodes must be >= 3 (got {args.num_nodes})")
    if args.object_size_bytes <= 100 * 1024:
        sys.exit("--object-size-bytes must exceed 100 KiB so objects land in plasma")

    print("[init] connecting to cluster")
    ray.init(address="auto")

    nodes = ramp_cluster(args.num_nodes, args.ramp_timeout_s)
    chosen = nodes[:3]
    producer_node = chosen[0]["NodeID"]
    consumer_a_node = chosen[1]["NodeID"]
    consumer_b_node = chosen[2]["NodeID"]
    for role, n in zip(("producer", "consumer_a", "consumer_b"), chosen):
        r = n.get("Resources", {})
        print(f"[place] {role}={n['NodeID'][:8]} "
              f"CPU={r.get('CPU', 0)} "
              f"object_store_memory={r.get('object_store_memory', 0) / 1e9:.2f}GB "
              f"addr={n.get('NodeManagerAddress')}")
    print(f"[place] bystanders={len(nodes) - 3}")

    baseline = cluster_plasma_used_bytes()
    print(f"[init] cluster plasma baseline: {baseline / 1e9:.3f} GB")

    producer = Producer.options(scheduling_strategy=pin_on(producer_node)).remote(
        args.object_size_bytes
    )
    consumer_a = Consumer.options(scheduling_strategy=pin_on(consumer_a_node)).remote(
        args.object_size_bytes
    )
    consumer_b = Consumer.options(scheduling_strategy=pin_on(consumer_b_node)).remote(
        args.object_size_bytes
    )
    ray.get([
        producer.__ray_ready__.remote(),
        consumer_a.__ray_ready__.remote(),
        consumer_b.__ray_ready__.remote(),
    ])
    print("[spawn] producer + 2 consumers ready")

    print(f"[produce] creating {args.num_objects} x {args.object_size_bytes}B objects")
    n = ray.get(producer.produce.remote(args.num_objects))
    print(f"[produce] {n} objects live "
          f"(cluster plasma={cluster_plasma_used_bytes() / 1e9:.3f} GB)")

    print("[pull] both consumers pulling in parallel")
    ray.get([
        consumer_a.pull.remote(producer, args.pull_batch_size),
        consumer_b.pull.remote(producer, args.pull_batch_size),
    ])
    print(f"[pull] done "
          f"(cluster plasma={cluster_plasma_used_bytes() / 1e9:.3f} GB)")

    print("[clear] dropping producer refs -> owner refcount=0 -> broadcast free")
    ray.get(producer.clear_refs.remote())

    wait_for_drain(baseline, args.drain_tolerance_bytes, args.drain_timeout_s,
                   args.drain_poll_interval_s)
    print("[done] broadcast free propagated, plasma drained")

    # Now that the first broadcast has drained, drive a second round entirely
    # from the consumer nodes: each consumer ray.puts its own batch of
    # equally-sized objects, then the driver drops the refs to trigger another
    # broadcast free. This exercises the lazy-free path on the secondary
    # copies that land on the sibling consumer's node (and on any bystander
    # nodes the pull fan-out later touches).
    print(f"[consumer-produce] each consumer producing {args.num_objects} x "
          f"{args.object_size_bytes}B")
    ray.get([
        consumer_a.produce.remote(args.num_objects),
        consumer_b.produce.remote(args.num_objects),
    ])
    print(f"[consumer-produce] done "
          f"(cluster plasma={cluster_plasma_used_bytes() / 1e9:.3f} GB)")

    print("[consumer-clear] dropping consumer refs -> broadcast free")
    ray.get([
        consumer_a.clear_refs.remote(),
        consumer_b.clear_refs.remote(),
    ])

    wait_for_drain(baseline, args.drain_tolerance_bytes, args.drain_timeout_s,
                   args.drain_poll_interval_s)
    print("[done] consumer-produced objects freed, plasma drained")


if __name__ == "__main__":
    main()

