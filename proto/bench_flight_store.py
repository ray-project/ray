"""Benchmark: streaming producer->consumer Arrow table transfer through Ray.

Producer actors return pa.Table; consumer actors take it as input. The
driver continuously submits produce->consume pairs, keeping at most
--max-in-flight outstanding, for --duration seconds. Reports steady-state
throughput and per-task latency.

CLI:
  --mode              ray | arrow-rdt | arrow-native (transfer path)
  --placement         same-node | cross-node | mixed (actor placement)
  --consumer-mode     read-only | modify (consumer work)
  --num-actor-pairs   number of producer/consumer actor pairs
  --concurrency       max_concurrency per actor
  --max-in-flight     cap on outstanding produce->consume pairs (default:
                      num-actor-pairs * concurrency)
  --duration          steady-state measurement duration (seconds)
  --sizes-mb          table sizes to sweep

Load balancing: each produced ObjectRef is handed to the consumer with the
fewest currently-outstanding tasks.

Cross-node and mixed require >= 2 worker nodes in the cluster.
"""

import argparse
import time

import numpy as np
import pyarrow as pa

import ray


MODE_LABELS = {
    "ray": "Ray object store (plasma)",
    "arrow-native": "Arrow Flight (native, RAY_USE_FLIGHT_NATIVE=1)",
    "arrow-rdt": "Arrow Flight (RDT, ARROW_FLIGHT transport)",
}


def parse_args():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--mode", choices=list(MODE_LABELS), default="ray")
    p.add_argument(
        "--placement", choices=["same-node", "cross-node", "mixed"], default="same-node"
    )
    p.add_argument("--consumer-mode", choices=["read-only", "modify"], default="read-only")
    p.add_argument("--num-actor-pairs", type=int, default=4)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument(
        "--max-in-flight",
        type=int,
        default=None,
        help="default: num-actor-pairs * concurrency",
    )
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--sizes-mb", type=int, nargs="+", default=[1, 10, 100])
    return p.parse_args()


# ------------------------------------------------------------------- actors

def _enable_ptrace():
    """Enable process_vm_readv/writev between sibling workers.

    Must run inside the actor process, since yama/ptrace_scope is read at
    syscall time per-caller. Requires passwordless sudo on the node.
    """
    import subprocess

    subprocess.check_output(
        "echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope",
        shell=True,
    )


def _make_producer_cls(mode: str, concurrency: int):
    if mode == "arrow-rdt":

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Producer:
            def __init__(self):
                _enable_ptrace()

            @ray.method(tensor_transport="ARROW_FLIGHT")
            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    else:

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Producer:
            def __init__(self):
                _enable_ptrace()

            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    return Producer


def _make_consumer_cls(consumer_mode: str, concurrency: int):
    if consumer_mode == "read-only":

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Consumer:
            def __init__(self):
                _enable_ptrace()

            def process(self, table):
                assert isinstance(table, pa.Table), f"got {type(table)}"
                return table.num_rows

    else:

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Consumer:
            def __init__(self):
                _enable_ptrace()

            def process(self, table):
                assert isinstance(table, pa.Table), f"got {type(table)}"
                # In-place mutate every numeric column. writable=True forces
                # a writable view; zero-copy when the source buffer is
                # mutable (flight path's pre-allocated local buffer), a
                # copy when not (plasma's immutable shared memory). Either
                # way the consumer pays the write cost over the whole
                # column.
                for col in table.columns:
                    if not (
                        pa.types.is_floating(col.type)
                        or pa.types.is_integer(col.type)
                    ):
                        continue
                    for chunk in col.chunks:
                        arr = chunk.to_numpy(zero_copy_only=False, writable=True)
                        arr += 1
                return table.num_rows

    return Consumer


# ----------------------------------------------------------------- placement

def _worker_nodes():
    """Alive nodes with at least 1 CPU — excludes head-only / dead nodes."""
    out = []
    for node in ray.nodes():
        if node.get("Alive") and node.get("Resources", {}).get("CPU", 0) >= 1:
            out.append(node["NodeID"])
    return out


def _plan_placement(placement: str, num_pairs: int):
    nodes = _worker_nodes()
    if placement == "same-node":
        node = nodes[0]
        return [node] * num_pairs, [node] * num_pairs, nodes
    if len(nodes) < 2:
        raise RuntimeError(
            f"placement={placement!r} requires >= 2 worker nodes; cluster has "
            f"{len(nodes)}"
        )
    if placement == "cross-node":
        return [nodes[0]] * num_pairs, [nodes[1]] * num_pairs, nodes
    if placement == "mixed":
        producer_nodes = [nodes[i % 2] for i in range(num_pairs)]
        consumer_nodes = [nodes[i % 2] for i in range(num_pairs)]
        return producer_nodes, consumer_nodes, nodes
    raise ValueError(f"unknown placement: {placement}")


def _create_actors(cls, node_ids):
    actors = []
    for node_id in node_ids:
        actors.append(
            cls.options(label_selector={"ray.io/node-id": node_id}).remote()
        )
    return actors


# ---------------------------------------------------------------------- core

class _Stream:
    """Streaming submit/drain state. Caller drives it via submit() + wait()."""

    def __init__(self, producers, consumers, size_mb):
        self._producers = producers
        self._consumers = consumers
        self._size_mb = size_mb
        self._in_flight = [0] * len(consumers)
        self._pending = []
        self._submit_times = {}
        self._ref_idx = {}
        self._prod_rr = 0

    def fill(self, target: int):
        """Submit until there are `target` outstanding pairs."""
        while len(self._pending) < target:
            producer = self._producers[self._prod_rr % len(self._producers)]
            self._prod_rr += 1
            # Consumer LB: argmin outstanding.
            idx = min(range(len(self._consumers)), key=lambda i: self._in_flight[i])
            ref = producer.make_table.remote(self._size_mb)
            cref = self._consumers[idx].process.remote(ref)
            self._in_flight[idx] += 1
            self._ref_idx[cref] = idx
            self._submit_times[cref] = time.perf_counter()
            self._pending.append(cref)

    def wait_available(self, timeout: float = 0.001):
        """Poll for all refs completed within `timeout`. Returns a list of
        observed latencies (seconds). Empty list if the pipeline is idle."""
        if not self._pending:
            return []
        done, self._pending = ray.wait(
            self._pending,
            num_returns=len(self._pending),
            timeout=timeout,
            fetch_local=False,
        )
        now = time.perf_counter()
        latencies = []
        for ref in done:
            latencies.append(now - self._submit_times.pop(ref))
            self._in_flight[self._ref_idx.pop(ref)] -= 1
        return latencies

    def drain(self):
        """Drain remaining without recording stats — for clean shutdown."""
        while self._pending:
            done, self._pending = ray.wait(
                self._pending,
                num_returns=len(self._pending),
                fetch_local=False,
            )
            for r in done:
                self._submit_times.pop(r, None)
                self._ref_idx.pop(r, None)

    @property
    def outstanding(self) -> int:
        return len(self._pending)


def bench(producers, consumers, size_mb, duration_s, max_in_flight):
    stream = _Stream(producers, consumers, size_mb)

    # Warmup: fill the pipeline and drain it once so first-time allocation /
    # actor startup / server boot is out of the measurement window.
    stream.fill(max_in_flight)
    while stream.outstanding:
        stream.wait_available(timeout=0.001)

    # Steady-state streaming window.
    latencies = []
    t0 = time.perf_counter()
    end = t0 + duration_s
    while time.perf_counter() < end:
        stream.fill(max_in_flight)
        latencies.extend(stream.wait_available(timeout=0.001))
    completed = len(latencies)
    elapsed = time.perf_counter() - t0

    # Don't leave tasks hanging for the next size_mb iteration.
    stream.drain()

    lat_sorted = sorted(latencies)
    if lat_sorted:
        avg_ms = sum(latencies) / len(latencies) * 1000
        p50_ms = lat_sorted[len(lat_sorted) // 2] * 1000
        p99_ms = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.99))] * 1000
    else:
        avg_ms = p50_ms = p99_ms = 0.0
    tables_per_s = completed / elapsed if elapsed > 0 else 0.0
    mb_per_s = size_mb * tables_per_s
    print(
        f"  {size_mb:4d} MB  done={completed:5d}  elapsed={elapsed:5.2f}s  "
        f"avg={avg_ms:6.1f}ms  p50={p50_ms:6.1f}ms  p99={p99_ms:6.1f}ms  "
        f"tables/s={tables_per_s:7.1f}  throughput={mb_per_s:8.1f} MB/s"
    )


def main():
    args = parse_args()

    runtime_env = None
    if args.mode == "arrow-native":
        runtime_env = {"env_vars": {"RAY_USE_FLIGHT_NATIVE": "1"}}
    ray.init(runtime_env=runtime_env)

    producer_nodes, consumer_nodes, all_nodes = _plan_placement(
        args.placement, args.num_actor_pairs
    )

    Producer = _make_producer_cls(args.mode, args.concurrency)
    Consumer = _make_consumer_cls(args.consumer_mode, args.concurrency)

    producers = _create_actors(Producer, producer_nodes)
    consumers = _create_actors(Consumer, consumer_nodes)

    max_in_flight = args.max_in_flight
    if max_in_flight is None:
        max_in_flight = args.num_actor_pairs * args.concurrency

    print(f"Mode:          {MODE_LABELS[args.mode]}")
    print(f"Placement:     {args.placement}  (cluster has {len(all_nodes)} worker nodes)")
    print(f"Consumer mode: {args.consumer_mode}")
    print(
        f"Actor pairs:   {args.num_actor_pairs} producers + {args.num_actor_pairs} "
        f"consumers across prod={len(set(producer_nodes))} cons={len(set(consumer_nodes))} node(s)"
    )
    print(f"Concurrency:   {args.concurrency} per actor")
    print(f"Max in flight: {max_in_flight}")
    print(f"Duration:      {args.duration:.1f}s per size")
    print(f"Sizes:         {args.sizes_mb} MB")
    print()

    for size_mb in args.sizes_mb:
        bench(producers, consumers, size_mb, args.duration, max_in_flight)

    ray.shutdown()


if __name__ == "__main__":
    main()
