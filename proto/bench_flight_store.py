"""Benchmark: streaming producer->consumer Arrow table transfer through Ray.

Producer actors return pa.Table; consumer actors take it as input. The
driver continuously submits produce->consume pairs, keeping at most
--max-in-flight outstanding, for --duration seconds. Reports steady-state
throughput and per-task latency.

CLI:
  --mode              ray | arrow-rdt | arrow-native (transfer path)
  --dataplane         vm | shm (same-node backend for arrow-* modes)
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
import os
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
        "--dataplane",
        choices=["vm", "shm"],
        default="vm",
        help=(
            "Same-node transfer backend for the arrow-* modes: 'vm' "
            "(process_vm_writev, Linux only) or 'shm' (anonymous shared memory + "
            "SCM_RIGHTS fd passing, Linux + macOS). Ignored for --mode ray."
        ),
    )
    p.add_argument(
        "--transport",
        choices=["flight", "tcp"],
        default="flight",
        help=(
            "Cross-node transport for the arrow-* modes: 'flight' (Arrow Flight / "
            "gRPC DoGet) or 'tcp' (raw length-prefixed Arrow IPC stream over a "
            "plain socket; lower consumer-side CPU). Ignored for --mode ray and "
            "for same-node transfers."
        ),
    )
    p.add_argument(
        "--placement", choices=["same-node", "cross-node", "mixed"], default="same-node"
    )
    p.add_argument(
        "--consumer-mode", choices=["read-only", "modify"], default="read-only"
    )
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
    p.add_argument(
        "--telemetry",
        action="store_true",
        help=(
            "sample per-node CPU-seconds and (non-loopback) network bytes around "
            "each measurement window; report CPU overhead, wire bandwidth, and "
            "amplification vs logical data. Most meaningful for cross-node/mixed."
        ),
    )
    # Append-pipeline mode: a chained N-stage pipeline where each stage appends
    # a derived column and hands the result to the next stage. Compares the
    # "copy" baseline (return the full table each hop, through plasma) against
    # "share" (return a multi-region shm manifest; only the new column is
    # materialized per hop). Runs instead of the steady-state throughput loop.
    p.add_argument(
        "--append-pipeline",
        action="store_true",
        help="run the chained append-column pipeline benchmark and exit",
    )
    p.add_argument(
        "--append-strategy", choices=["copy", "share", "both"], default="both"
    )
    p.add_argument("--append-stages", type=int, default=4, help="number of append hops")
    p.add_argument("--rows", type=int, default=1_250_000, help="rows in the base table")
    p.add_argument(
        "--base-cols", type=int, default=12, help="columns in the base table"
    )
    p.add_argument("--iters", type=int, default=20, help="pipeline iterations to time")
    return p.parse_args()


# ------------------------------------------------------------------- actors


def _enable_ptrace():
    """Enable process_vm_readv/writev between sibling workers.

    Only relevant to the "vm" dataplane on Linux (yama/ptrace_scope gates
    process_vm_readv). No-op elsewhere: macOS has no /proc or yama, and the
    "shm" dataplane and plasma path don't need it. Best-effort — a failure
    here must not kill the actor (that would hang the driver waiting on it).
    """
    import subprocess
    import sys

    if sys.platform != "linux":
        return
    if os.environ.get("RAY_FLIGHT_DATAPLANE", "vm").lower() != "vm":
        return
    try:
        subprocess.check_output(
            "echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope",
            shell=True,
            stderr=subprocess.STDOUT,
        )
    except Exception as e:
        print(f"[warn] _enable_ptrace best-effort failed: {e}")


def _make_producer_cls(mode: str, concurrency: int):
    if mode == "arrow-rdt":

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Producer:
            def __init__(self):
                # _enable_ptrace()
                pass

            @ray.method(tensor_transport="ARROW_FLIGHT")
            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    else:

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Producer:
            def __init__(self):
                # _enable_ptrace()
                pass

            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    return Producer


def _make_consumer_cls(consumer_mode: str, concurrency: int):
    if consumer_mode == "read-only":

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Consumer:
            def __init__(self):
                # _enable_ptrace()
                pass

            def process(self, table):
                assert isinstance(table, pa.Table), f"got {type(table)}"
                return table.num_rows

    else:

        @ray.remote(num_cpus=0, max_concurrency=concurrency)
        class Consumer:
            def __init__(self):
                # _enable_ptrace()
                pass

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
                        pa.types.is_floating(col.type) or pa.types.is_integer(col.type)
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
        actors.append(cls.options(label_selector={"ray.io/node-id": node_id}).remote())
    return actors


# --------------------------------------------------------------- telemetry


@ray.remote(num_cpus=0)
def _node_counters():
    """Snapshot system-wide CPU time and non-loopback network bytes on the node
    this task lands on. Deltas between two snapshots over a window give the CPU
    consumed and the bytes that crossed the wire during it."""
    import psutil

    ct = psutil.cpu_times()
    per_nic = psutil.net_io_counters(pernic=True)
    sent = sum(v.bytes_sent for k, v in per_nic.items() if not k.startswith("lo"))
    recv = sum(v.bytes_recv for k, v in per_nic.items() if not k.startswith("lo"))
    return {
        "cpu_s": ct.user + ct.system,
        "sent": sent,
        "recv": recv,
        "cores": psutil.cpu_count() or 1,
    }


def _snapshot_nodes(node_ids):
    """Return {node_id: counters} sampled concurrently, one task per node."""
    refs = {
        nid: _node_counters.options(label_selector={"ray.io/node-id": nid}).remote()
        for nid in node_ids
    }
    return {nid: ray.get(ref) for nid, ref in refs.items()}


def _telemetry_deltas(snap0, snap1):
    """CPU-seconds, core count, and non-loopback MB sent/received over a window.

    Each transferred byte is sent once (producer node) and received once
    (consumer node), so bytes-sent is used as the wire volume."""
    nodes = snap0.keys()
    cpu_s = sum(snap1[n]["cpu_s"] - snap0[n]["cpu_s"] for n in nodes)
    cores = sum(snap0[n]["cores"] for n in nodes)
    sent_mb = sum(snap1[n]["sent"] - snap0[n]["sent"] for n in nodes) / 1e6
    recv_mb = sum(snap1[n]["recv"] - snap0[n]["recv"] for n in nodes) / 1e6
    return {"cpu_s": cpu_s, "cores": cores, "sent_mb": sent_mb, "recv_mb": recv_mb}


def _print_efficiency(label, tel, elapsed, logical_mb):
    """Print one CPU/network efficiency line from telemetry deltas."""
    cpu_s, cores = tel["cpu_s"], tel["cores"]
    sent_mb, recv_mb = tel["sent_mb"], tel["recv_mb"]
    cpu_util = (cpu_s / (elapsed * cores) * 100) if elapsed and cores else 0.0
    cpu_ms_per_mb = (cpu_s / logical_mb * 1e3) if logical_mb else 0.0
    ampl = (sent_mb / logical_mb) if logical_mb else 0.0
    print(
        f"  {label}  cpu={cpu_s:7.1f}s ({cpu_util:4.1f}% of {cores}c)  "
        f"net_sent={sent_mb:9.1f}MB  net_recv={recv_mb:9.1f}MB  "
        f"logical={logical_mb:9.1f}MB  wire_ampl={ampl:4.2f}x  "
        f"cpu={cpu_ms_per_mb:5.2f}ms/MB"
    )


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


def bench(
    producers, consumers, size_mb, duration_s, max_in_flight, telemetry_nodes=None
):
    stream = _Stream(producers, consumers, size_mb)

    # Warmup: fill the pipeline and drain it once so first-time allocation /
    # actor startup / server boot is out of the measurement window.
    stream.fill(max_in_flight)
    while stream.outstanding:
        stream.wait_available(timeout=0.001)

    # Steady-state streaming window.
    snap0 = _snapshot_nodes(telemetry_nodes) if telemetry_nodes else None
    latencies = []
    t0 = time.perf_counter()
    end = t0 + duration_s
    while time.perf_counter() < end:
        stream.fill(max_in_flight)
        latencies.extend(stream.wait_available(timeout=0.001))
    completed = len(latencies)
    elapsed = time.perf_counter() - t0
    snap1 = _snapshot_nodes(telemetry_nodes) if telemetry_nodes else None

    # Don't leave tasks hanging for the next size_mb iteration.
    stream.drain()

    lat_sorted = sorted(latencies)
    if lat_sorted:
        avg_ms = sum(latencies) / len(latencies) * 1000
        p50_ms = lat_sorted[len(lat_sorted) // 2] * 1000
        p99_ms = (
            lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.99))] * 1000
        )
    else:
        avg_ms = p50_ms = p99_ms = 0.0
    tables_per_s = completed / elapsed if elapsed > 0 else 0.0
    mb_per_s = size_mb * tables_per_s
    print(
        f"  {size_mb:4d} MB  done={completed:5d}  elapsed={elapsed:5.2f}s  "
        f"avg={avg_ms:6.1f}ms  p50={p50_ms:6.1f}ms  p99={p99_ms:6.1f}ms  "
        f"tables/s={tables_per_s:7.1f}  throughput={mb_per_s:8.1f} MB/s"
    )

    logical_mb = size_mb * completed
    result = {"logical_mb": logical_mb, "elapsed": elapsed, "telemetry": None}
    if snap0 is not None and snap1 is not None:
        tel = _telemetry_deltas(snap0, snap1)
        _print_efficiency("telemetry:", tel, elapsed, logical_mb)
        result["telemetry"] = tel
    return result


def run_append_pipeline(args):
    """Chained N-stage append-column pipeline through real Ray actors.

    copy : each stage returns the full (growing) table -> re-serialized into
           plasma every hop.
    share: each stage returns a multi-region shm manifest; only the newly
           appended column is materialized per hop, upstream regions forwarded.
    """
    rows = args.rows
    base_cols = args.base_cols
    n = args.append_stages

    def make_base():
        rng = np.random.default_rng(0)
        return pa.table({f"c{i}": rng.standard_normal(rows) for i in range(base_cols)})

    def derived(stage):
        rng = np.random.default_rng(1000 + stage)
        return pa.array(rng.standard_normal(rows))

    base = make_base()
    base_mb = base.nbytes / 1e6
    col_mb = derived(0).nbytes / 1e6
    final_cols = base_cols + n
    # Bytes pushed through the Ray object store per iteration for `copy`:
    # every hop re-serializes the whole (growing) table.
    copy_store_mb = sum((base_cols + k) for k in range(n + 1)) * (rows * 8) / 1e6
    del base

    @ray.remote(num_cpus=0)
    class CopyStage:
        def produce(self):
            return make_base()

        def append(self, table, stage):
            return table.append_column(f"d{stage}", derived(stage))

        def sink(self, table):
            return table.num_columns

    @ray.remote(num_cpus=0)
    class ShareStage:
        def __init__(self):
            from ray._private.flight_core import get_flight_core

            self._core = get_flight_core()
            self._prev_key = None

        def _gc_prev(self):
            if self._prev_key is not None:
                self._core.delete_shared(self._prev_key)
                self._prev_key = None

        def produce(self, it):
            self._gc_prev()
            key = f"o0:{it}"
            manifest = self._core.store_shared(key, make_base())
            self._prev_key = key
            return manifest

        def append(self, manifest, stage, it):
            self._gc_prev()
            # We only need the handle (to forward the upstream segments); the
            # reconstructed table isn't used here, so let it drop immediately.
            _, handle = self._core.fetch_shared(manifest)
            col = derived(stage)
            key = f"o{stage}:{it}"
            out = self._core.store_shared_append(key, handle, f"d{stage}", col)
            self._prev_key = key
            return out

        def sink(self, manifest):
            table, handle = self._core.fetch_shared(manifest)
            ncols = table.num_columns
            table = None
            handle.close()
            return ncols

    def time_pipeline(run_once):
        run_once(0)  # warmup
        lat = []
        result = None
        for it in range(1, args.iters + 1):
            t0 = time.perf_counter()
            result = run_once(it)
            lat.append(time.perf_counter() - t0)
        return sorted(lat), result

    def summarize(name, lat, extra):
        avg = sum(lat) / len(lat) * 1e3
        p50 = lat[len(lat) // 2] * 1e3
        p99 = lat[min(len(lat) - 1, int(len(lat) * 0.99))] * 1e3
        print(
            f"  {name:6s}  avg={avg:7.2f}ms  p50={p50:7.2f}ms  p99={p99:7.2f}ms  {extra}"
        )

    print(
        f"Append pipeline:  base {base_mb:.1f} MB ({base_cols} cols x {rows} rows), "
        f"{n} append hops, +{col_mb:.1f} MB/hop, final {final_cols} cols"
    )
    print(f"Iterations:       {args.iters}")
    print()

    if args.append_strategy in ("copy", "both"):
        actors = [CopyStage.remote() for _ in range(n + 2)]
        producer, appenders, sink = actors[0], actors[1 : 1 + n], actors[1 + n]

        def run_copy(it):
            ref = producer.produce.remote()
            for k in range(n):
                ref = appenders[k].append.remote(ref, k)
            return ray.get(sink.sink.remote(ref))

        lat, ncols = time_pipeline(run_copy)
        assert ncols == final_cols, f"copy produced {ncols} cols, expected {final_cols}"
        summarize("copy", lat, f"objstore={copy_store_mb:.0f} MB/iter")

    if args.append_strategy in ("share", "both"):
        actors = [ShareStage.remote() for _ in range(n + 2)]
        producer, appenders, sink = actors[0], actors[1 : 1 + n], actors[1 + n]

        def run_share(it):
            ref = producer.produce.remote(it)
            for k in range(n):
                ref = appenders[k].append.remote(ref, k, it)
            return ray.get(sink.sink.remote(ref))

        lat, ncols = time_pipeline(run_share)
        assert (
            ncols == final_cols
        ), f"share produced {ncols} cols, expected {final_cols}"
        # share pushes only tiny manifest dicts + the per-hop column materialized
        # in shm (not through the object store).
        summarize("share", lat, f"objstore~0 MB/iter (+{col_mb * n:.0f} MB shm/iter)")


def main():
    args = parse_args()

    if args.append_pipeline:
        ray.init()
        try:
            run_append_pipeline(args)
        finally:
            ray.shutdown()
        return

    runtime_env = None
    if args.mode in ("arrow-native", "arrow-rdt"):
        env_vars = {
            "RAY_FLIGHT_DATAPLANE": args.dataplane,
            "RAY_FLIGHT_TRANSPORT": args.transport,
        }
        if args.mode == "arrow-native":
            env_vars["RAY_USE_FLIGHT_NATIVE"] = "1"
        # Pass through the fetch-path debug flag so it reaches worker processes.
        if os.environ.get("RAY_FLIGHT_DEBUG"):
            env_vars["RAY_FLIGHT_DEBUG"] = os.environ["RAY_FLIGHT_DEBUG"]
        runtime_env = {"env_vars": env_vars}
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
    if args.mode in ("arrow-native", "arrow-rdt"):
        print(f"Dataplane:     {args.dataplane}  (same-node transfer backend)")
        print(f"Transport:     {args.transport}  (cross-node transport)")
    print(
        f"Placement:     {args.placement}  (cluster has {len(all_nodes)} worker nodes)"
    )
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

    telemetry_nodes = all_nodes if args.telemetry else None
    if args.telemetry:
        print(f"Telemetry:     on ({len(all_nodes)} node(s) sampled)")
        print()

    results = []
    for size_mb in args.sizes_mb:
        results.append(
            bench(
                producers,
                consumers,
                size_mb,
                args.duration,
                max_in_flight,
                telemetry_nodes=telemetry_nodes,
            )
        )

    tel_results = [r for r in results if r["telemetry"] is not None]
    if tel_results:
        total_logical = sum(r["logical_mb"] for r in tel_results)
        total_elapsed = sum(r["elapsed"] for r in tel_results)
        total = {
            "cpu_s": sum(r["telemetry"]["cpu_s"] for r in tel_results),
            "cores": tel_results[0]["telemetry"]["cores"],
            "sent_mb": sum(r["telemetry"]["sent_mb"] for r in tel_results),
            "recv_mb": sum(r["telemetry"]["recv_mb"] for r in tel_results),
        }
        app_mb_s = total_logical / total_elapsed if total_elapsed else 0.0
        wire_mb_s = total["sent_mb"] / total_elapsed if total_elapsed else 0.0
        print()
        print(f"Summary (aggregate over {len(tel_results)} size(s)):")
        _print_efficiency("TOTAL:    ", total, total_elapsed, total_logical)
        print(
            f"             app throughput={app_mb_s:8.1f} MB/s   "
            f"wire bandwidth={wire_mb_s:8.1f} MB/s"
        )

    ray.shutdown()


if __name__ == "__main__":
    main()
