"""One-knob benchmark: v2 (plasma hash-shuffle) vs v3 (file-transport).

Unifies benchmark_v3_ooc.py and benchmark_ooc_shuffle.py. The only mandatory
difference between v2 and v3 is which `DataContext` flags get flipped --
everything else (read, limit, repartition, write, stats collection) is shared.

  v2 -> use_hash_shuffle_v3 = False, shuffle_strategy = HASH_SHUFFLE
        (plasma map outputs; spills via object store)
  v3 -> use_hash_shuffle_v3 = True,  shuffle_strategy = HASH_SHUFFLE
        (file-transport; local disk + socket; plasma carries small handles)

Optional rich-measurement features (timeline dump, raylet spill-event
collection) are gated behind flags and degrade gracefully when the
spill_metrics_dump helper isn't on PYTHONPATH (so this script works on a
bare Anyscale workspace as well as on the original benchmark harness).

Usage:
  # quick: one shot, just print the wall-clock
  python bench_shuffle.py --shuffle v3 --data-size-gb 100 --num-partitions 100

  # both back-to-back (prints two RESULT lines for easy diff)
  python bench_shuffle.py --shuffle both --data-size-gb 100 --num-partitions 100

  # full measurement: timeline + spill metrics + per-run stats
  RAY_DATA_SHUFFLE_PROFILE=1 \
  python bench_shuffle.py --shuffle v3 --data-size-gb 512 --num-partitions 512 \
      --timeline-out /tmp/v3.json --collect-spill-metrics
"""

import argparse
import gc
import json
import os
import shutil
import sys
import time
from datetime import datetime

import ray
from ray.data.context import DataContext, ShuffleStrategy

KEY_COLUMNS = ["column00"]  # l_orderkey
APPROX_BYTES_PER_ROW = 145

# Env vars that the driver should forward to workers (Anyscale workspace
# attaches to a running cluster, so driver-side os.environ doesn't
# propagate by default). Anything read at module-import time on a worker
# (e.g. RAY_DATA_SHUFFLE_PROFILE in _shuffle_tasks.py) must go here.
_WORKER_ENV_VARS_TO_FORWARD = ("RAY_DATA_SHUFFLE_PROFILE",)


def pick_sf(data_size_gb: int) -> int:
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(f"    draining object store ({used_pct:.0f}% used)...", flush=True)
        time.sleep(poll_s)
    print(f"    object store drain timed out after {timeout_s}s", flush=True)


def configure_shuffle(ctx, shuffle: str) -> None:
    """Flip the one knob that distinguishes v2 from v3. Both keep the
    HASH_SHUFFLE strategy (the v3 flag just routes through a different
    transport layer when True)."""
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_hash_shuffle_v3 = shuffle == "v3"


def _maybe_truncate_spill_logs() -> None:
    """Try to truncate raylet event logs on all nodes. No-op when the
    spill_metrics_dump helper isn't importable (e.g. on a bare workspace
    without that companion script)."""
    try:
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except Exception:
        return

    @ray.remote(num_cpus=0)
    def _truncate(paths):
        out = []
        for p in paths:
            try:
                open(p, "w").close()
                out.append(f"truncated {p}")
            except OSError as e:
                out.append(f"FAILED to truncate {p}: {e}")
        return out

    paths = ["/tmp/raylet_spill_events.out", "/tmp/raylet_pull_events.out"]
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    futures = []
    for n in nodes:
        sched = NodeAffinitySchedulingStrategy(node_id=n["NodeID"], soft=False)
        futures.append(_truncate.options(scheduling_strategy=sched).remote(paths))
    for lines in ray.get(futures):
        for line in lines:
            print(f"  [event-log] {line}", flush=True)


def run_one(
    *,
    data_size_gb: int,
    num_partitions: int,
    shuffle: str,
    output_path: str,
    dump_stats: bool,
) -> dict:
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    shutil.rmtree(output_path, ignore_errors=True)

    ds = ray.data.read_parquet(path).limit(target_rows)
    configure_shuffle(ds.context, shuffle)
    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)

    print(
        f"  [{shuffle}] read(sf{sf}, limit {target_rows:,}) "
        f"+ shuffle -> {num_partitions} partitions + write ... ",
        end="",
        flush=True,
    )
    start = time.perf_counter()
    repartitioned.write_parquet(output_path)
    elapsed = time.perf_counter() - start
    print(f"{elapsed:.1f}s ({target_rows:,} rows, {data_size_gb} GB)", flush=True)

    stats_str = None
    if dump_stats:
        stats_str = repartitioned.stats()
        print("\n===== ds.stats() =====\n" + stats_str + "\n", flush=True)

    del repartitioned, ds
    gc.collect()
    shutil.rmtree(output_path, ignore_errors=True)
    wait_for_object_store_to_drain()

    gbps = data_size_gb / elapsed if elapsed > 0 else 0.0
    return {
        "shuffle": shuffle,
        "data_size_gb": data_size_gb,
        "num_partitions": num_partitions,
        "sf": sf,
        "rows": target_rows,
        "elapsed_s": elapsed,
        "throughput_gbps": gbps,
        "stats": stats_str,
    }


def _wait_for_fleet(target_cpu: int, timeout_s: int = 1200) -> None:
    print(f"Waiting for {target_cpu} CPU to come online ...", flush=True)
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        cur = ray.cluster_resources().get("CPU", 0)
        if cur >= target_cpu:
            break
        time.sleep(10)
    final = ray.cluster_resources().get("CPU", 0)
    print(f"Fleet ready: {final:.0f} CPU", flush=True)


def _print_cluster_summary(data_size_gb: int) -> None:
    c = ray.cluster_resources()
    cpu = c.get("CPU", 0)
    mem_gb = c.get("memory", 0) / 1e9
    obj_gb = c.get("object_store_memory", 0) / 1e9
    nodes = len([n for n in ray.nodes() if n.get("Alive")])
    in_core = obj_gb / 3 if obj_gb > 0 else 0
    ratio = data_size_gb / in_core if in_core > 0 else float("inf")
    zone = "SPILL/OOC" if ratio > 1 else "in-core"
    print(
        f"Cluster: {nodes} nodes, {cpu:.0f} CPU, "
        f"{mem_gb:.0f} GB mem, {obj_gb:.0f} GB obj store",
        flush=True,
    )
    print(
        f"In-core limit (obj/3) ~= {in_core:.0f} GB; "
        f"test {data_size_gb} GB => {ratio:.1f}x ({zone})",
        flush=True,
    )


def _print_result_line(r: dict) -> None:
    print(
        f"\nRESULT shuffle={r['shuffle']} "
        f"size={r['data_size_gb']}GB "
        f"parts={r['num_partitions']} "
        f"rows={r['rows']:,} "
        f"wall={r['elapsed_s']:.1f}s "
        f"throughput={r['throughput_gbps']:.2f}GB/s",
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--shuffle",
        choices=["v2", "v3", "both"],
        default="v3",
        help="Pick one transport, or 'both' to run v2 then v3 "
        "back-to-back for direct comparison.",
    )
    p.add_argument("--data-size-gb", type=int, required=True)
    p.add_argument("--num-partitions", type=int, required=True)
    p.add_argument(
        "--target-cpu",
        type=int,
        default=256,
        help="Wait for this many CPU before starting. Default 256.",
    )
    p.add_argument(
        "--output-path",
        type=str,
        default="/tmp/shuffle_output",
        help="Local sink directory for write_parquet.",
    )
    p.add_argument(
        "--timeline-out",
        type=str,
        default=None,
        help="If set, dump ray.timeline() (Chrome trace) "
        "after each run as <path>.<shuffle>.json.",
    )
    p.add_argument(
        "--collect-spill-metrics",
        action="store_true",
        help="Try to truncate + harvest raylet event logs. "
        "No-op when the supporting helpers aren't importable.",
    )
    p.add_argument(
        "--result-json",
        type=str,
        default=None,
        help="If set, dump all RESULT dicts as a JSON array to " "this path.",
    )
    p.add_argument(
        "--stats", action="store_true", help="Print ds.stats() after each run."
    )
    args = p.parse_args()

    forwarded = {
        k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD if k in os.environ
    }
    runtime_env = {"env_vars": forwarded} if forwarded else None
    ray.init(address="auto", runtime_env=runtime_env)

    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True

    _wait_for_fleet(args.target_cpu)
    _print_cluster_summary(args.data_size_gb)

    shuffles = ["v2", "v3"] if args.shuffle == "both" else [args.shuffle]
    print(
        f"Running: {', '.join(shuffles)} ({args.num_partitions} partitions)\n",
        flush=True,
    )

    results = []
    for s in shuffles:
        if args.collect_spill_metrics:
            _maybe_truncate_spill_logs()

        t0 = time.time()
        bench_start = time.time()
        r = run_one(
            data_size_gb=args.data_size_gb,
            num_partitions=args.num_partitions,
            shuffle=s,
            output_path=args.output_path,
            dump_stats=args.stats,
        )

        if args.timeline_out:
            tl_path = f"{args.timeline_out}.{s}.json"
            try:
                ray.timeline(filename=tl_path)
                print(
                    f"TIMELINE {tl_path} " f"(bench_start_ts={bench_start:.3f})",
                    flush=True,
                )
            except Exception as e:
                print(f"timeline dump failed: {e}", flush=True)

        _print_result_line(r)
        print(
            f"(wall-clock total incl. setup: {time.time()-t0:.1f}s)  "
            f"{datetime.now().isoformat()}",
            flush=True,
        )
        results.append(r)

    if args.shuffle == "both" and len(results) == 2:
        v2_t = results[0]["elapsed_s"]
        v3_t = results[1]["elapsed_s"]
        speedup = v2_t / v3_t if v3_t > 0 else float("inf")
        print(
            f"\nSPEEDUP v3 vs v2: {speedup:.2f}x  " f"(v2={v2_t:.1f}s, v3={v3_t:.1f}s)",
            flush=True,
        )

    if args.result_json:
        # Strip stats text from JSON output (too verbose for an aggregate dump);
        # individual stats already went to stdout if --stats was set.
        slim = [{k: v for k, v in r.items() if k != "stats"} for r in results]
        with open(args.result_json, "w") as f:
            json.dump(slim, f, indent=2)
        print(f"RESULTS_JSON {args.result_json}", flush=True)

    print("DONE", flush=True)


if __name__ == "__main__":
    sys.exit(main())
