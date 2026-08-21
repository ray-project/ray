"""One-knob benchmark: hash-shuffle v1 vs SHUFFLE_V2 in-memory vs external.

  v1        -> shuffle_strategy = HASH_SHUFFLE (aggregator actors)
  in-memory -> shuffle_strategy = SHUFFLE_V2,
               use_external_hash_shuffle = False
               (map outputs via Ray object store; spills to disk when full)
  external  -> shuffle_strategy = SHUFFLE_V2,
               use_external_hash_shuffle = True
               (file-transport; local disk + Flight; object store carries
                small handles only)

Optional timeline dump is gated behind a CLI flag and off by default.

Usage:
  # quick: one shot, just print the wall-clock
  python bench_shuffle.py --shuffle external --data-size-gb 100 --num-partitions 100

  # both back-to-back (prints two RESULT lines for easy diff)
  python bench_shuffle.py --shuffle both --data-size-gb 100 --num-partitions 100

  # release-test style: stream out with write_parquet (matches prior Anyscale runs;
  # materialize() of 512GB OOMs/spills hard on m5.2xlarge object stores)
  python bench_shuffle.py --shuffle external --data-size-gb 512 --num-partitions 512 \\
      --write-parquet

  # 1 TiB / 1024 partitions
  python bench_shuffle.py --shuffle external --data-size-gb 1024 --num-partitions 1024 \\
      --write-parquet

  # with timeline dump + per-run stats
  RAY_DATA_SHUFFLE_PROFILE=1 \\
  python bench_shuffle.py --shuffle external --data-size-gb 512 --num-partitions 512 \\
      --timeline-out /tmp/external.json --stats
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
from benchmark import Benchmark
from ray.data.context import DataContext, ShuffleStrategy

KEY_COLUMNS = ["column00"]  # l_orderkey
APPROX_BYTES_PER_ROW = 145

# Env vars that the driver should forward to workers (Anyscale workspace
# attaches to a running cluster, so driver-side os.environ doesn't
# propagate by default). Anything read at module-import time on a worker
# (e.g. RAY_DATA_SHUFFLE_PROFILE) must go here.
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
    """Select hash-shuffle engine / transport.

    - ``v1``: aggregator-based ``HASH_SHUFFLE`` (legacy)
    - ``in-memory``: ``SHUFFLE_V2`` with object-store shards
    - ``external``: ``SHUFFLE_V2`` with on-disk / Flight transport
    """
    if shuffle == "v1":
        ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
        ctx.use_external_hash_shuffle = False
    else:
        ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
        ctx.use_external_hash_shuffle = shuffle == "external"


def run_one(
    *,
    data_size_gb: int,
    num_partitions: int,
    shuffle: str,
    output_path: str,
    dump_stats: bool,
    write_parquet: bool,
) -> dict:
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    if write_parquet:
        shutil.rmtree(output_path, ignore_errors=True)

    configure_shuffle(DataContext.get_current(), shuffle)
    ds = ray.data.read_parquet(path).limit(target_rows)
    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)

    sink = "write" if write_parquet else "materialize"
    print(
        f"  [{shuffle}] read(sf{sf}, limit {target_rows:,}) "
        f"+ shuffle -> {num_partitions} partitions + {sink} ... ",
        end="",
        flush=True,
    )
    start = time.perf_counter()
    if write_parquet:
        repartitioned.write_parquet(output_path)
    else:
        repartitioned.materialize()
    elapsed = time.perf_counter() - start
    print(f"{elapsed:.1f}s ({target_rows:,} rows, {data_size_gb} GB)", flush=True)

    stats_str = None
    if dump_stats:
        stats_str = repartitioned.stats()
        print("\n===== ds.stats() =====\n" + stats_str + "\n", flush=True)

    del repartitioned, ds
    gc.collect()
    if write_parquet:
        shutil.rmtree(output_path, ignore_errors=True)
    wait_for_object_store_to_drain()

    gbps = data_size_gb / elapsed if elapsed > 0 else 0.0
    ctx = DataContext.get_current()
    return {
        "shuffle": shuffle,
        "shuffle_strategy": str(ctx.shuffle_strategy),
        "use_external_hash_shuffle": bool(ctx.use_external_hash_shuffle),
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
        choices=["v1", "in-memory", "external", "both"],
        default="external",
        help="v1=HASH_SHUFFLE aggregators; in-memory/external=SHUFFLE_V2 "
        "object-store vs Flight; 'both' runs in-memory then external.",
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
        "--result-json",
        type=str,
        default=None,
        help="If set, dump all RESULT dicts as a JSON array to this path.",
    )
    p.add_argument(
        "--stats", action="store_true", help="Print ds.stats() after each run."
    )
    p.add_argument(
        "--write-parquet",
        action="store_true",
        help="Write shuffled output to --output-path. Default is materialize() "
        "(used by the release test so shuffle is not mixed with local disk I/O).",
    )
    args = p.parse_args()

    forwarded = {
        k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD if k in os.environ
    }
    if not ray.is_initialized():
        runtime_env = {"env_vars": forwarded} if forwarded else None
        ray.init(address="auto", runtime_env=runtime_env)

    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True

    _wait_for_fleet(args.target_cpu)
    _print_cluster_summary(args.data_size_gb)

    shuffles = (
        ["in-memory", "external"] if args.shuffle == "both" else [args.shuffle]
    )
    print(
        f"Running: {', '.join(shuffles)} ({args.num_partitions} partitions)\n",
        flush=True,
    )

    benchmark = Benchmark()
    results = []
    for s in shuffles:
        t0 = time.time()
        bench_start = time.time()

        def _case(shuffle=s):
            return run_one(
                data_size_gb=args.data_size_gb,
                num_partitions=args.num_partitions,
                shuffle=shuffle,
                output_path=args.output_path,
                dump_stats=args.stats,
                write_parquet=args.write_parquet,
            )

        # Benchmark.run_fn times the whole fn (incl. object-store drain) and
        # records spill/peak-util; the shuffle wall we care about is elapsed_s
        # inside the returned dict.
        benchmark.run_fn(s, _case)
        case_metrics = benchmark.result[s]
        r = {
            "shuffle": case_metrics["shuffle"],
            "shuffle_strategy": case_metrics.get("shuffle_strategy"),
            "use_external_hash_shuffle": case_metrics.get(
                "use_external_hash_shuffle"
            ),
            "data_size_gb": case_metrics["data_size_gb"],
            "num_partitions": case_metrics["num_partitions"],
            "sf": case_metrics["sf"],
            "rows": case_metrics["rows"],
            "elapsed_s": case_metrics["elapsed_s"],
            "throughput_gbps": case_metrics["throughput_gbps"],
            # Full Benchmark wall (shuffle + drain/gc); not the RESULT wall.
            "benchmark_wall_s": case_metrics.get("time"),
            "object_store_spilled_total_gb": case_metrics.get(
                "object_store_spilled_total_gb"
            ),
            "object_store_memory_used_peak_gb": case_metrics.get(
                "object_store_memory_used_peak_gb"
            ),
            "object_store_memory_utilization_peak": case_metrics.get(
                "object_store_memory_utilization_peak"
            ),
        }

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
        in_memory_t = results[0]["elapsed_s"]
        external_t = results[1]["elapsed_s"]
        speedup = in_memory_t / external_t if external_t > 0 else float("inf")
        print(
            f"\nSPEEDUP external vs in-memory: {speedup:.2f}x  "
            f"(in-memory={in_memory_t:.1f}s, external={external_t:.1f}s)",
            flush=True,
        )

    # Write RESULT metrics as-is (not Benchmark's nested/mixed schema).
    # Release infra reads TEST_OUTPUT_JSON; default locally is ./result.json.
    out_path = args.result_json or os.environ.get("TEST_OUTPUT_JSON", "./result.json")
    payload = results[0] if len(results) == 1 else {"runs": results}
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"RESULTS_JSON {out_path}", flush=True)
    print(json.dumps(payload, indent=2), flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    sys.exit(main())
