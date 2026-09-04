"""The two suspect release TPC-H queries, A/B'd on one box (M46 / T27).

A/B #4's only real tpch signals (findings M46, T27; everything else is
symmetric autoscaling noise — group medians 0.99-1.00):

  q9   the T-only spiller: arrow-rs spilled 3.9 GB (A/B #4) / 3.4 GB (A/B #3)
       on autoscaling while PyArrow spilled 0 — wall 1.65x/1.53x follow the
       spill. Multi-join (lineitem x part x supplier x partsupp x orders).
  q20  the ONLY fixed_size-replicated tpch wall loss, and it is
       hash_shuffle_v2-only: fv2 1.18 / av2 1.15 vs 1.01 on both v1 variants.
       Semi-join heavy. Suspect: reader output block granularity interacting
       with hash_shuffle_v2's partitioning.

This probe runs the RELEASE tpch scripts themselves (release/nightly_tests/
dataset/tpch/tpch_q*.py — same code, same public bucket, smaller --sf) in a
fresh process per cell, over the matrix

    queries x shuffle strategies (hash_shuffle, hash_shuffle_v2)
            x readers (RAY_DATA_USE_ARROW_RS_PARQUET_READER=0/1)

and reports wall + spilled_gb per cell, R per (query, strategy). What it can
and cannot settle: a box-visible q20 gap that follows hash_shuffle_v2 under
the rs reader = the block-granularity suspect is real and local; no gap = the
loss needs the release regime (autoscaling cluster / sf1000) — fold into TODO
items 19/20. q9's spill is object-store pressure, so a single node with a
default object store may not reproduce it; a T-vs-B spill *difference* here
would still be signal.

Usage:
  python tpch_probe.py --outdir DIR [--sf 10] [--repeat 1]
      [--queries tpch_q9,tpch_q20] [--strategies hash_shuffle,hash_shuffle_v2]
      [--dry-run]
Needs AWS credentials (public bucket s3://ray-benchmark-data/tpch/parquet).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from release_regression_probe import ARMS, arm_env  # noqa: E402  same dir

DATASET_DIR = os.path.abspath(os.path.join(HERE, ".."))
TPCH_DIR = os.path.join(DATASET_DIR, "tpch")

# One cell, fresh process: import the release query module, time main(), then
# read this session's spill total. ray.init happens here (the scripts only
# init under __main__).
SNIPPET = r"""
import importlib, json, re, sys, time
from types import SimpleNamespace

query, sf, dry = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "1"
sched_mem_gb = int(sys.argv[4])
mod = importlib.import_module(query)
if dry:
    print("CELL_JSON " + json.dumps({"dry_run": True}))
    raise SystemExit(0)
import ray

# _memory inflates only the SCHEDULING memory resource (nothing is allocated).
# Without it, one 8-core/30GB box deadlocks on multi-join hash_shuffle queries:
# two JoinOperators each reserve num_partitions x ~450MB of the ~14.4GiB budget
# and upstream shuffle tasks starve forever (seen on q2 sf10, PyArrow arm too).
ray.init(address="local", **({"_memory": sched_mem_gb << 30} if sched_mem_gb else {}))
# The workspace's own Ray (:6379) coexists with this cell's local instance, and
# the state API's address autodetection dies on "multiple active Ray instances"
# — which silently emptied every per-task stats dist. Pin it to this cell.
import os

os.environ["RAY_ADDRESS"] = ray.get_runtime_context().gcs_address
t0 = time.monotonic()
mod.main(SimpleNamespace(sf=sf))
wall = time.monotonic() - t0
spilled_gb = None
try:
    import ray._private.internal_api as api

    m = re.search(r"Spilled (\d+) MiB", api.memory_summary(stats_only=True))
    spilled_gb = round(int(m.group(1)) / 1024, 3) if m else 0.0
except Exception:
    pass
print("CELL_JSON " + json.dumps({"wall_s": round(wall, 1), "spilled_gb": spilled_gb}))
"""


def run_cell(
    query,
    strategy,
    reader,
    sf,
    outdir,
    dry_run,
    timeout_s,
    sched_mem_gb,
    monitor_interval=1.0,
):
    tag = f"{query}.{strategy}.{reader}"
    env = dict(os.environ)
    env["PYTHONPATH"] = (
        TPCH_DIR + os.pathsep + DATASET_DIR + os.pathsep + env.get("PYTHONPATH", "")
    )
    env["RAY_DATA_DEFAULT_SHUFFLE_STRATEGY"] = strategy
    env["RAY_DATA_BENCH_NODE_MEM_MONITOR"] = "1"
    # Anyscale pins RAY_OVERRIDE_RESOURCES (memory=14.4GiB on this box) and it
    # beats ray.init(_memory=...): that budget deadlocks multi-join hash_shuffle
    # cells (two JoinOperators' aggregator reservations consume all of it).
    # Rewrite just the memory field for the cell; scheduling-only, not allocated.
    if sched_mem_gb and env.get("RAY_OVERRIDE_RESOURCES"):
        ovr = json.loads(env["RAY_OVERRIDE_RESOURCES"])
        ovr["memory"] = sched_mem_gb << 30
        env["RAY_OVERRIDE_RESOURCES"] = json.dumps(ovr)
    arm_env(env, reader)
    env["RAY_DATA_BENCH_NODE_MEM_INTERVAL"] = str(monitor_interval)
    env["TEST_OUTPUT_JSON"] = os.path.join(outdir, f"{tag}.benchmark.json")
    cmd = [
        sys.executable,
        "-c",
        SNIPPET,
        query,
        str(sf),
        "1" if dry_run else "0",
        str(sched_mem_gb),
    ]
    log_path = os.path.join(outdir, f"{tag}.log")
    if not dry_run:
        # A q9 cell at sf10 runs for MINUTES; stream the query's own output to
        # the log live (stdout+stderr interleaved) so `tail -f` shows progress —
        # the buffered version looked like a hang.
        print(f"    -> {tag} running (tail -f {log_path})", flush=True)
    t0 = time.perf_counter()
    timed_out = False
    with open(log_path, "w") as fh:
        fh.write(f"# strategy={strategy} reader={reader} sf={sf}\n")
        fh.flush()
        # start_new_session puts the cell + its local Ray daemons in one process
        # group, so a timeout can reap raylet/gcs/workers too (a bare kill of the
        # driver leaks them, and idle workers hold RSS the next cells then lack).
        proc = subprocess.Popen(
            cmd, env=env, stdout=fh, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            proc.wait(timeout=None if dry_run else timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
    with open(log_path) as fh:
        out = fh.read()
    if timed_out:
        print(f"    !! {tag} TIMEOUT after {timeout_s}s (see {tag}.log)", flush=True)
        return {"timeout_s": timeout_s}
    line = next((ln for ln in out.splitlines() if ln.startswith("CELL_JSON ")), None)
    if line is None:
        print(f"    !! {tag} FAILED rc={proc.returncode} (see {tag}.log)", flush=True)
        print("       " + out.strip()[-400:], flush=True)
        return None
    rec = json.loads(line[len("CELL_JSON ") :])
    rec["wall_incl_startup_s"] = round(time.perf_counter() - t0, 1)
    print(f"    {tag:<40} {rec}", flush=True)
    return rec


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--outdir", required=True)
    p.add_argument("--sf", type=int, default=10)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--queries", default="tpch_q9,tpch_q20")
    p.add_argument("--strategies", default="hash_shuffle,hash_shuffle_v2")
    p.add_argument(
        "--cell-timeout",
        type=int,
        default=int(os.environ.get("TPCH_CELL_TIMEOUT", "1200")),
        help="kill a cell's whole process group after this many seconds",
    )
    p.add_argument(
        "--sched-mem-gb",
        type=int,
        default=int(os.environ.get("PROBE_SCHED_MEM_GB", "64")),
        help="scheduling-only memory resource for the local Ray instance "
        "(0 = stock; stock deadlocks multi-join hash_shuffle cells on one box)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="import-and-exit per cell: validates module/env plumbing offline",
    )
    p.add_argument(
        "--arms",
        default="pa,rs,rseos",
        help=f"comma list from {sorted(ARMS)}; pa is the denominator",
    )
    p.add_argument(
        "--monitor-interval",
        type=float,
        default=float(os.environ.get("RAY_DATA_BENCH_NODE_MEM_INTERVAL", "1.0")),
        help="node-memory sampler period in seconds (release: 1.0; 0.1 = 10 Hz "
        "for the short q6 sustained-wUSS rows)",
    )
    a = p.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    arms = [s.strip() for s in a.arms.split(",") if s.strip()]
    for arm in arms:
        arm_env({}, arm)  # validate names up front

    results = {}
    for query in a.queries.split(","):
        for strategy in a.strategies.split(","):
            for reader in arms:
                runs = [
                    run_cell(
                        query,
                        strategy,
                        reader,
                        a.sf,
                        a.outdir,
                        a.dry_run,
                        a.cell_timeout,
                        a.sched_mem_gb,
                        a.monitor_interval,
                    )
                    for _ in range(a.repeat)
                ]
                good = sorted(
                    (r for r in runs if r and "wall_s" in r),
                    key=lambda r: r["wall_s"],
                )
                results[f"{query}.{strategy}.{reader}"] = (
                    good[len(good) // 2] if good else (runs[0] if runs else None)
                )

    with open(os.path.join(a.outdir, "summary.json"), "w") as fh:
        json.dump(results, fh, indent=2)
    if a.dry_run:
        print("\ndry run OK — all query modules import")
        return

    print("\n================ TPCH PROBE (R = arm/pyarrow) ================")
    print(
        f"{'cell [arm]':<42} {'wall pa':>8} {'wall arm':>8} {'R':>6} "
        f"{'spill pa/arm GB':>15}"
    )
    for query in a.queries.split(","):
        for strategy in a.strategies.split(","):
            pa_r = results.get(f"{query}.{strategy}.pa") or {}
            for arm in (m for m in arms if m != "pa"):
                rs_r = results.get(f"{query}.{strategy}.{arm}") or {}
                wp, wr = pa_r.get("wall_s"), rs_r.get("wall_s")
                ratio = f"{wr / wp:.2f}" if wp and wr else "—"
                spill = f"{pa_r.get('spilled_gb')}/{rs_r.get('spilled_gb')}"
                cell = f"{query}.{strategy} [{arm}]"
                print(
                    f"{cell:<42} {wp or '—':>8} {wr or '—':>8} {ratio:>6} {spill:>15}"
                )
    print(
        "\nRead it as: q20 gap only under hash_shuffle_v2+rs => block-granularity"
        "\nsuspect confirmed locally (M46); no gap => release-regime-only, fold into"
        "\nitems 19/20. Any rs-only spill on q9 = T27 reproduced."
    )


if __name__ == "__main__":
    main()
