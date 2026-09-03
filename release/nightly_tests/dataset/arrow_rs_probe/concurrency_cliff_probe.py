"""Concurrency-cliff probe (M92): does the native S3 read path fall off a cliff
when concurrent read tasks on one node exceed ~physical cores, and is the node
actually out of runnable-thread capacity when it happens?

Background (2026-09-01.md §9): in the 2x2 release run, arrow-rs read tasks ran at
pyarrow speed (or faster) up to ~47 concurrent tasks on an m5.24xlarge, then
jumped 5-8x at >=48 (= 96 hyperthreads / 2). Hypothesis: an rs task keeps ~2
threads busy under real S3 (tokio decode task + fetch/TLS work + the Python
consumer) vs pyarrow's ~1, so Ray's num_cpus=1 accounting oversubscribes the
node at ~cores/2 tasks. This script measures BOTH the wall-vs-concurrency curve
and the runnable-thread count, so it discriminates:

  * cliff in wall AND runnable threads ~= hardware threads at the cliff
      -> CPU/thread oversubscription confirmed (fix: honest num_cpus, or make a
         task ~1 busy thread: shrink tokio pool / decode on calling thread).
  * cliff in wall while runnable threads stay LOW
      -> tasks are stalling, not competing for CPU (fetch starvation, scheduler
         or channel pathology) -> different fix, profile with
         RAY_DATA_ARROW_RS_PROFILE=1.
  * no cliff at all on this box
      -> the release loss needs something this box lacks; escalate to the
         release confirmation cell instead.

Run on a LINUX box with real S3 credentials (the effect needs real network +
TLS; moto/localhost shows nothing — measured: single-read avg busy threads
0.4-0.6 rs vs 0.9 pa on moto). One command, both arms, sweeps concurrency:

  python concurrency_cliff_probe.py --path s3://<bucket>/cliff_probe --gen 600
  python concurrency_cliff_probe.py --path s3://<bucket>/cliff_probe

Per (arm, N): fresh ray.init, ray.data.read_parquet(path, concurrency=N),
consume via .sum(), while a sampler thread counts runnable (R-state) threads
across all ray:: worker processes at 5 Hz plus 1-min load average. Expected
signature if M92 holds (T = hardware threads of the box):
  pyarrow : wall keeps improving (or flat) as N grows through T
  arrow-rs: wall improves until N ~= T/2, then DEGRADES; runnable ~= T there.
"""

import argparse
import glob
import json
import os
import re
import threading
import time

FLAG = "RAY_DATA_USE_ARROW_RS_PARQUET_READER"


# ---------------------------------------------------------------- fixtures
def gen_fixtures(path: str, n_files: int, rows_per_file: int) -> None:
    """~64 MiB/file at the default 4M rows: int64 + float64 + short string,
    snappy, 4 row groups — the boring many-medium-files shape (like rlp)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import fs as pafs

    fs, root = pafs.FileSystem.from_uri(path)
    rng = np.random.default_rng(0)
    t = pa.table(
        {
            "a": rng.integers(0, 1 << 40, rows_per_file),
            "b": rng.random(rows_per_file),
            "c": pa.array(rng.integers(0, 99999, rows_per_file).astype("U8")),
        }
    )
    for i in range(n_files):
        with fs.open_output_stream(f"{root}/part-{i:05d}.parquet") as f:
            pq.write_table(
                t, f, row_group_size=rows_per_file // 4, compression="snappy"
            )
        if i % 50 == 0:
            print(f"  wrote {i}/{n_files}", flush=True)
    print(f"fixtures: {n_files} files at {path}", flush=True)


# ------------------------------------------------- runnable-thread sampler
def _ray_worker_pids():
    pids = []
    for cmdline in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            with open(cmdline, "rb") as f:
                if f.read().split(b"\x00", 1)[0].startswith(b"ray::"):
                    pids.append(int(cmdline.split("/")[2]))
        except OSError:
            continue
    return pids


def _runnable_threads(pids):
    n = 0
    for pid in pids:
        for stat in glob.glob(f"/proc/{pid}/task/[0-9]*/stat"):
            try:
                with open(stat) as f:
                    # state is the first field after the last ')' (comm may
                    # contain spaces/parens)
                    if f.read().rsplit(")", 1)[1].split()[0] == "R":
                        n += 1
            except OSError:
                continue
    return n


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.samples = []
        self.stop_evt = threading.Event()

    def run(self):
        if not os.path.isdir("/proc"):  # macOS smoke runs: wall-only
            return
        last_scan, pids = 0.0, []
        while not self.stop_evt.is_set():
            if time.time() - last_scan > 2:
                pids, last_scan = _ray_worker_pids(), time.time()
            self.samples.append((_runnable_threads(pids), os.getloadavg()[0]))
            time.sleep(0.2)

    def summary(self):
        if not self.samples:
            return {"runnable_p50": None, "runnable_p90": None, "load1_max": None}
        runnable = sorted(s[0] for s in self.samples)
        return {
            "runnable_p50": runnable[len(runnable) // 2],
            "runnable_p90": runnable[int(len(runnable) * 0.9)],
            "load1_max": max(s[1] for s in self.samples),
        }


# ------------------------------------------------------------------ cells
def run_cell(arm: str, path: str, n: int) -> dict:
    os.environ[FLAG] = "1" if arm == "rs" else "0"
    import ray

    ray.init(
        ignore_reinit_error=True,
        include_dashboard=False,
        runtime_env={"env_vars": {FLAG: os.environ[FLAG]}},
        logging_level="ERROR",
    )
    import ray.data

    sampler = Sampler()
    sampler.start()
    t0 = time.time()
    ds = ray.data.read_parquet(path, concurrency=n)
    ds.sum("a")  # forces full decode of every file; the sum op itself is tiny
    wall = time.time() - t0
    sampler.stop_evt.set()
    sampler.join()

    stats = ds.stats()
    m = re.search(r"(\d+) tasks executed", stats)
    tasks = int(m.group(1)) if m else None
    ray.shutdown()
    return {
        "arm": arm,
        "concurrency": n,
        "wall_s": round(wall, 2),
        "read_tasks": tasks,
        **sampler.summary(),
    }


def default_concurrencies() -> list:
    t = os.cpu_count() or 8
    cand = [t // 8, t // 4, 3 * t // 8, t // 2 - 4, t // 2, t // 2 + 8, 3 * t // 4, t]
    return sorted({c for c in cand if c >= 1})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--path", required=True, help="s3://bucket/prefix with fixture files"
    )
    ap.add_argument(
        "--gen", type=int, default=0, help="generate N fixture files, then exit"
    )
    ap.add_argument("--rows-per-file", type=int, default=4_000_000)
    ap.add_argument("--arms", default="pa,rs")
    ap.add_argument(
        "--concurrencies", default="", help="comma list; default derived from cores"
    )
    ap.add_argument("--out", default="cliff_probe_results.jsonl")
    args = ap.parse_args()

    if args.gen:
        gen_fixtures(args.path, args.gen, args.rows_per_file)
        return

    ns = [int(x) for x in args.concurrencies.split(",") if x] or default_concurrencies()
    print(
        f"box: {os.cpu_count()} hw threads | sweep N={ns} | arms={args.arms}",
        flush=True,
    )
    rows = []
    for n in ns:  # interleave arms per N so cluster/S3 weather cancels
        for arm in args.arms.split(","):
            r = run_cell(arm, args.path, n)
            rows.append(r)
            print(
                f"{arm:>3} N={n:<3} wall {r['wall_s']:>7.2f}s tasks {r['read_tasks']} "
                f"runnable p50/p90 {r['runnable_p50']}/{r['runnable_p90']} load1max {r['load1_max']}",
                flush=True,
            )
            with open(args.out, "a") as f:
                f.write(json.dumps(r) + "\n")

    print("\n=== wall_s by concurrency (rows=arm) ===")
    arms = sorted({r["arm"] for r in rows})
    print("arm | " + " | ".join(f"N={n}" for n in ns))
    for arm in arms:
        vals = {r["concurrency"]: r["wall_s"] for r in rows if r["arm"] == arm}
        print(
            f"{arm:>3} | " + " | ".join(f"{vals.get(n, float('nan')):.1f}" for n in ns)
        )
    print(
        "\nM92 predicts: pa monotone-improving through N=cores; rs improves to ~cores/2 "
        "then degrades, with runnable_p90 ~= hw threads at the cliff. Runnable staying low "
        "while rs wall explodes = stall, not CPU -> profile instead."
    )


if __name__ == "__main__":
    main()
