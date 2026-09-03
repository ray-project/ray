"""worker_constant_probe.py — TODO 31: measure the per-worker resident constant.

A/B #3-#5 showed arrow-rs read workers carrying a ~90-105 MB higher resident
floor than pyarrow workers (findings M84), which multiplies into the
sustained-wUSS tripper cluster (M77) at high workers-per-node. This probe
measures it directly instead of inferring it from release aggregates:

  - one cell = a fresh subprocess (reader pinned by env BEFORE the first
    ray/ray.data import — the DataContext singleton fix from the cliff probe)
    starting a fresh 2-CPU local Ray cluster (2, not 1: the V2 listing/footer
    actor pool must not starve the read task) reading ONE file at
    concurrency=1, so one worker does the read and pre-existing
    (workspace/platform) ray processes are excluded by a before-init /proc
    census; the read worker is identified afterwards by USS growth;
  - USS (Private_Clean + Private_Dirty from /proc/<pid>/smaps_rollup) of that
    worker is recorded idle-after-spawn, at 20 Hz through two consecutive
    reads (peak), and settled after each read — read #2 separates a one-time
    constant (after1 == after2) from per-task growth (retention slope);
  - the read is S3 (one file, concurrency=1) so the arrow-rs arm pays its full
    release-path setup: crate .so mapping, shared tokio runtime + blocking
    pool, object_store client, allocator state. The pa arm is the control;
    the rs-pa delta on `after` IS the constant.

Usage (box):
  python worker_constant_probe.py --path s3://<bucket>/cliff_probe/<one-file>.parquet
Requires AWS creds in the env (source env.sh). macOS: /proc absent — Linux only.
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time

FLAG = "RAY_DATA_USE_ARROW_RS_PARQUET_READER"


def _proc_pids():
    return [int(d) for d in os.listdir("/proc") if d.isdigit()]


def _cmdline(pid):
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        return ""


def _uss_kb(pid):
    """USS in KiB from smaps_rollup (Private_Clean + Private_Dirty)."""
    try:
        total = 0
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith(("Private_Clean:", "Private_Dirty:")):
                    total += int(line.split()[1])
        return total
    except OSError:
        return None


class _PeakSampler(threading.Thread):
    """20 Hz USS sampler over a fixed pid set; keeps the per-pid peak."""

    def __init__(self, pids):
        super().__init__(daemon=True)
        self.pids = list(pids)
        self.peak = {p: 0 for p in self.pids}
        # NOT named _stop: threading.Thread.join() calls its own internal
        # self._stop() method, which an Event attribute would shadow.
        self._halt = threading.Event()

    def run(self):
        while not self._halt.is_set():
            for p in self.pids:
                v = _uss_kb(p)
                if v is not None and v > self.peak[p]:
                    self.peak[p] = v
            time.sleep(0.05)

    def stop(self):
        self._halt.set()
        self.join(timeout=2)


def run_cell_body(arm, path):
    want_rs = arm == "rs"
    os.environ[FLAG] = "1" if want_rs else "0"

    before_init = set(_proc_pids())

    import ray

    # address="local" forces a NEW cluster from THIS venv (plain init would
    # auto-discover a workspace raylet via /tmp/ray/ray_current_cluster).
    ray.init(
        address="local", num_cpus=2, include_dashboard=False, logging_level="ERROR"
    )
    import ray.data

    ctx = ray.data.DataContext.get_current()
    ctx.use_arrow_rs_parquet_reader = want_rs
    if want_rs:
        import ray_data_arrow_rs  # noqa: F401  fail loudly if crate absent

    # Warm the pool, then census OUR cluster's workers (new ray:: pids only).
    ray.get(ray.remote(lambda: os.getpid()).remote())
    time.sleep(2)
    workers = [
        p
        for p in _proc_pids()
        if p not in before_init and _cmdline(p).startswith("ray::")
    ]
    if not workers:
        raise RuntimeError("no worker found in the fresh local cluster")
    idle_uss = {p: _uss_kb(p) for p in workers}

    sampler = _PeakSampler(workers)
    sampler.start()

    def one_read():
        ds = ray.data.read_parquet(path, concurrency=1)
        n = 0
        for bundle in ds.iter_internal_ref_bundles():
            n += bundle.num_rows() or 0
        return n

    rows1 = one_read()
    time.sleep(2)  # settle before the after-read floor
    after1 = {p: _uss_kb(p) for p in workers}
    rows2 = one_read()
    time.sleep(2)
    after2 = {p: _uss_kb(p) for p in workers}
    sampler.stop()

    # The read worker is the one that grew; report it (max after2 delta).
    rw = max(workers, key=lambda p: (after2[p] or 0) - (idle_uss[p] or 0))
    result = {
        "arm": arm,
        "rows_read": rows1,
        "rows_read_2": rows2,
        "worker_pid": rw,
        "idle_uss_mb": round((idle_uss[rw] or 0) / 1024, 1),
        "peak_uss_mb": round(sampler.peak[rw] / 1024, 1),
        "after_read1_uss_mb": round((after1[rw] or 0) / 1024, 1),
        "after_read2_uss_mb": round((after2[rw] or 0) / 1024, 1),
        "n_workers_seen": len(workers),
    }
    print("CELLRESULT " + json.dumps(result), flush=True)
    ray.shutdown()


def run_cell_subprocess(arm, path):
    env = dict(os.environ)
    env[FLAG] = "1" if arm == "rs" else "0"
    env.pop("RAY_ADDRESS", None)
    proc = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--cell", arm, "--path", path],
        env=env,
        capture_output=True,
        text=True,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("CELLRESULT "):
            return json.loads(line[len("CELLRESULT ") :])
    raise RuntimeError(
        f"cell {arm} produced no CELLRESULT (rc={proc.returncode})\n"
        f"stdout tail: {proc.stdout[-2000:]}\nstderr tail: {proc.stderr[-2000:]}"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True, help="parquet dir/file (S3 or local)")
    ap.add_argument("--arms", default="pa,rs")
    ap.add_argument("--cell", help="internal: run one arm in-process")
    args = ap.parse_args()

    if args.cell:
        run_cell_body(args.cell, args.path)
        return

    results = [run_cell_subprocess(a.strip(), args.path) for a in args.arms.split(",")]
    print(json.dumps(results, indent=2))
    if len(results) == 2:
        a, b = results
        print(
            f"\nper-worker constant (rs - pa, settled after read 2): "
            f"{b['after_read2_uss_mb'] - a['after_read2_uss_mb']:+.1f} MB "
            f"(idle {b['idle_uss_mb'] - a['idle_uss_mb']:+.1f}, "
            f"peak {b['peak_uss_mb'] - a['peak_uss_mb']:+.1f})"
        )


if __name__ == "__main__":
    main()
