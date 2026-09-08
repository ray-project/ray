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
    census; the sampler keeps DISCOVERING new ray:: workers while it runs
    (Ray may fork a fresh worker for the read instead of reusing a prestarted
    one — a fixed-pid census would then report an idle worker's numbers as
    the read worker's); the read worker is identified POSITIVELY by
    proctitle — Ray retitles an executing worker `ray::<TaskName>`, and the
    sampler records every title a pid ever shows — because USS growth cannot
    identify a worker first seen mid-read (its first-seen baseline already
    holds decode buffers, so its settled delta reads ~0 and loses to an idle
    pid's noise); growth-based selection remains only as a flagged fallback;
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


# Rescan /proc for newly forked workers every N 50 ms ticks (0.5 s: worker
# fork-to-first-task is slower than that, and full-scan cmdline reads at 20 Hz
# would be needless load).
_DISCOVER_EVERY_TICKS = 10

# V2 read tasks execute under this proctitle (ray::ReadFilesParquetV2, both
# arms — the reader flag branches inside the task). A pid that ever bore it is
# the read worker, positively; no match falls back to growth selection.
_READ_TASK_TITLE_SUBSTR = "ReadFiles"


class _WorkerSampler(threading.Thread):
    """20 Hz USS sampler over ray:: workers that discovers new ones as they spawn.

    The read task is not guaranteed to land on a prestarted worker: Ray may
    fork one after any one-shot census, and a fixed-pid sampler would then
    attribute the read to an idle worker. Each newly seen pid gets its
    first-seen USS as baseline; pids first seen after sampling started are
    flagged `late` (their baseline already includes whatever the read did
    before discovery, ≤0.5 s in).
    """

    def __init__(self, exclude_pids):
        super().__init__(daemon=True)
        self.exclude = set(exclude_pids)
        self.baseline = {}  # pid -> first-seen USS KiB
        self.peak = {}  # pid -> peak USS KiB
        self.titles = {}  # pid -> set of ray:: cmdlines ever observed
        self.late = set()  # pids first seen after start()
        # NOT named _stop: threading.Thread.join() calls its own internal
        # self._stop() method, which an Event attribute would shadow.
        self._halt = threading.Event()

    def census(self, late):
        for p in _proc_pids():
            if p in self.exclude:
                continue
            cmd = _cmdline(p).strip()
            if not cmd.startswith("ray::"):
                continue
            if p not in self.baseline:
                v = _uss_kb(p)
                if v is None:
                    continue
                self.baseline[p] = v
                self.peak[p] = v
                if late:
                    self.late.add(p)
            # A worker is retitled ray::<TaskName> while executing, so the
            # accumulated title set positively identifies the read worker.
            self.titles.setdefault(p, set()).add(cmd)

    def snapshot(self):
        return {p: _uss_kb(p) for p in list(self.baseline)}

    def run(self):
        tick = 0
        while not self._halt.is_set():
            if tick % _DISCOVER_EVERY_TICKS == 0:
                self.census(late=True)
            for p in list(self.peak):
                v = _uss_kb(p)
                if v is not None and v > self.peak[p]:
                    self.peak[p] = v
            tick += 1
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

    # Warm the pool, census OUR cluster's workers (new ray:: pids only), then
    # keep the sampler discovering workers forked later.
    ray.get(ray.remote(lambda: os.getpid()).remote())
    time.sleep(2)
    sampler = _WorkerSampler(before_init)
    sampler.census(late=False)
    if not sampler.baseline:
        raise RuntimeError("no worker found in the fresh local cluster")
    sampler.start()

    def one_read():
        ds = ray.data.read_parquet(path, concurrency=1)
        n = 0
        for bundle in ds.iter_internal_ref_bundles():
            n += bundle.num_rows() or 0
        return n

    rows1 = one_read()
    time.sleep(2)  # settle before the after-read floor
    after1 = sampler.snapshot()
    rows2 = one_read()
    time.sleep(2)
    sampler.census(late=True)  # catch a worker forked at the tail of read 2
    after2 = sampler.snapshot()
    sampler.stop()

    idle_uss = sampler.baseline

    # Positive ID first: any pid that ever bore the read-op proctitle. Growth
    # selection is only the fallback (a sub-0.5 s read the title poll missed):
    # a worker first seen mid-read has an inflated baseline, so its settled
    # delta reads ~0 and an idle pid's noise can win the argmax.
    matched = [
        p
        for p in idle_uss
        if any(_READ_TASK_TITLE_SUBSTR in t for t in sampler.titles.get(p, ()))
    ]

    def _delta(snap, p):
        v = snap.get(p)
        # Not yet discovered at that snapshot (or /proc read failed) = no
        # observed growth, not negative growth.
        return 0 if v is None else v - idle_uss[p]

    if matched:
        rw = max(matched, key=lambda p: after2.get(p) or 0)
        id_method = "title"
        same_worker = len(matched) == 1
    else:
        # Attribute each read separately: if read 1 and read 2 landed on
        # different pids, one pid's after1/after2 deltas would mix the reads.
        rw = max(idle_uss, key=lambda p: _delta(after2, p))
        grower1 = max(idle_uss, key=lambda p: _delta(after1, p))
        grower2 = max(idle_uss, key=lambda p: _delta(after2, p) - _delta(after1, p))
        id_method = "growth"
        same_worker = grower1 == grower2 == rw

    result = {
        "arm": arm,
        "rows_read": rows1,
        "rows_read_2": rows2,
        "worker_pid": rw,
        "idle_uss_mb": round(idle_uss[rw] / 1024, 1),
        "peak_uss_mb": round(sampler.peak[rw] / 1024, 1),
        "after_read1_uss_mb": round((after1.get(rw) or idle_uss[rw]) / 1024, 1),
        "after_read2_uss_mb": round((after2.get(rw) or idle_uss[rw]) / 1024, 1),
        "n_workers_seen": len(idle_uss),
        # "title" = pid positively bore ray::ReadFiles* while executing;
        # "growth" = fallback max-delta pick (weaker — see comment above).
        "read_worker_id_method": id_method,
        "n_read_task_workers": len(matched),
        # True = worker first seen after sampling began; its idle baseline may
        # already include read work (constant would read low).
        "read_worker_late_spawn": rw in sampler.late,
        # False = read tasks ran on more than one pid (title path) or the two
        # reads grew different pids (growth path); after1/after2 deltas on rw
        # then do NOT mean "read-1 floor / read-2 floor" for one worker.
        "reads_on_same_worker": same_worker,
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
