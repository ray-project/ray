#!/usr/bin/env python3
"""Soak/churn discriminator for the release-regime retention losses (M37/M38).

Every box replication so far (replication_matrix, loss_triage - M34/M35) drove
each cell with a FRESH Ray session and ~tens of tasks per worker, and none of
the release losses reproduced there. A/B #4 (2026-08-18.md) then showed the
losses are real at a 20 Hz poll with byte-identical decoder work (M37:
read_large_parquet_autoscaling task-USS 1.42/1.62; M38: write_parquet
1.26/1.74) - so the one release variable no box run has ever reproduced is
WORKER LIFETIME x TASK COUNT: release workers process hundreds-to-thousands of
high-churn tasks over minutes (940-5769 tasks/test), and the USS signature in
every loss is a resident FLOOR that climbs across a worker's task sequence
(arrow-rs min-task USS BELOW baseline's min, everything past p25 above it).

This probe holds ONE long-lived local Ray session per arm with a pinned worker
pool (num_cpus=W) and pushes R rounds of the same read (or fused read->write)
through it, so each worker executes hundreds of tasks, then reads the floor:

  - a per-PID USS time series (2 Hz, raylet-descendant workers) written to
    series.jsonl - the climb curve itself;
  - an idle-floor snapshot after each round (settle + gc) - floor(round);
  - Ray's own per-task USS (20 Hz in-task poll) per round - the avg/max trend.

Arms (the allocator ablation): pa / rs / rs_arena2 (MALLOC_ARENA_MAX=2) /
rs_trim (MALLOC_TRIM_THRESHOLD_=0, eager top-of-heap trim on free) /
rs_jemalloc (LD_PRELOAD system libjemalloc - routes the crate's glibc
allocations through the same allocator family PyArrow bundles; skipped with a
warning if no libjemalloc.so is found - `apt install libjemalloc2`). Every env
lever is a deployable fix candidate on its own. Read the verdict as:

  rs floor climbs round-over-round while pa stays flat => the retention is
      real and single-node-reproducible (the release "multi-node" difference
      was worker lifetime all along);
  ...and rs_arena2 collapses to pa   => glibc ARENA retention; fixes: arena
      cap via runtime_env, or crate-side malloc_trim(0) at end-of-stream
      (glibc-only, cfg(target_os="linux"));
  ...and only rs_trim collapses      => retention at top-of-heap, few arenas
      involved; fix = trim (env or crate call);
  ...and only rs_jemalloc collapses  => glibc-vs-jemalloc policy generally
      (fragmentation across arenas trim can't reach); fix = LD_PRELOAD
      jemalloc in the workers' runtime_env (cheap, no crate change);
  ...and NONE of the rs_* arms collapse => not the C allocator - Rust-side
      caching or genuine fragmentation; back to crate profiling;
  rs floor flat like pa even at 100s of tasks/worker   => the losses need
      something only the release cluster has (autoscaling node churn, plasma
      pressure, genuine multi-node) - escalate to TODO item 18's both-arms
      release trigger.

Shapes (fixtures + release-yaml bins exactly as loss_triage.py):

  auto   M37 read_large_parquet_autoscaling: one ~69 MiB row group per task
         (bin 64 MiB), sub-second tasks - the many-small-tasks regime.
  write  M38 write_parquet: fused read->write_parquet, ~1.2 GiB decode churn
         per task (bin 1342177280 on the bin_sweep fixture).

Usage (Linux box; venv + fixtures via run_soak.sh, or piecemeal):

  python gen_local_fixtures.py --root ~/arrow_rs_repl_fixtures \
      --shapes auto_rg,bin_sweep
  python soak_probe.py --fixture-root ~/arrow_rs_repl_fixtures
  python soak_probe.py --fixture-root ... --shapes auto --rounds 4 \
      --path-repeat 4 --workers 4

Results: <outdir>/summary.json + per-cell logs + per-cell series.jsonl
(rows of {t, pid, uss_mib} - plot to SEE the climb). The printed table is
R = arrow_rs / pyarrow on the end-of-run idle floor; >1.00 = arrow-rs worse.
"""
import argparse
import gc
import glob
import json
import os
import shutil
import subprocess
import sys
import threading
import time

PY = sys.executable
HERE = os.path.dirname(os.path.abspath(__file__))
MiB = 1024 * 1024

# Release-yaml bin sizes per shape (release/release_data_tests.yaml), same as
# loss_triage.py.
SHAPE_BINS = {
    "auto": 67_108_864,
    "write": 1_342_177_280,
}
SHAPE_FIXTURE = {
    "auto": "auto_rg",
    "write": "bin_sweep",
}
# rounds x path_repeat sized so tasks/worker lands in the release regime
# (O(100) and up for auto; write churns ~1.2 GiB per task so fewer tasks carry
# the same churn volume per worker).
SHAPE_DEFAULTS = {
    "auto": dict(rounds=6, path_repeat=8),
    # v2 bin_sweep is ~4.6 GiB decoded = ~3.5 write bins per pass; x12 gives
    # ~42 tasks/round -> ~60+ tasks/worker over 6 rounds at 1.29 GiB churn each.
    "write": dict(rounds=6, path_repeat=12),
}


def _median(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


class SeriesSampler:
    """Per-PID USS/RSS time series over OUR raylet's descendant workers.

    read_probe.WorkerMemSampler keeps only the summed peak; here the ORDER is
    the signal (the floor climb across a worker's task sequence), so every
    sample row is kept and written to series.jsonl on exit. USS needs Linux
    (memory_full_info); on macOS rows fall back to RSS and say so.
    """

    def __init__(self, interval_s, root_pid, out_path):
        self._interval_s = interval_s
        self._root_pid = root_pid
        self._out_path = out_path
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self._rows = []
        self._t0 = time.monotonic()
        self._uss_ok = True
        self.peak_uss = 0
        self.peak_rss = 0

    def _procs(self):
        import psutil

        if self._root_pid is None:
            return []
        try:
            children = psutil.Process(self._root_pid).children(recursive=True)
        except psutil.NoSuchProcess:
            return []
        # The raylet also parents agents (dashboard_agent, runtime_env_agent);
        # keep only task workers: proctitle "ray::<task>" / "ray::IDLE", or the
        # unretitled default_worker.py. The smoke run showed an agent pid
        # polluting the floor median at >1 GiB.
        out = []
        for proc in children:
            try:
                cmd = " ".join(proc.cmdline() or [])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            if "ray::" in cmd or "default_worker.py" in cmd:
                out.append(proc)
        return out

    def _sample(self, label=None):
        import psutil

        t = round(time.monotonic() - self._t0, 2)
        snap = {}
        sum_uss = sum_rss = 0
        for proc in self._procs():
            try:
                if self._uss_ok:
                    try:
                        mi = proc.memory_full_info()
                        uss = getattr(mi, "uss", 0)
                        rss = mi.rss
                    except (psutil.AccessDenied, NotImplementedError):
                        self._uss_ok = False
                        uss, rss = 0, proc.memory_info().rss
                else:
                    uss, rss = 0, proc.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            row = {"t": t, "pid": proc.pid, "rss_mib": round(rss / MiB, 1)}
            if self._uss_ok:
                row["uss_mib"] = round(uss / MiB, 1)
            if label:
                row["label"] = label
            with self._lock:
                self._rows.append(row)
            snap[proc.pid] = round((uss if self._uss_ok else rss) / MiB, 1)
            sum_uss += uss
            sum_rss += rss
        self.peak_uss = max(self.peak_uss, sum_uss)
        self.peak_rss = max(self.peak_rss, sum_rss)
        return snap

    def snapshot(self, label):
        """One labeled sample right now; returns {pid: uss_mib} (rss on mac)."""
        return self._sample(label=label)

    def _run(self):
        while not self._stop.wait(self._interval_s):
            self._sample()

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread:
            self._thread.join()
        with self._lock, open(self._out_path, "w") as fh:
            for row in self._rows:
                fh.write(json.dumps(row) + "\n")


# --------------------------------------------------------------------------
# The case: one arm = one fresh process = ONE long-lived Ray session.
# --------------------------------------------------------------------------


def run_case(a):
    import ray
    from ray.data.context import DataContext

    from read_probe import collect_read_op_metrics

    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    ctx.use_arrow_rs_parquet_reader = a.reader == "rs"
    # The release instrument: 20 Hz per-task USS poll (A/B #4 parity).
    ctx.memory_usage_poll_interval_s = 0.05

    # num_cpus pins the worker pool: W workers live for the WHOLE session, so
    # tasks/worker = rounds * tasks_per_round / W - the release regime the
    # fresh-session cells could never reach.
    ray.init(num_cpus=a.workers, ignore_reinit_error=True)
    try:
        gcs = ray.get_runtime_context().gcs_address
        if gcs:
            os.environ["RAY_ADDRESS"] = gcs  # read_probe's stats-resolution fix
    except Exception:
        pass
    root_pid = None
    try:
        node = ray._private.worker._global_node  # noqa: SLF001
        for name, procs in (node.all_processes or {}).items():
            if "raylet" in name.lower() and procs:
                root_pid = procs[0].process.pid
                break
    except Exception:
        pass

    if a.path.startswith("s3://"):
        # The S3 leg (user, 2026-08-21: does the M38 release write loss need
        # S3 transport?). aws-cli is the lister so the fixture needs no
        # local mirror on the driver.
        ls = subprocess.run(
            ["aws", "s3", "ls", a.path.rstrip("/") + "/"],
            capture_output=True,
            text=True,
        ).stdout
        files = sorted(
            a.path.rstrip("/") + "/" + ln.split()[-1]
            for ln in ls.splitlines()
            if ln.strip().endswith(".parquet")
        )
    else:
        files = sorted(glob.glob(os.path.join(os.path.expanduser(a.path), "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet files under {a.path}")
    paths = files * a.path_repeat
    write_out = a.write_out or os.path.join(a.workdir, f"soak_write_out_{a.tag}")

    def clean_write_out():
        if write_out.startswith("s3://"):
            subprocess.run(
                ["aws", "s3", "rm", "--recursive", "--quiet", write_out],
                capture_output=True,
            )
        else:
            shutil.rmtree(write_out, ignore_errors=True)

    series_path = os.path.join(a.workdir, f"{a.tag}.series.jsonl")

    rounds = []
    tasks_total = 0
    sampler = SeriesSampler(a.sample_s, root_pid, series_path)
    try:
        with sampler:
            start_floor = sampler.snapshot("start")
            for rnd in range(a.rounds):
                t0 = time.perf_counter()
                ds = ray.data.read_parquet(paths)
                if a.shape == "write":
                    clean_write_out()
                    ds.write_parquet(write_out)
                    clean_write_out()
                else:
                    # capture_executor=True so per-task USS survives (read_probe's
                    # GE1 snapshot-race fix); bundles are dropped as they stream.
                    bundle_iter, _, _ = ds._execute_to_iterator(capture_executor=True)
                    for _ in bundle_iter:
                        pass
                wall = time.perf_counter() - t0
                m = collect_read_op_metrics(ds)
                m.pop("uss_debug", None)
                del ds
                gc.collect()
                time.sleep(a.settle_s)  # let workers go idle before reading floors
                floor = sampler.snapshot(f"after_round_{rnd}")
                ntasks = m.get("read_num_tasks") or 0
                tasks_total += ntasks
                if rnd == 0 and ntasks < 2 * a.workers:
                    print(
                        f"  !! only {ntasks} read tasks/round for {a.workers} "
                        "workers - this is NOT soaking. Check the fixture is v2 "
                        "(gen_local_fixtures M41 fix) and raise --path-repeat.",
                        flush=True,
                    )
                to_mib = lambda gb: round(gb * 1024, 1) if gb else None  # noqa: E731
                rounds.append(
                    dict(
                        round=rnd,
                        wall_s=round(wall, 2),
                        num_tasks=ntasks,
                        task_uss_avg_mib=to_mib(m.get("read_avg_max_uss_gb")),
                        task_uss_max_mib=to_mib(m.get("read_max_uss_gb")),
                        idle_floor_mib=_median(list(floor.values())),
                        workers_live=len(floor),
                        idle_floor_by_pid={str(k): v for k, v in sorted(floor.items())},
                    )
                )
                print(f"  round {rnd}: {rounds[-1]}", flush=True)
    finally:
        clean_write_out()
        import ray as _ray

        _ray.shutdown()

    floors = [r["idle_floor_mib"] for r in rounds if r["idle_floor_mib"] is not None]
    stable_pids = None
    if len(rounds) > 1:
        first = set(rounds[0]["idle_floor_by_pid"])
        last = set(rounds[-1]["idle_floor_by_pid"])
        stable_pids = len(first & last)
    result = dict(
        shape=a.shape,
        reader=a.reader,
        workers=a.workers,
        rounds=a.rounds,
        tasks_total=tasks_total,
        tasks_per_worker=round(tasks_total / a.workers, 1) if a.workers else None,
        stable_worker_pids=stable_pids,
        start_floor_mib=_median(list(start_floor.values())),
        first_round_floor_mib=floors[0] if floors else None,
        end_floor_mib=floors[-1] if floors else None,
        floor_climb_mib=(round(floors[-1] - floors[0], 1) if len(floors) > 1 else None),
        peak_uss_gb=round(sampler.peak_uss / 1024**3, 3)
        if sampler.peak_uss
        else None,
        peak_rss_gb=round(sampler.peak_rss / 1024**3, 3),
        task_uss_avg_first_mib=rounds[0]["task_uss_avg_mib"] if rounds else None,
        task_uss_avg_last_mib=rounds[-1]["task_uss_avg_mib"] if rounds else None,
        task_uss_max_last_mib=rounds[-1]["task_uss_max_mib"] if rounds else None,
        rounds_detail=rounds,
        series_jsonl=series_path,
    )
    print("=== CASE RESULT ===")
    print(json.dumps(result))


# --------------------------------------------------------------------------
# The matrix: shapes x arms, one subprocess (= one long session) per cell.
# --------------------------------------------------------------------------


def run_cell(logdir, name, shape, reader, path, env_extra, a, write_out=None):
    defaults = SHAPE_DEFAULTS[shape]
    cmd = [
        PY,
        os.path.abspath(__file__),
        "case",
        "--shape",
        shape,
        "--reader",
        reader,
        "--path",
        path,
        "--workers",
        str(a.workers),
        "--rounds",
        str(a.rounds or defaults["rounds"]),
        "--path-repeat",
        str(a.path_repeat or defaults["path_repeat"]),
        "--settle-s",
        str(a.settle_s),
        "--sample-s",
        str(a.sample_s),
        "--workdir",
        logdir,
        "--tag",
        name,
    ]
    if write_out:
        cmd += ["--write-out", write_out]
    env = dict(os.environ)
    env["RAY_DATA_PARQUET_BIN_PACKING_BYTES"] = str(SHAPE_BINS[shape])
    env.update(env_extra)
    print(f"  -> {name} (env_extra={env_extra})", flush=True)
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    with open(os.path.join(logdir, f"{name}.log"), "w") as fh:
        fh.write(f"# cmd: {' '.join(cmd)}\n# env_extra: {env_extra}\n")
        fh.write(f"# wall_including_startup_s: {time.perf_counter() - t0:.1f}\n")
        fh.write("# ---- STDOUT ----\n" + proc.stdout)
        fh.write("\n# ---- STDERR ----\n" + proc.stderr)
    for i, line in enumerate(proc.stdout.splitlines()):
        if "=== CASE RESULT ===" in line:
            try:
                res = json.loads(proc.stdout.splitlines()[i + 1])
                print(
                    f"     floor {res.get('first_round_floor_mib')} -> "
                    f"{res.get('end_floor_mib')} MiB (climb "
                    f"{res.get('floor_climb_mib')}), "
                    f"{res.get('tasks_per_worker')} tasks/worker",
                    flush=True,
                )
                return res
            except (IndexError, json.JSONDecodeError):
                break
    print(
        f"    !! {name} CASE FAIL rc={proc.returncode} (see {logdir}/{name}.log)\n"
        f"       {proc.stderr.strip()[-400:]}",
        flush=True,
    )
    return {}


def _find_jemalloc():
    """Locate a system libjemalloc for the rs_jemalloc LD_PRELOAD arm.

    JEMALLOC_PATH env overrides; otherwise probe the usual Linux locations.
    Returns None (arm skipped) when not found or on macOS (LD_PRELOAD n/a).
    """
    if not sys.platform.startswith("linux"):
        return None
    cand = os.environ.get("JEMALLOC_PATH")
    if cand and os.path.exists(cand):
        return cand
    import glob as _glob

    for pat in (
        "/usr/lib/x86_64-linux-gnu/libjemalloc.so*",
        "/usr/lib/aarch64-linux-gnu/libjemalloc.so*",
        "/usr/lib64/libjemalloc.so*",
        "/usr/local/lib/libjemalloc.so*",
    ):
        hits = sorted(_glob.glob(pat))
        if hits:
            return hits[0]
    return None


def _ratio(a, b):
    return round(a / b, 2) if a and b else None


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("case", help="internal: one arm, one long Ray session")
    c.add_argument("--shape", choices=list(SHAPE_BINS), required=True)
    c.add_argument("--reader", choices=["pa", "rs"], required=True)
    c.add_argument("--path", required=True)
    c.add_argument("--workers", type=int, default=4)
    c.add_argument("--rounds", type=int, default=6)
    c.add_argument("--path-repeat", type=int, default=8)
    c.add_argument("--settle-s", type=float, default=3.0)
    c.add_argument("--sample-s", type=float, default=0.5)
    c.add_argument("--workdir", default=".")
    c.add_argument("--tag", default="case")
    c.add_argument(
        "--write-out",
        default=None,
        help="write-shape output root (s3://... allowed); default <workdir>/...",
    )

    p.add_argument("--fixture-root", default=None)
    p.add_argument("--outdir", default=None)
    p.add_argument("--shapes", default="auto,write")
    p.add_argument("--arms", default="pa,rs,rs_arena2,rs_trim,rs_jemalloc")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument(
        "--rounds", type=int, default=None, help="override per-shape default"
    )
    p.add_argument(
        "--path-repeat", type=int, default=None, help="override per-shape default"
    )
    p.add_argument("--settle-s", type=float, default=3.0)
    p.add_argument("--sample-s", type=float, default=0.5)
    p.add_argument(
        "--transports",
        default="local",
        help="comma list of local,s3 — s3 soaks read from AND write to the bucket",
    )
    p.add_argument("--s3-bucket", default=os.environ.get("ARROW_RS_S3_BUCKET"))
    args = p.parse_args()

    if args.cmd == "case":
        run_case(args)
        return

    if not args.fixture_root:
        p.error("--fixture-root is required (see gen_local_fixtures.py)")
    fixture_root = os.path.expanduser(args.fixture_root)
    with open(os.path.join(fixture_root, "manifest.json")) as fh:
        manifest = json.load(fh)

    shapes = [s.strip() for s in args.shapes.split(",") if s.strip()]
    arm_defs = {
        "pa": ("pa", {}),
        "rs": ("rs", {}),
        "rs_arena2": ("rs", {"MALLOC_ARENA_MAX": "2"}),
        # Eager glibc trim: return top-of-heap to the OS on every free that
        # leaves >=0 bytes free above the break (also freezes the dynamic
        # mmap/trim threshold adjustment - fine for an ablation arm).
        "rs_trim": ("rs", {"MALLOC_TRIM_THRESHOLD_": "0"}),
    }
    jemalloc = _find_jemalloc()
    if jemalloc:
        arm_defs["rs_jemalloc"] = ("rs", {"LD_PRELOAD": jemalloc})
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    if "rs_jemalloc" in arms and not jemalloc:
        print(
            "WARNING: rs_jemalloc arm skipped - no libjemalloc found "
            "(set JEMALLOC_PATH or `apt install libjemalloc2`)",
            flush=True,
        )
        arms = [a for a in arms if a != "rs_jemalloc"]

    outdir = args.outdir or os.path.join(
        HERE, "soak_runs", time.strftime("%Y%m%d_%H%M%S")
    )
    os.makedirs(outdir, exist_ok=True)
    print(f"shapes={shapes} arms={arms} workers={args.workers} outdir={outdir}")

    transports = [t.strip() for t in args.transports.split(",") if t.strip()]
    if "s3" in transports and not args.s3_bucket:
        p.error("--transports s3 needs --s3-bucket or ARROW_RS_S3_BUCKET")

    summary = {"shapes": shapes, "arms": arms, "workers": args.workers, "cells": {}}
    for shape in shapes:
        entry = manifest[SHAPE_FIXTURE[shape]]
        local_path = entry["path"] if isinstance(entry, dict) else entry
        for transport in transports:
            if transport == "s3":
                from loss_triage import s3_sync

                bucket = args.s3_bucket.rstrip("/")
                path = f"{bucket}/soak/{SHAPE_FIXTURE[shape]}"
                s3_sync(local_path, path)
                write_out = f"{bucket}/soak_write_out/{shape}"
            else:
                path, write_out = local_path, None
            print(
                f"\n=== [{shape}] soak/{transport} ({SHAPE_FIXTURE[shape]}) ===",
                flush=True,
            )
            cells = {}
            for tag in arms:
                reader, env_extra = arm_defs[tag]
                cells[tag] = run_cell(
                    outdir,
                    f"{shape}.{transport}.{tag}",
                    shape,
                    reader,
                    path,
                    env_extra,
                    args,
                    write_out=write_out,
                )
            summary["cells"][f"{shape}.{transport}"] = cells

    print("\n\n============ SOAK SUMMARY (R = arrow_rs/pyarrow) ============")
    if sys.platform == "darwin":
        print("(macOS: USS unavailable - rows are RSS; smoke run only)")
    header = (
        f"{'cell':<20} {'end floor MiB':>14} {'climb MiB':>10} "
        f"{'tUSS avg last':>14} {'floor R':>8}"
    )
    print(header)
    print("-" * len(header))
    for shape, cells in summary["cells"].items():
        pa = cells.get("pa") or {}
        for tag in arms:
            r = cells.get(tag) or {}
            floor_r = (
                _ratio(r.get("end_floor_mib"), pa.get("end_floor_mib"))
                if tag != "pa"
                else None
            )
            fmt = lambda v: f"{v}" if v is not None else "-"  # noqa: E731
            print(
                f"{shape + '.' + tag:<20} {fmt(r.get('end_floor_mib')):>14} "
                f"{fmt(r.get('floor_climb_mib')):>10} "
                f"{fmt(r.get('task_uss_avg_last_mib')):>14} "
                f"{fmt(floor_r):>8}"
            )
    print(
        "\nRead it as: rs floor climbs while pa stays flat => retention reproduced\n"
        "single-node (worker lifetime was the release variable). Whichever rs_* arm\n"
        "collapses floor R to ~pa names the mechanism AND the fix (arena cap / trim /\n"
        "LD_PRELOAD jemalloc - see the module docstring verdict table);\n"
        "rs flat like pa => release-cluster-only (escalate to TODO item 18).\n"
        "Climb curves: plot each cell's series.jsonl. Full metrics: "
        + os.path.join(outdir, "summary.json")
    )
    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)


if __name__ == "__main__":
    main()
