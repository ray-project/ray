#!/usr/bin/env python3
"""Single-box replication of the A/B #5 >1.15 regressions (non-tpch legs).

User cut 2026-08-28 (arrow_rs_docs/2026-08-27.md §11): every test where wall,
wUSS, or per-task USS R > 1.15 is a P0. This probe runs the single-node analog
of each NON-tpch P0 — the tpch queries go through tpch_probe.py, which already
takes --queries/--strategies (see run_release_regressions.sh for the pairing).

Each cell runs the RELEASE benchmark script itself (read_and_consume_benchmark
/ groupby_benchmark / join_benchmark — same code, same public bucket) in a
fresh process per (cell, reader), with the same env pins the release yaml sets
(bin-packing bytes, node-mem monitor), toggling only
RAY_DATA_USE_ARROW_RS_PARQUET_READER. TEST_OUTPUT_JSON lands per cell, so every
run carries the full per-task dists (read_max_uss_per_task_dist etc.) that the
release A/B reports — the final table is computed from those, with the same
ratio convention (R = arrow_rs / pyarrow, <1 = arrow-rs better).

What is deliberately NOT here, and why (doc §12 has the full map):
  read_large_parquet_*   s3://ray-benchmark-data-internal-* is ACCESS_DENIED
                         from our account. Analog = run_loss_triage.sh's
                         auto_rg S3 shape (same ~69 MiB-row-group geometry).
  read_parquet_*         same internal bucket (imagenet/parquet). The
                         read_parquet_binned cell below keeps the test's 64 MiB
                         bin pin but on public tpch lineitem — an approximation,
                         labeled as such.
  wide_schema_objects    internal bucket AND no local fixture for the "objects"
                         data-type (tensors fixtures don't cover it). Cannot be
                         replicated; release-only signal.
  autoscaling variants   one box has no autoscaler; each runs as its fixed-size
                         analog. A loss that lives in pool dynamics (T27) will
                         NOT show here — that absence is itself the signal.

Downsizing: release tpch data is sf1000 (write_parquet) / sf100 (joins,
map_groups); one box gets sf100 / sf10 defaults, overridable per family. The
regime caveat from M35 stands: short fresh-session runs sit below the
allocator-churn floor, so a clean table here does NOT clear the retention
cluster — it separates "reproduces anywhere" from "needs the release regime".

Usage:
  python release_regression_probe.py --outdir DIR [--repeat 1]
      [--only write_parquet,mapg_hash] [--write-sf 100] [--groupby-sf 10]
      [--joins-sf 10] [--join-types right_outer] [--dry-run]
Needs AWS creds; ARROW_RS_S3_BUCKET redirects write_parquet's output (the
release write bucket s3://ray-data-write-benchmark may be unwritable to us).
"""
import argparse
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.abspath(os.path.join(HERE, ".."))
TPCH = "s3://ray-benchmark-data/tpch/parquet"

# One cell, fresh process: import the release script as a module, patch its
# write root if asked, run its own parse_args()+main() under our sys.argv.
SNIPPET = r"""
import importlib, json, re, sys, time

mod_name, argv_json, write_root, dry = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
mod = importlib.import_module(mod_name)
if dry:
    print("CELL_JSON " + json.dumps({"dry_run": True}))
    raise SystemExit(0)
if write_root and hasattr(mod, "WRITE_PATH"):
    mod.WRITE_PATH = write_root
sys.argv = [mod_name] + json.loads(argv_json)
import ray
ray.init(address="local")
t0 = time.monotonic()
mod.main(mod.parse_args())
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


def cells(a):
    """(name, module, argv, extra_env) per non-tpch P0. Env pins mirror
    release_data_tests.yaml for the corresponding test."""
    out = [
        # Exact release replica — same script, same sf10 public data.
        # P0 trip: tUSS max 1.18 (p50 0.93 win).
        (
            "iter_batches_pyarrow",
            "read_and_consume_benchmark",
            [
                f"{TPCH}/sf10/lineitem",
                "--format",
                "parquet",
                "--iter-batches",
                "pyarrow",
            ],
            {},
        ),
        # Release reads sf1000 lineitem; sf100 here. Same 1.25 GiB bin pin
        # (~one file per bin). P0 trip: wUSS 1.38, tUSS 1.26/max 1.63 (M38/M74).
        (
            "write_parquet",
            "read_and_consume_benchmark",
            [f"{TPCH}/sf{a.write_sf}/lineitem", "--format", "parquet", "--write"],
            {"RAY_DATA_PARQUET_BIN_PACKING_BYTES": "1342177280"},
        ),
        # APPROXIMATION of read_parquet_autoscaling (wall 1.28): release data is
        # the internal imagenet bucket (denied); keeps the 64 MiB bin pin on
        # public lineitem so the many-small-tasks geometry survives.
        (
            "read_parquet_binned",
            "read_and_consume_benchmark",
            [
                f"{TPCH}/sf{a.write_sf}/lineitem",
                "--format",
                "parquet",
                "--iter-bundles",
            ],
            {"RAY_DATA_PARQUET_BIN_PACKING_BYTES": "67108864"},
        ),
    ]
    # map_groups_autoscaling_{hash,sort}_column02+column14: wall 1.16 / 1.20,
    # T spills more than B (53.6 vs 40.8 GB) — release is sf100 multi-node.
    for tag, strat in [
        ("mapg_hash", "hash_shuffle"),
        ("mapg_sort", "sort_shuffle_pull_based"),
    ]:
        out.append(
            (
                f"{tag}_col02+col14",
                "groupby_benchmark",
                [
                    "--sf",
                    a.groupby_sf,
                    "--map-groups",
                    "--group-by",
                    "column02",
                    "column14",
                    "--shuffle-strategy",
                    strat,
                ],
                {},
            )
        )
    # joins_sf100_right_outer wall 1.58 (first occurrence); inner/left/full ride
    # along on request for the sustained-wUSS addendum (all three sat 1.27-1.35).
    for jt in a.join_types.split(","):
        out.append(
            (
                f"joins_{jt}",
                "join_benchmark",
                [
                    "--left_dataset",
                    f"{TPCH}/sf{a.joins_sf}/lineitem",
                    "--right_dataset",
                    f"{TPCH}/sf{a.joins_sf}/orders",
                    "--left_join_keys",
                    "column00",
                    "--right_join_keys",
                    "column0",
                    "--join_type",
                    jt,
                    "--num_partitions",
                    "50",
                ],
                {},
            )
        )
    return out


def run_cell(name, module, argv, extra_env, reader, a):
    tag = f"{name}.{reader}"
    env = dict(os.environ)
    env["PYTHONPATH"] = DATASET_DIR + os.pathsep + env.get("PYTHONPATH", "")
    env["RAY_DATA_USE_ARROW_RS_PARQUET_READER"] = "1" if reader == "rs" else "0"
    env["RAY_DATA_BENCH_NODE_MEM_MONITOR"] = "1"
    env["TEST_OUTPUT_JSON"] = os.path.join(a.outdir, f"{tag}.benchmark.json")
    env.update(extra_env)
    write_root = ""
    if module == "read_and_consume_benchmark" and "--write" in argv:
        bucket = os.environ.get("ARROW_RS_S3_BUCKET", "")
        if bucket:
            write_root = f"{bucket.rstrip('/')}/regression_probe/{tag}"
    cmd = [
        sys.executable,
        "-c",
        SNIPPET,
        module,
        json.dumps(argv),
        write_root,
        "1" if a.dry_run else "0",
    ]
    log_path = os.path.join(a.outdir, f"{tag}.log")
    if not a.dry_run:
        print(f"    -> {tag} running (tail -f {log_path})", flush=True)
    t0 = time.perf_counter()
    with open(log_path, "w") as fh:
        fh.write(f"# {module} {' '.join(argv)} reader={reader}\n")
        fh.flush()
        proc = subprocess.run(cmd, env=env, stdout=fh, stderr=subprocess.STDOUT)
    with open(log_path) as fh:
        out = fh.read()
    line = next((ln for ln in out.splitlines() if ln.startswith("CELL_JSON ")), None)
    if line is None:
        print(f"    !! {tag} FAILED rc={proc.returncode} (see {tag}.log)", flush=True)
        print("       " + out.strip()[-400:], flush=True)
        return None
    rec = json.loads(line[len("CELL_JSON ") :])
    # Fold in the benchmark's own metrics (per-task dists, node-mem monitor).
    try:
        with open(env["TEST_OUTPUT_JSON"]) as fh:
            bench = json.load(fh)
        rec["bench"] = next(iter(bench.values()))
    except Exception:
        rec["bench"] = {}
    rec["wall_incl_startup_s"] = round(time.perf_counter() - t0, 1)
    print(
        f"    {tag:<34} wall={rec.get('wall_s')}s spill={rec.get('spilled_gb')}GB",
        flush=True,
    )
    return rec


def _g(rec, *path):
    cur = rec or {}
    for p in path:
        cur = cur.get(p) if isinstance(cur, dict) else None
        if cur is None:
            return None
    return cur


def _r(rs, pa):
    return f"{rs / pa:.2f}" if rs and pa else "—"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--outdir", required=True)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--only", default="", help="comma-separated cell-name filter")
    p.add_argument(
        "--write-sf", default="100", help="sf for write/read cells (release: 1000)"
    )
    p.add_argument(
        "--groupby-sf", default="10", help="sf for map_groups (release: 100)"
    )
    p.add_argument("--joins-sf", default="10", help="sf for joins (release: 100)")
    p.add_argument(
        "--join-types",
        default="right_outer",
        help="right_outer, or all four for the sustained-wUSS addendum",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="import-and-exit per cell: validates plumbing offline",
    )
    a = p.parse_args()
    os.makedirs(a.outdir, exist_ok=True)

    todo = cells(a)
    if a.only:
        keep = {s.strip() for s in a.only.split(",")}
        todo = [c for c in todo if any(k in c[0] for k in keep)]

    results = {}
    for name, module, argv, extra_env in todo:
        for reader in ("pa", "rs"):
            runs = [
                run_cell(name, module, argv, extra_env, reader, a)
                for _ in range(a.repeat)
            ]
            good = sorted(
                (r for r in runs if r and r.get("wall_s")), key=lambda r: r["wall_s"]
            )
            results[f"{name}.{reader}"] = (
                good[len(good) // 2] if good else (runs[0] if runs else None)
            )

    with open(os.path.join(a.outdir, "summary.json"), "w") as fh:
        json.dump(results, fh, indent=2)
    if a.dry_run:
        print("\ndry run OK — all release script modules import")
        return

    # Same columns as arrow_rs_docs/2026-08-27.md §12: the numbers the P0 cut
    # was made on, computed from each cell's own TEST_OUTPUT_JSON.
    print("\n========== RELEASE-REGRESSION PROBE (R = arrow_rs/pyarrow) ==========")
    hdr = (
        f"{'cell':<24} {'wall R':>7} {'tUSS p50 R':>10} {'tUSS max R':>10} "
        f"{'wUSS pk R':>9} {'wUSS sust R':>11} {'pkbatch R':>9} {'n tasks':>8} "
        f"{'spill pa/rs':>12}"
    )
    print(hdr)
    for name, _, _, _ in todo:
        pa = results.get(f"{name}.pa") or {}
        rs = results.get(f"{name}.rs") or {}
        bp, br = pa.get("bench", {}), rs.get("bench", {})
        row = (
            f"{name:<24} "
            f"{_r(rs.get('wall_s'), pa.get('wall_s')):>7} "
            f"{_r(_g(br, 'read_max_uss_per_task_dist', 'p50'), _g(bp, 'read_max_uss_per_task_dist', 'p50')):>10} "
            f"{_r(_g(br, 'read_max_uss_per_task_dist', 'max'), _g(bp, 'read_max_uss_per_task_dist', 'max')):>10} "
            f"{_r(_g(br, 'node_mem_peak_worker_uss_gb'), _g(bp, 'node_mem_peak_worker_uss_gb')):>9} "
            f"{_r(_g(br, 'node_mem_p50_worker_uss_gb'), _g(bp, 'node_mem_p50_worker_uss_gb')):>11} "
            f"{_r(_g(br, 'read_peak_batch_bytes_per_task_dist', 'p50'), _g(bp, 'read_peak_batch_bytes_per_task_dist', 'p50')):>9} "
            f"{_g(br, 'read_max_uss_per_task_dist', 'num_samples') or '—':>8} "
            f"{str(pa.get('spilled_gb')) + '/' + str(rs.get('spilled_gb')):>12}"
        )
        print(row)
    print(
        "\nRead it as: a cell that reproduces its release ratio here is debuggable"
        "\non this box; a clean cell pushes that P0 into the release regime"
        "\n(autoscaling pool dynamics / allocator churn floor — M35, items 18/19)."
    )


if __name__ == "__main__":
    main()
