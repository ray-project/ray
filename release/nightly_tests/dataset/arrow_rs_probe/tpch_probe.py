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
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.abspath(os.path.join(HERE, ".."))
TPCH_DIR = os.path.join(DATASET_DIR, "tpch")

# One cell, fresh process: import the release query module, time main(), then
# read this session's spill total. ray.init happens here (the scripts only
# init under __main__).
SNIPPET = r"""
import importlib, json, re, sys, time
from types import SimpleNamespace

query, sf, dry = sys.argv[1], int(sys.argv[2]), sys.argv[3] == "1"
mod = importlib.import_module(query)
if dry:
    print("CELL_JSON " + json.dumps({"dry_run": True}))
    raise SystemExit(0)
import ray
ray.init(address="local")
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


def run_cell(query, strategy, reader, sf, outdir, dry_run):
    tag = f"{query}.{strategy}.{reader}"
    env = dict(os.environ)
    env["PYTHONPATH"] = (
        TPCH_DIR + os.pathsep + DATASET_DIR + os.pathsep + env.get("PYTHONPATH", "")
    )
    env["RAY_DATA_DEFAULT_SHUFFLE_STRATEGY"] = strategy
    env["RAY_DATA_USE_ARROW_RS_PARQUET_READER"] = "1" if reader == "rs" else "0"
    env["TEST_OUTPUT_JSON"] = os.path.join(outdir, f"{tag}.benchmark.json")
    cmd = [sys.executable, "-c", SNIPPET, query, str(sf), "1" if dry_run else "0"]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    with open(os.path.join(outdir, f"{tag}.log"), "w") as fh:
        fh.write(f"# strategy={strategy} reader={reader} sf={sf}\n")
        fh.write(proc.stdout + "\n# ---- STDERR ----\n" + proc.stderr)
    line = next(
        (ln for ln in proc.stdout.splitlines() if ln.startswith("CELL_JSON ")), None
    )
    if line is None:
        print(f"    !! {tag} FAILED rc={proc.returncode} (see {tag}.log)", flush=True)
        print("       " + proc.stderr.strip()[-400:], flush=True)
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
        "--dry-run",
        action="store_true",
        help="import-and-exit per cell: validates module/env plumbing offline",
    )
    a = p.parse_args()
    os.makedirs(a.outdir, exist_ok=True)

    results = {}
    for query in a.queries.split(","):
        for strategy in a.strategies.split(","):
            for reader in ("pa", "rs"):
                runs = [
                    run_cell(query, strategy, reader, a.sf, a.outdir, a.dry_run)
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

    print("\n================ TPCH PROBE (R = arrow_rs/pyarrow) ================")
    print(f"{'cell':<34} {'wall pa':>8} {'wall rs':>8} {'R':>6} {'spill pa/rs GB':>15}")
    for query in a.queries.split(","):
        for strategy in a.strategies.split(","):
            pa_r = results.get(f"{query}.{strategy}.pa") or {}
            rs_r = results.get(f"{query}.{strategy}.rs") or {}
            wp, wr = pa_r.get("wall_s"), rs_r.get("wall_s")
            ratio = f"{wr / wp:.2f}" if wp and wr else "—"
            spill = f"{pa_r.get('spilled_gb')}/{rs_r.get('spilled_gb')}"
            print(
                f"{query + '.' + strategy:<34} {wp or '—':>8} {wr or '—':>8} {ratio:>6} {spill:>15}"
            )
    print(
        "\nRead it as: q20 gap only under hash_shuffle_v2+rs => block-granularity"
        "\nsuspect confirmed locally (M46); no gap => release-regime-only, fold into"
        "\nitems 19/20. Any rs-only spill on q9 = T27 reproduced."
    )


if __name__ == "__main__":
    main()
