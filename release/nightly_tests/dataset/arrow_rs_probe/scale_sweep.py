#!/usr/bin/env python3
"""Scale sweep: is the arrow-rs regression an issue of SCALE?

Generates local parquet fixtures at increasing scale for the two worse-in-release
layouts and runs BOTH readers via read_probe.py at --concurrency 1. Use this to
separate a scaling effect from an environment (S3/Linux) effect.

  imagenet : scale total bytes, tiny row groups fixed. If arrow_rs stays faster as
             data grows, the release time gap is pure S3 network (not scale). On the
             Mac it stayed 0.65-0.82x of pyarrow at 0.4-2.6 GB -> confirmed network.
  wide     : scale ROW-GROUP SIZE (the per-rg decode transient). On the Mac the
             arrow_rs/pyarrow RSS ratio climbed 0.67x -> 1.04x as rg grew from 200 to
             4000 rows -> the mem regression's lever is row-group size, not row count.
             On Linux read `peak_uss_gb` (populated there) for the real magnitude.

Run: python scale_sweep.py <case>   where case in {imagenet, wide}
Fixtures land under $SWEEP_DIR (default /tmp/arrow_rs_sweep); each run prints a table.
"""
import json
import os
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

SWEEP = os.environ.get("SWEEP_DIR", "/tmp/arrow_rs_sweep")
PROBE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "read_probe.py")
PY = sys.executable
os.makedirs(SWEEP, exist_ok=True)


def gen_imagenet(tag, n_rows, blob_kb=48, rg=32, n_files=4):
    d = os.path.join(SWEEP, f"imagenet_{tag}")
    if os.path.exists(os.path.join(d, "_done")):
        return d
    os.makedirs(d, exist_ok=True)
    rng = np.random.default_rng(0)
    per = n_rows // n_files
    for f in range(n_files):
        images = [rng.bytes(blob_kb * 1024) for _ in range(per)]
        labels = rng.integers(0, 1000, per, dtype=np.int64)
        ids = np.arange(f * per, (f + 1) * per, dtype=np.int64)
        t = pa.table(
            {
                "id": ids,
                "image": pa.array(images, type=pa.binary()),
                "label": pa.array(labels),
            }
        )
        pq.write_table(
            t,
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=rg,
        )
    open(os.path.join(d, "_done"), "w").close()
    print(
        f"  gen imagenet_{tag}: {n_rows} rows ~{n_rows*blob_kb/1024:.0f}MB "
        f"rg={rg} -> ~{per//rg} rgs/file x{n_files}",
        flush=True,
    )
    return d


def gen_wide(tag, n_rows, n_cols=5000, str_len=100, rg=200):
    d = os.path.join(SWEEP, f"wide_{tag}")
    if os.path.exists(os.path.join(d, "_done")):
        return d
    os.makedirs(d, exist_ok=True)
    base = np.array(["x" * str_len] * n_rows, dtype=object)
    cols = {"id": pa.array(np.arange(n_rows, dtype=np.int64))}
    for c in range(n_cols):
        cols[f"c{c}"] = pa.array(base, type=pa.string())
    t = pa.table(cols)
    pq.write_table(
        t, os.path.join(d, "wide.parquet"), write_page_index=True, row_group_size=rg
    )
    open(os.path.join(d, "_done"), "w").close()
    print(
        f"  gen wide_{tag}: {n_rows}x{n_cols} ~{n_rows*n_cols*str_len/1e6:.0f}MB rg={rg}",
        flush=True,
    )
    return d


def run_probe(path, reader, columns=None):
    cmd = [PY, PROBE, "--path", path, "--reader", reader, "--concurrency", "1"]
    if columns:
        cmd += ["--columns", *columns]
    out = subprocess.run(cmd, capture_output=True, text=True, env=dict(os.environ))
    res = {}
    in_result = False
    for line in out.stdout.splitlines():
        if "=== RESULT ===" in line:
            in_result = True
            continue
        if in_result and ":" in line:
            k, v = line.strip().split(":", 1)
            res[k.strip()] = v.strip()
    if not res:
        print(
            f"    PROBE FAIL ({reader}) rc={out.returncode}\n"
            f"{out.stdout[-800:]}\n{out.stderr[-800:]}"
        )
    return res


def sweep_imagenet():
    print("=== imagenet scale sweep (cols=[image,label], tiny rgs) ===", flush=True)
    scales = [("s", 8000), ("m", 24000), ("l", 56000)]  # ~375MB, ~1.1GB, ~2.6GB
    rows = []
    for tag, n in scales:
        d = gen_imagenet(tag, n)
        for reader in ("pyarrow", "arrow_rs"):
            r = run_probe(d, reader, columns=["image", "label"])
            rows.append((tag, n, reader, r))
            print(
                f"  {tag:>2} n={n:<6} {reader:<8} wall={r.get('wall_s')} "
                f"rss={r.get('peak_rss_gb')} uss={r.get('peak_uss_gb')} "
                f"cpu/wall={r.get('cpu_over_wall')}",
                flush=True,
            )
    return rows


def sweep_wide():
    print("=== wide_schema row-group-size sweep (5000 cols) ===", flush=True)
    variants = [("rg200", 4000, 200), ("rg1000", 4000, 1000), ("rg4000", 4000, 4000)]
    rows = []
    for tag, n, rg in variants:
        d = gen_wide(tag, n, rg=rg)
        for reader in ("pyarrow", "arrow_rs"):
            r = run_probe(d, reader)
            rows.append((tag, rg, reader, r))
            print(
                f"  {tag:<7} rg={rg:<5} {reader:<8} wall={r.get('wall_s')} "
                f"rss={r.get('peak_rss_gb')} uss={r.get('peak_uss_gb')} "
                f"read_uss={r.get('read_avg_max_uss_gb')}",
                flush=True,
            )
    return rows


if __name__ == "__main__":
    case = sys.argv[1] if len(sys.argv) > 1 else "imagenet"
    rows = sweep_imagenet() if case == "imagenet" else sweep_wide()
    print("\n=== JSON ===")
    print(json.dumps([{"tag": t, "k": k, "reader": rd, **r} for t, k, rd, r in rows]))
