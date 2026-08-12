#!/usr/bin/env python3
"""Replicate the 2026-08-12 release A/B's *trusted* signals on one Linux box.

TODO item 1ab phase 1 (arrow_rs_docs/TODO.md): the multi-node run's wall / decode
task-seconds are trusted, its memory data mostly is not — so before touching the
release harness, each trusted loss (and the headline win) is replicated locally with
the probe, whose per-task USS (`read_avg_max_uss_gb`) and 50 Hz worker sampler ARE
trustworthy on Linux. Fix what reproduces, keep what's right.

Stages (pick with --skip / --only):

  tensors   R1/item 1y — wide_schema tensors decoded natively 5.59x slower (wall
            1.77x). Fixture: 5000 fixed_size_list<float32,2> columns (lookalike;
            the S3 original is unreadable to us). Runs both readers at
            concurrency=1 (pure decode-speed diagnostic) AND fanned out (adds the
            thread-pool asymmetry: base = unbounded fragment threads, arrow-rs =
            min(4, fragments) — the "PyArrow scales better?" hypothesis).
  binsweep  R2/item 10 — sweep RAY_DATA_PARQUET_BIN_PACKING_BYTES across
            {1 RG, 4 RGs, 1 file, 5 files, 10 files} x both readers. The 5x/10x
            multi-file bins are the first cells to exercise C9 mechanism (i)
            (N sub-fragments on the base's unbounded pool). Plus a PyArrow
            pre_buffer=off arm at {1 file, 10 files} for mechanism (ii)
            (RAY_DATA_PARQUET_PRE_BUFFER=0 — the knob exists only for this).
            Predictions to falsify are in TODO item 10.
  write     R3/item 1aa — write_parquet showed per-task USS 1.23x (trusted
            instrument) at a wall WIN 0.83x. read bin_sweep fixture ->
            write_parquet, both readers; stats come from the materialized
            write plan (no teardown race).
  fatcol    R4/item 1o — known wall ~1.2x on the fat-column shape; rides along
            for a fresh number on this base.

Usage (Linux box, venv active; fixtures first):

  python gen_local_fixtures.py --root ~/arrow_rs_repl_fixtures \
      --shapes bin_sweep,tensors_wide,fat_col
  python replication_matrix.py --fixture-root ~/arrow_rs_repl_fixtures
  python replication_matrix.py --fixture-root ... --only binsweep --repeat 3

Each cell runs read_probe.py in its own process; logs + summary.json land in
--outdir (default ./replication_runs/<ts>). Ratios printed are arrow_rs/pyarrow,
>1 = arrow-rs worse.
"""
import argparse
import json
import os
import time

from run_matrix import _num, median_cell, ratio

MiB = 1024 * 1024


def load_manifest(fixture_root):
    path = os.path.join(os.path.expanduser(fixture_root), "manifest.json")
    with open(path) as fh:
        return json.load(fh)


def binsweep_grid(entry):
    """Derive the bin-budget grid from the bin_sweep fixture's actual geometry, so
    --scale'd fixtures keep the same three regimes (sub-file / one-file / multi-file)."""
    rg = entry["uncompressed_bytes"] // (entry["files"] * entry["rgs_per_file"])
    fl = entry["uncompressed_bytes"] // entry["files"]
    return [
        ("1rg", rg),
        ("4rg", 4 * rg),
        ("1file", fl),
        ("5file", 5 * fl),
        ("10file", 10 * fl),
    ]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fixture-root", required=True)
    p.add_argument("--outdir", default=None)
    p.add_argument("--repeat", type=int, default=1, help="runs per cell, keep median")
    p.add_argument(
        "--skip", default="", help="comma list: tensors,binsweep,write,fatcol"
    )
    p.add_argument("--only", default="", help="comma list: run only these stages")
    args = p.parse_args()

    manifest = load_manifest(args.fixture_root)
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    only = {s.strip() for s in args.only.split(",") if s.strip()}

    def enabled(stage):
        if only:
            return stage in only
        return stage not in skip

    def fixture_path(shape):
        if shape not in manifest:
            raise SystemExit(
                f"fixture '{shape}' missing from {args.fixture_root}/manifest.json — "
                f"run gen_local_fixtures.py --shapes {shape} first"
            )
        return manifest[shape]["path"]

    ts = time.strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "replication_runs", ts
    )
    os.makedirs(outdir, exist_ok=True)
    print(f"logs -> {outdir}\n", flush=True)

    rows = {}

    def cell(name, **kw):
        rows[name] = median_cell(outdir, name, args.repeat, **kw)

    # -------- [tensors] R1 / item 1y --------
    if enabled("tensors"):
        print(
            "=== [tensors] 5000 fixed_size_list cols — decode 5.59x repro ===",
            flush=True,
        )
        path = fixture_path("tensors_wide")
        for reader in ("pyarrow", "arrow_rs"):
            cell(
                f"tensors.c1.{reader}",
                path=path,
                reader=reader,
                concurrency=1,
                columns=None,
                extra_env={},
            )
            cell(
                f"tensors.fan.{reader}",
                path=path,
                reader=reader,
                concurrency=None,
                columns=None,
                extra_env={},
            )

    # -------- [binsweep] R2 / item 10 --------
    grid = []
    if enabled("binsweep"):
        path = fixture_path("bin_sweep")
        grid = binsweep_grid(manifest["bin_sweep"])
        print(
            "=== [binsweep] bins "
            + ", ".join(f"{n}={b // MiB}MiB" for n, b in grid)
            + " ===",
            flush=True,
        )
        for bin_name, bin_bytes in grid:
            for reader in ("pyarrow", "arrow_rs"):
                cell(
                    f"binsweep.{bin_name}.{reader}",
                    path=path,
                    reader=reader,
                    concurrency=None,
                    columns=None,
                    extra_env={"RAY_DATA_PARQUET_BIN_PACKING_BYTES": str(bin_bytes)},
                )
        # pre_buffer=off arm, PyArrow only (C9 mechanism (ii) attribution).
        for bin_name, bin_bytes in grid:
            if bin_name not in ("1file", "10file"):
                continue
            cell(
                f"binsweep.{bin_name}.pyarrow.nopb",
                path=path,
                reader="pyarrow",
                concurrency=None,
                columns=None,
                extra_env={
                    "RAY_DATA_PARQUET_BIN_PACKING_BYTES": str(bin_bytes),
                    "RAY_DATA_PARQUET_PRE_BUFFER": "0",
                },
            )

    # -------- [write] R3 / item 1aa --------
    if enabled("write"):
        print("=== [write] read -> write_parquet (fused) ===", flush=True)
        path = fixture_path("bin_sweep")
        for reader in ("pyarrow", "arrow_rs"):
            cell(
                f"write.{reader}",
                path=path,
                reader=reader,
                concurrency=None,
                columns=None,
                extra_env={},
                extra_args=["--consume", "write_parquet"],
            )

    # -------- [fatcol] R4 / item 1o --------
    if enabled("fatcol"):
        print("=== [fatcol] fat binary column — wall ~1.2x recheck ===", flush=True)
        path = fixture_path("fat_col")
        for reader in ("pyarrow", "arrow_rs"):
            cell(
                f"fatcol.{reader}",
                path=path,
                reader=reader,
                concurrency=1,
                columns=None,
                extra_env={},
            )

    # -------- summary --------
    print("\n============ SUMMARY (R = arrow_rs / pyarrow, >1 worse) ============")

    def pair_line(
        prefix, metrics=("wall_s", "read_wall_s", "read_avg_max_uss_gb", "peak_uss_gb")
    ):
        pa_r = rows.get(f"{prefix}.pyarrow", {})
        ar_r = rows.get(f"{prefix}.arrow_rs", {})
        parts = []
        for m in metrics:
            a, b = _num(ar_r, m), _num(pa_r, m)
            r = ratio(a, b)
            if r is not None:
                parts.append(f"{m}: {b} -> {a} R={r}")
        print(f"  {prefix:<22} " + ("  |  ".join(parts) if parts else "(no data)"))

    if enabled("tensors"):
        print("[tensors] c1 wall R is the decode-speed verdict; fan adds pool-width")
        pair_line("tensors.c1")
        pair_line("tensors.fan")
    if enabled("binsweep"):
        print("[binsweep] prediction: pyarrow USS rises with bin, arrow_rs flat")
        for bin_name, bin_bytes in grid:
            pair_line(f"binsweep.{bin_name}")
        for bin_name in ("1file", "10file"):
            nopb = rows.get(f"binsweep.{bin_name}.pyarrow.nopb", {})
            base = rows.get(f"binsweep.{bin_name}.pyarrow", {})
            v, b = _num(nopb, "read_avg_max_uss_gb"), _num(base, "read_avg_max_uss_gb")
            if v is not None or b is not None:
                print(
                    f"  binsweep.{bin_name}.pyarrow pre_buffer off/on "
                    f"uss={v}/{b} ratio={ratio(v, b)} "
                    f"wall={_num(nopb, 'wall_s')}/{_num(base, 'wall_s')}"
                )
    if enabled("write"):
        print("[write] release said USS R=1.23 at wall R=0.83")
        pair_line("write")
    if enabled("fatcol"):
        print("[fatcol] release-adjacent wall ~1.2x")
        pair_line("fatcol")

    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(rows, fh, indent=2)
    print(f"\nfull JSON + per-cell logs in {outdir}")


if __name__ == "__main__":
    main()
