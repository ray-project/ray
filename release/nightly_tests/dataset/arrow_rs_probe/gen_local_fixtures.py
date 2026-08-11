#!/usr/bin/env python3
"""Generate the local Parquet fixture shapes for grand_experiment.py.

Five shapes, each isolating one axis of the NEW footer-based planner (#64985 series:
footers read up front, row groups pruned then bin-packed by uncompressed size into
read tasks) and/or one known arrow-rs behaviour:

  lone_big_rg     1 file, ONE ~800 MiB uncompressed row group. Unsplittable by any
                  planner (a row group is the atom). arrow-rs K-splits it internally;
                  PyArrow must materialize the whole decoded group. The headline
                  memory shape.
  single_rg_files N files x one ~128 MiB row group each (the parquet_split /
                  release-regression shape). Under the new planner these PACK —
                  several files' groups can share one bin/read task. Exercises the
                  per-native-call fixed cost and the bin-size knob.
  tiny_rgs        blob column, MANY ~2 MiB row groups per file (imagenet/rg_50k
                  shape). Bin packing coalesces dozens of groups per task; without
                  row_hash our reader issues ONE native call per bin (the coalescing
                  win). Also the shape where allocator retention showed up.
  wide            2000 float64 columns, one row group per file (wide_schema shape;
                  mem was 1.50x worse in the old release run).
  fat_col         one ~256 KiB/row binary column + one tiny int column, single row
                  group. The shape that mis-selected the crate's S3 column-group path
                  (Hstack whole-row-group retention). Local decode control here;
                  the S3 stage is where it bites.

All files are written with write_page_index=True (crate requirement) and mostly
incompressible data so on-disk bytes ~ uncompressed bytes (S3 fetch cost stays
honest when synced up).

  python gen_local_fixtures.py --root ~/arrow_rs_grand_fixtures
  python gen_local_fixtures.py --root ... --shapes lone_big_rg,wide   # subset
  python gen_local_fixtures.py --root ... --scale 0.25                # smaller/faster

Writes <root>/manifest.json mapping shape -> path + stats; grand_experiment.py
reads that. Idempotent per shape: skips a shape whose directory already exists.
"""
import argparse
import json
import os
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ~34 B/row of int64 + ~256 B of random-ish string payload -> ~260 B/row uncompressed.
_STR_COLS = 4
_STR_LEN = 64
_POOL_SIZE = 4096


def _str_pool(rng):
    alphabet = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype="S1")
    idx = rng.integers(0, len(alphabet), size=(_POOL_SIZE, _STR_LEN))
    return np.array([alphabet[row].tobytes().decode() for row in idx], dtype=object)


def _string_table(rng, pool, n_rows, id_start=0):
    cols = {"id": pa.array(np.arange(id_start, id_start + n_rows, dtype=np.int64))}
    for c in range(_STR_COLS):
        cols[f"s{c}"] = pa.array(
            pool[rng.integers(0, _POOL_SIZE, size=n_rows)], type=pa.string()
        )
    return pa.table(cols)


def gen_lone_big_rg(d, scale):
    """One file, one big row group (~800 MiB uncompressed at scale=1)."""
    rng = np.random.default_rng(0)
    pool = _str_pool(rng)
    n_rows = int(3_200_000 * scale)
    t = _string_table(rng, pool, n_rows)
    pq.write_table(
        t,
        os.path.join(d, "part0.parquet"),
        write_page_index=True,
        row_group_size=n_rows,  # >= n_rows -> single row group
    )
    return {"files": 1, "rows": n_rows, "uncompressed_bytes": t.nbytes}


def gen_single_rg_files(d, scale):
    """16 files x one ~128 MiB row group each (parquet_split shape)."""
    rng = np.random.default_rng(1)
    pool = _str_pool(rng)
    n_files = max(2, int(16 * scale))
    rows_per = 500_000  # ~128 MiB uncompressed per file
    total = 0
    for f in range(n_files):
        t = _string_table(rng, pool, rows_per, id_start=f * rows_per)
        pq.write_table(
            t,
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=rows_per,
        )
        total += t.nbytes
    return {"files": n_files, "rows": n_files * rows_per, "uncompressed_bytes": total}


def gen_tiny_rgs(d, scale):
    """4 files, blob column, ~2 MiB row groups (32 rows x 64 KiB blobs)."""
    rng = np.random.default_rng(2)
    n_files = 4
    rows_per = max(64, int(1024 * scale))
    blob_bytes = 64 * 1024
    total = 0
    for f in range(n_files):
        blobs = [rng.bytes(blob_bytes) for _ in range(rows_per)]
        t = pa.table(
            {
                "id": pa.array(
                    np.arange(f * rows_per, (f + 1) * rows_per, dtype=np.int64)
                ),
                "image": pa.array(blobs, type=pa.binary()),
                "label": pa.array(rng.integers(0, 1000, rows_per, dtype=np.int64)),
            }
        )
        pq.write_table(
            t,
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=32,  # ~2 MiB per row group
        )
        total += t.nbytes
    return {"files": n_files, "rows": n_files * rows_per, "uncompressed_bytes": total}


def gen_wide(d, scale):
    """4 files, 2000 float64 columns, one row group per file (~64 MiB each)."""
    rng = np.random.default_rng(3)
    n_files = 4
    n_cols = 2000
    rows_per = max(512, int(4096 * scale))
    total = 0
    for f in range(n_files):
        cols = {f"c{c}": rng.random(rows_per) for c in range(n_cols)}
        t = pa.table(cols)
        pq.write_table(
            t,
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=rows_per,
        )
        total += t.nbytes
    return {"files": n_files, "rows": n_files * rows_per, "uncompressed_bytes": total}


def gen_fat_col(d, scale):
    """1 file: one ~256 KiB/row binary column + one int column, single row group."""
    rng = np.random.default_rng(4)
    n_rows = max(128, int(1024 * scale))
    fat = [rng.bytes(256 * 1024) for _ in range(n_rows)]
    t = pa.table(
        {
            "fat": pa.array(fat, type=pa.binary()),
            "small": pa.array(np.arange(n_rows, dtype=np.int64)),
        }
    )
    pq.write_table(
        t,
        os.path.join(d, "part0.parquet"),
        write_page_index=True,
        row_group_size=n_rows,
    )
    return {"files": 1, "rows": n_rows, "uncompressed_bytes": t.nbytes}


SHAPES = {
    "lone_big_rg": gen_lone_big_rg,
    "single_rg_files": gen_single_rg_files,
    "tiny_rgs": gen_tiny_rgs,
    "wide": gen_wide,
    "fat_col": gen_fat_col,
}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", required=True)
    p.add_argument("--shapes", default=",".join(SHAPES))
    p.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="shrink every shape by this factor (0.25 for a quick smoke run)",
    )
    args = p.parse_args()

    root = os.path.expanduser(args.root)
    os.makedirs(root, exist_ok=True)
    manifest_path = os.path.join(root, "manifest.json")
    manifest = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as fh:
            manifest = json.load(fh)

    for shape in [s.strip() for s in args.shapes.split(",") if s.strip()]:
        gen = SHAPES[shape]
        d = os.path.join(root, shape)
        if os.path.isdir(d) and shape in manifest:
            # Skip ONLY at the same scale. A 0.25-scale smoke run must not leave
            # quarter-size fixtures behind for the full run to silently benchmark.
            if manifest[shape].get("scale") == args.scale:
                print(f"  {shape}: exists, skipping ({manifest[shape]})", flush=True)
                continue
            print(
                f"  {shape}: exists at scale={manifest[shape].get('scale')}, "
                f"want scale={args.scale} — regenerating",
                flush=True,
            )
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
        print(f"  {shape}: generating (scale={args.scale}) ...", flush=True)
        stats = gen(d, args.scale)
        on_disk = sum(
            os.path.getsize(os.path.join(d, f))
            for f in os.listdir(d)
            if f.endswith(".parquet")
        )
        stats.update(path=d, on_disk_bytes=on_disk, scale=args.scale)
        manifest[shape] = stats
        print(
            f"  {shape}: {stats['files']} files, {stats['rows']} rows, "
            f"{stats['uncompressed_bytes'] / 2**20:.0f} MiB uncompressed, "
            f"{on_disk / 2**20:.0f} MiB on disk",
            flush=True,
        )
        with open(manifest_path, "w") as fh:
            json.dump(manifest, fh, indent=2)

    print(f"\nmanifest -> {manifest_path}")


if __name__ == "__main__":
    main()
