#!/usr/bin/env python3
"""Generate the two release-test Parquet layouts on OUR S3 bucket.

The release regressions live on two layouts we can't tune in the shared bucket, so
we recreate them on a scratch bucket we own (fill/empty at will) and point the probe
at them:

  wide  -> wide_schema_pipeline_primitives : many columns, ONE big row group per file.
           This is the K=1 lone-big-row-group case where the crate's byte-budgeted
           windowed-async S3 decode is supposed to keep the working set to a page
           while PyArrow materializes the whole decoded row group. mem was 1.50x worse.
  img   -> imagenet (mix.8ds_equal_random_mix) : blob column, MANY tiny row groups.
           This is the I/O-bound-on-S3 case (many small serial range GETs). time 1.67x.

Data is written locally then `aws s3 sync`d up (more robust than pyarrow's S3
region/creds handling). Requires: aws CLI on PATH + creds/region exported, and
write_page_index=True (the crate's page-index requirement).

  python gen_s3_fixtures.py --bucket s3://arrowrs-bench-21f6c795
  python gen_s3_fixtures.py --bucket s3://... --cases wide          # just one layout
  python gen_s3_fixtures.py --bucket s3://... --wide-files 8 --wide-rows 1500

Prints the exact --path values to hand to run_matrix.py at the end.
"""
import argparse
import os
import shutil
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# A pool of distinct, incompressible-ish strings so Snappy can't crush the fixture to
# nothing -- we want on-disk (== S3 fetch) bytes to be non-trivial, not just decoded size.
_POOL_SIZE = 4096


def _str_pool(rng, str_len):
    alphabet = np.frombuffer(b"abcdefghijklmnopqrstuvwxyz0123456789", dtype="S1")
    idx = rng.integers(0, len(alphabet), size=(_POOL_SIZE, str_len))
    return np.array([alphabet[row].tobytes().decode() for row in idx], dtype=object)


def gen_wide(d, n_rows, n_cols, str_len, rg, n_files):
    """Wide schema, ONE big row group per file (rg == n_rows) -> the K=1 S3 case."""
    os.makedirs(d, exist_ok=True)
    rng = np.random.default_rng(0)
    pool = _str_pool(rng, str_len)
    for f in range(n_files):
        cols = {"id": pa.array(np.arange(f * n_rows, (f + 1) * n_rows, dtype=np.int64))}
        for c in range(n_cols):
            picks = pool[rng.integers(0, _POOL_SIZE, size=n_rows)]
            cols[f"c{c}"] = pa.array(picks, type=pa.string())
        pq.write_table(
            pa.table(cols),
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=rg,  # >= n_rows -> a single row group
        )
        print(
            f"  wide part{f}: {n_rows}x{n_cols} str{str_len} " f"rg={rg} (1 rg/file)",
            flush=True,
        )


def gen_imagenet(d, n_rows, blob_kb, rg, n_files):
    """Blob column, MANY tiny row groups -> the S3-IO-bound case."""
    os.makedirs(d, exist_ok=True)
    rng = np.random.default_rng(0)
    per = n_rows // n_files
    for f in range(n_files):
        images = [rng.bytes(blob_kb * 1024) for _ in range(per)]
        labels = rng.integers(0, 1000, per, dtype=np.int64)
        ids = np.arange(f * per, (f + 1) * per, dtype=np.int64)
        pq.write_table(
            pa.table(
                {
                    "id": ids,
                    "image": pa.array(images, type=pa.binary()),
                    "label": pa.array(labels),
                }
            ),
            os.path.join(d, f"part{f}.parquet"),
            write_page_index=True,
            row_group_size=rg,
        )
        print(
            f"  img part{f}: {per} rows blob={blob_kb}KB rg={rg} "
            f"-> ~{per // rg} rgs/file",
            flush=True,
        )


def sync_up(local_dir, s3_prefix):
    print(f"  aws s3 sync {local_dir} -> {s3_prefix}", flush=True)
    subprocess.run(
        ["aws", "s3", "sync", "--only-show-errors", local_dir, s3_prefix],
        check=True,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True, help="e.g. s3://arrowrs-bench-21f6c795")
    p.add_argument("--cases", default="wide,img", help="comma list of {wide,img}")
    p.add_argument("--local-tmp", default="/tmp/arrow_rs_s3_gen")
    p.add_argument("--keep-local", action="store_true", help="don't delete local tmp")
    # wide knobs
    p.add_argument("--wide-rows", type=int, default=1000)
    p.add_argument("--wide-cols", type=int, default=5000)
    p.add_argument("--wide-str-len", type=int, default=100)
    p.add_argument("--wide-files", type=int, default=4)
    # imagenet knobs
    p.add_argument("--img-rows", type=int, default=24000)
    p.add_argument("--img-blob-kb", type=int, default=48)
    p.add_argument("--img-rg", type=int, default=32)
    p.add_argument("--img-files", type=int, default=8)
    args = p.parse_args()

    bucket = args.bucket.rstrip("/")
    cases = [c.strip() for c in args.cases.split(",") if c.strip()]
    paths = {}

    if "wide" in cases:
        print("=== gen wide (1 big row group/file) ===", flush=True)
        d = os.path.join(args.local_tmp, "wide_schema", "primitives")
        gen_wide(
            d,
            args.wide_rows,
            args.wide_cols,
            args.wide_str_len,
            rg=args.wide_rows,  # one row group per file
            n_files=args.wide_files,
        )
        s3 = f"{bucket}/wide_schema/primitives"
        sync_up(d, s3)
        paths["wide"] = s3

    if "img" in cases:
        print("=== gen imagenet (many tiny row groups) ===", flush=True)
        d = os.path.join(args.local_tmp, "imagenet", "parquet")
        gen_imagenet(d, args.img_rows, args.img_blob_kb, args.img_rg, args.img_files)
        s3 = f"{bucket}/imagenet/parquet"
        sync_up(d, s3)
        paths["img"] = s3

    if not args.keep_local and os.path.isdir(args.local_tmp):
        shutil.rmtree(args.local_tmp, ignore_errors=True)

    print("\n=== S3 fixtures ready ===")
    for k, v in paths.items():
        print(f"  {k}: {v}")
    print("\nNext:")
    wide = paths.get("wide", "<wide S3 path>")
    img = paths.get("img", "<imagenet S3 path>")
    print(f"  python run_matrix.py --wide-path {wide} --imagenet-path {img}")


if __name__ == "__main__":
    sys.exit(main())
