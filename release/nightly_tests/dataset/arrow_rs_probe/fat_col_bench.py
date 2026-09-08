#!/usr/bin/env python3
"""Tiny standalone A/B: PyArrow vs the arrow-rs crate on the fat_col shape.

fat_col = 1 file, 1 row group, N rows (default 1024), one ~256 KiB/row column
of RANDOM bytes plus one int64 (~256 MiB file at the default). Random bytes
are incompressible, so encoded == decoded and there is nothing to "decode":
the read is pure allocate-and-copy. That makes this the shape where the
crate's allocator behavior (glibc malloc, fresh pages each batch) loses on
wall clock to PyArrow's jemalloc (warm page reuse) while still winning ~2x on
peak memory — findings M56/M69/M70: rs/pa1 wall 1.7-2.7x at peak RSS 0.4-0.5x
(the wall gap varies with box session; the memory win is stable).

Usage (needs pyarrow + numpy + the branch-built ray_data_arrow_rs crate):
    python fat_col_bench.py                    # 1024 rows, 5 reps per arm
    python fat_col_bench.py --rows 2048 --reps 10

Arms (each rep runs in a fresh subprocess so ru_maxrss is a clean per-arm
high-water mark; reps are interleaved pa,pa1,rs to spread machine drift):
    pa   PyArrow dataset scanner, default threads
    pa1  same with use_threads=False — the fair single-thread baseline;
         pa1 == pa on this shape proves threads are NOT why PyArrow wins
    rs   arrow-rs crate at shipping defaults (budget 128 MiB, K=1)
"""
import argparse
import json
import os
import resource
import statistics
import subprocess
import sys
import time

MiB = 1024 * 1024
BUDGET = 128 * MiB  # shipping default (= DataContext.target_max_block_size)


def make_fixture(path, rows):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = np.random.default_rng(4)  # same seed as gen_local_fixtures.gen_fat_col
    fat = [rng.bytes(256 * 1024) for _ in range(rows)]
    t = pa.table(
        {
            "fat": pa.array(fat, type=pa.binary()),
            "small": pa.array(np.arange(rows, dtype=np.int64)),
        }
    )
    pq.write_table(t, path, write_page_index=True, row_group_size=rows)


def batch_size_for(path):
    """Ray's request: budget // encoded-bytes-per-row, floored at 2048 rows."""
    import pyarrow.parquet as pq

    md = pq.read_metadata(path)
    total = sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups))
    bpr = max(1, total // max(1, md.num_rows))
    return max(2048, int(BUDGET // bpr))


def leg_pa(path, use_threads):
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    fmt = pds.ParquetFileFormat(
        default_fragment_scan_options=pds.ParquetFragmentScanOptions(pre_buffer=True)
    )
    frag = fmt.make_fragment(path, filesystem=LocalFileSystem())
    scanner = frag.scanner(batch_size=batch_size_for(path), use_threads=use_threads)
    rows = nbytes = 0
    for b in scanner.to_batches():
        rows += b.num_rows
        nbytes += b.nbytes
    return rows, nbytes


def leg_rs(path):
    import pyarrow as pa
    import ray_data_arrow_rs as rs

    handle = rs.open_parquet_file(path, page_index=False)
    stream = pa.RecordBatchReader.from_stream(
        handle.read_row_groups(
            row_groups=None,
            columns=None,
            batch_size=batch_size_for(path),
            decode_budget_bytes=BUDGET,
            k=1,
            split_threshold_bytes=128 * MiB,
            predicate_json=None,
            fetch_window_mb=16,
            column_fetch_mb=16,
            prefetch_budget_mb=64,
        )
    )
    rows = nbytes = 0
    for b in stream:
        rows += b.num_rows
        nbytes += b.nbytes
    return rows, nbytes


def child(leg, path, rows):
    if leg == "create":
        if not os.path.exists(path):
            make_fixture(path, rows)
        print(
            json.dumps(
                {
                    "size_mib": os.path.getsize(path) / MiB,
                    "batch_rows": batch_size_for(path),
                }
            )
        )
        return
    t0 = time.perf_counter()
    if leg == "pa":
        rows, nb = leg_pa(path, use_threads=True)
    elif leg == "pa1":
        rows, nb = leg_pa(path, use_threads=False)
    else:
        rows, nb = leg_rs(path)
    wall = time.perf_counter() - t0
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mib = ru / MiB if sys.platform == "darwin" else ru / 1024
    print(json.dumps({"wall_s": wall, "peak_rss_mib": peak_mib, "rows": rows}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1024)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--dir", default=os.path.expanduser("~/fat_col_bench_data"))
    ap.add_argument("--leg", choices=["create", "pa", "pa1", "rs"], help="internal")
    args = ap.parse_args()

    os.makedirs(args.dir, exist_ok=True)
    path = os.path.join(args.dir, f"fat_col_{args.rows}.parquet")

    if args.leg:
        child(args.leg, path, args.rows)
        return

    # The parent must stay small: on Linux subprocess forks, so each child's
    # ru_maxrss high-water starts at the parent's RSS at fork time — building
    # the fixture (or importing pyarrow) here would floor every child at the
    # parent's footprint and erase the per-arm memory signal.
    def spawn(leg):
        out = subprocess.run(
            [
                sys.executable,
                os.path.abspath(__file__),
                "--leg",
                leg,
                "--rows",
                str(args.rows),
                "--dir",
                args.dir,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(out.stdout.strip().splitlines()[-1])

    info = spawn("create")
    print(f"file: {path} ({info['size_mib']:.0f} MiB on disk)")
    print(f"batch request: {info['batch_rows']} rows, budget {BUDGET // MiB} MiB")
    results = {"pa": [], "pa1": [], "rs": []}
    for rep in range(args.reps):
        for leg in ("pa", "pa1", "rs"):
            out = subprocess.run(
                [
                    sys.executable,
                    os.path.abspath(__file__),
                    "--leg",
                    leg,
                    "--rows",
                    str(args.rows),
                    "--dir",
                    args.dir,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            r = json.loads(out.stdout.strip().splitlines()[-1])
            results[leg].append(r)
            print(
                f"  rep {rep + 1} {leg:>3}: {r['wall_s']:6.3f} s  "
                f"{r['peak_rss_mib']:7.1f} MiB  rows={r['rows']}"
            )

    rows_seen = {r["rows"] for legs in results.values() for r in legs}
    assert len(rows_seen) == 1, f"arms read different row counts: {rows_seen}"

    def p50(leg, key):
        return statistics.median(r[key] for r in results[leg])

    print(
        f"\n{'arm':>4}  {'wall p50 (s)':>12}  {'wall min':>9}  {'peak RSS p50 (MiB)':>18}"
    )
    for leg in ("pa", "pa1", "rs"):
        wmin = min(r["wall_s"] for r in results[leg])
        print(
            f"{leg:>4}  {p50(leg, 'wall_s'):12.3f}  {wmin:9.3f}  "
            f"{p50(leg, 'peak_rss_mib'):18.1f}"
        )
    print(
        f"\nrs / pa1 (single-thread kernel gap):  "
        f"wall {p50('rs', 'wall_s') / p50('pa1', 'wall_s'):.2f}x   "
        f"peak RSS {p50('rs', 'peak_rss_mib') / p50('pa1', 'peak_rss_mib'):.2f}x"
    )
    print("expected on Linux (M69/M70): wall ~1.7-2.7x slower, RSS ~0.4-0.5x")


if __name__ == "__main__":
    main()
