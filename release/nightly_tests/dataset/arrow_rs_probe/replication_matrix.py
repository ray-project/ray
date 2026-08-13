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
  binbound  R2b — THE BOUND CHECK (user ask 2026-08-12): one bin is one read
            task, its whole decoded output lives in that worker process, so the
            arrow-rs guarantee is "per-task USS is bounded by the bin budget" —
            the property PyArrow lacks (it buffers a whole decoded row group per
            fragment, plus a pre_buffer'd compressed span). Same bin grid as
            binsweep but with --task-concurrency 1 (one bin resident per process,
            so per-task USS is attributable to ONE bin) and --mem-poll-s 0.05
            (at the 1 Hz default a short read task gets one sample or none, which
            silently flattens the very number the verdict rests on). Verdict is a
            least-squares slope of per-task USS vs DECODED bytes per task:
            slope <~0.3 = flat (bounded far below the bin), <~1 = bounded by the
            bin, >1 = unbounded => retention or leak. Denominator is decoded
            bytes, not the knob: the knob budgets Parquet total_uncompressed_size
            (pages decompressed but still dictionary/RLE-ENCODED), so
            decoded/knob is an expansion factor >= 1 that the table prints
            instead of assuming.
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


def footer_geometry(path):
    """Measure the bin packer's OWN accounting unit from the fixture's footers.

    Three things this pins down that no nominal fixture size can (all verified on
    the tree, 2026-08-12):

    1. On a no-projection read the packer prices a row group at
       ``row_group.total_byte_size`` (``listing/footer_reader.py:148-151``), NOT at
       the summed per-column ``total_uncompressed_size``. Those are equal for a
       well-behaved writer but ``total_byte_size`` can carry the *compressed* size
       (apache/arrow#48138 — which is why the reader's own batch-size estimator
       refuses that accessor, ``readers/parquet_file_reader.py:148-155``). We report
       the ratio so a fixture that trips the bug is visible instead of silently
       shrinking every bin.
    2. Decoded Arrow bytes are ``expansion`` x the packer's number, because the
       footer counts pages that are decompressed but still dictionary/RLE-*encoded*.
       Ray itself assumes 5x here (``PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT``,
       ``datasource/parquet_datasource.py:109``). So "USS bounded by the bin" can
       only ever mean "bounded by ``expansion`` x bin"; this makes the factor a
       measured column rather than an unstated assumption.
    3. The grid labels ("1file") then mean a real file's worth of the packer's
       bytes, so bin sizes and task counts agree.
    """
    import glob as _glob

    import pyarrow.parquet as pq

    files = sorted(_glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"no parquet files under {path}")
    tbs_total = unc_total = rg_count = 0
    for f in files:
        md = pq.ParquetFile(f).metadata
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            tbs_total += rg.total_byte_size
            unc_total += sum(
                rg.column(j).total_uncompressed_size for j in range(rg.num_columns)
            )
            rg_count += 1
    # Expansion from ONE file (cheap) — decoded Arrow bytes / the packer's bytes.
    md0 = pq.ParquetFile(files[0]).metadata
    f0_tbs = sum(md0.row_group(i).total_byte_size for i in range(md0.num_row_groups))
    decoded0 = pq.read_table(files[0]).nbytes
    return {
        "files": len(files),
        "row_groups": rg_count,
        "packer_bytes_total": tbs_total,
        "rg_bytes": tbs_total // rg_count,
        "file_bytes": tbs_total // len(files),
        "tbs_over_uncompressed": round(tbs_total / unc_total, 3) if unc_total else None,
        "expansion": round(decoded0 / f0_tbs, 3) if f0_tbs else None,
    }


def bin_grid(geom):
    """Bin budgets in the packer's own units: 1 and 4 row groups, then 1/2/5/10 files.

    The 5x/10x-a-file cells are the "much bigger bin" ask. Any cell that already
    swallows the whole fixture is collapsed to a single ``all`` cell — two cells
    that both pack everything into one bin measure the same thing twice.
    """
    rg, fl = geom["rg_bytes"], geom["file_bytes"]
    total = geom["packer_bytes_total"]
    out, saturated = [], False
    for name, size in [
        ("1rg", rg),
        ("4rg", 4 * rg),
        ("1file", fl),
        ("2file", 2 * fl),
        ("5file", 5 * fl),
        ("10file", 10 * fl),
    ]:
        if size >= total:
            if saturated:
                continue
            saturated = True
            name = f"all({name})"
        out.append((name, size))
    return out


def binsweep_grid(entry):
    """Fallback grid from the fixture manifest's *nominal* sizes, used only if the
    footers can't be read. Nominal bytes are the generator's row-width arithmetic,
    which ran 1.6x above the footers on the smoke fixture — so labels drift and task
    counts won't match the names. Prefer ``bin_grid(footer_geometry(path))``."""
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
        "--skip",
        default="",
        help="comma list: tensors,binsweep,binbound,write,fatcol",
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
    geom = {}
    if enabled("binsweep") or enabled("binbound"):
        try:
            geom = footer_geometry(fixture_path("bin_sweep"))
            grid = bin_grid(geom)
            print(
                f"bin_sweep footers: {geom['files']} files / {geom['row_groups']} row "
                f"groups, packer prices a row group at {geom['rg_bytes'] // MiB}MiB "
                f"(total_byte_size/uncompressed={geom['tbs_over_uncompressed']}"
                + (
                    "  <-- NOT 1.0: apache/arrow#48138, bins are priced in COMPRESSED "
                    "bytes on this fixture"
                    if geom["tbs_over_uncompressed"]
                    and abs(geom["tbs_over_uncompressed"] - 1.0) > 0.02
                    else ""
                )
                + f"), decoded/packer expansion={geom['expansion']}x "
                f"(Ray's own default assumption is 5x)\n",
                flush=True,
            )
        except Exception as e:  # noqa: BLE001 - fall back, don't lose the run
            print(f"!! footer_geometry failed ({e!r}); using nominal manifest grid")
            grid = binsweep_grid(manifest["bin_sweep"])
    if enabled("binsweep"):
        path = fixture_path("bin_sweep")
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

    # -------- [binbound] R2b — is per-task USS bounded by the bin budget? --------
    if enabled("binbound"):
        path = fixture_path("bin_sweep")
        print(
            "=== [binbound] one bin per process (task-concurrency 1, 20 Hz USS) ===",
            flush=True,
        )
        for bin_name, bin_bytes in grid:
            for reader in ("pyarrow", "arrow_rs"):
                cell(
                    f"binbound.{bin_name}.{reader}",
                    path=path,
                    reader=reader,
                    concurrency=None,
                    columns=None,
                    extra_env={"RAY_DATA_PARQUET_BIN_PACKING_BYTES": str(bin_bytes)},
                    extra_args=[
                        "--task-concurrency",
                        "1",
                        "--mem-poll-s",
                        "0.05",
                    ],
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
    if enabled("binbound"):
        print(
            "\n[binbound] THE BOUND CHECK — per-task USS vs the bin it decoded.\n"
            "  bin      = RAY_DATA_PARQUET_BIN_PACKING_BYTES, in the packer's units\n"
            "             (row_group.total_byte_size: decompressed but still ENCODED)\n"
            "  dec/task = decoded Arrow bytes per read task (the real bin size);\n"
            "             dec/bin is the encoding expansion — measured "
            f"{geom.get('expansion')}x on this\n"
            "             fixture, and Ray's own planner assumes 5x. The knob is a\n"
            "             proxy for decoded bytes, never an upper bound on them.\n"
            "  uss/dec  = per-task peak USS over decoded bytes. This is the number:\n"
            "             it includes the fixed ~0.2-0.4 GB python+ray+pyarrow floor,\n"
            "             so it is large at tiny bins and must FALL toward a constant.\n"
            "  mx/av    = worst task / average task USS. ~1 = every task costs the\n"
            "             same; rising = the worker retains across tasks (allocator\n"
            "             retention or a leak), which no bin cap can bound."
        )
        for reader in ("pyarrow", "arrow_rs"):
            print(f"  --- {reader} ---")
            pts = []
            for bin_name, bin_bytes in grid:
                r = rows.get(f"binbound.{bin_name}.{reader}", {})
                dec = _num(r, "read_bytes_per_task_gb")
                uss = _num(r, "read_max_uss_gb") or _num(r, "read_avg_max_uss_gb")
                bin_gb = bin_bytes / (1024**3)
                if dec is not None and uss is not None:
                    pts.append((dec, uss))
                print(
                    f"    {bin_name:<11} bin={bin_bytes // MiB:>6}MiB "
                    f"tasks={r.get('read_num_tasks')} "
                    f"dec/task={dec}GB dec/bin={ratio(dec, bin_gb)} "
                    f"uss_max={uss} uss/dec={ratio(uss, dec)} "
                    f"mx/av={r.get('uss_max_over_avg')} wall={_num(r, 'wall_s')}"
                )
            # Least squares on USS = a + b*decoded_bytes. b is the verdict: how many
            # bytes of private memory each extra decoded byte in the bin costs.
            if len(pts) >= 3:
                n = len(pts)
                mx = sum(p[0] for p in pts) / n
                my = sum(p[1] for p in pts) / n
                den = sum((p[0] - mx) ** 2 for p in pts)
                if den > 0:
                    b = sum((p[0] - mx) * (p[1] - my) for p in pts) / den
                    a = my - b * mx
                    if b <= 0.3:
                        verdict = "FLAT — bounded well below the bin"
                    elif b <= 1.1:
                        verdict = "BOUNDED by the bin (slope ~1)"
                    else:
                        verdict = (
                            "UNBOUNDED — grows FASTER than the bin; "
                            "suspect retention/leak, not just buffering"
                        )
                    print(
                        f"    fit: uss ~= {round(a, 3)}GB + {round(b, 3)} x decoded  "
                        f"-> {verdict}"
                    )
            else:
                print("    fit: not enough cells with USS (Linux only) to fit a slope")
        print(
            "  arrow-rs must be FLAT or BOUNDED; anything else is the leak this stage\n"
            "  exists to catch. PyArrow is expected to slope up (whole decoded row\n"
            "  group per fragment x fragments in the bin, + the pre_buffer span)."
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
