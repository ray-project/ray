#!/usr/bin/env python3
"""3-part triage of the 2026-08-15 release-A/B losses (findings M31/M32/M33).

Each loss shape runs in THREE parts so the layer that owns the regression is
read off one table instead of argued about:

  standalone   no Ray, no S3 — the decoder (or decode+realign / decode+write
               pipeline) alone in a fresh process on local files. If a loss
               shows here it is a native-decoder problem.
  ray_local    the same shape through `ray.data.read_parquet` on local files
               (read_probe.py, private local cluster). A loss that appears
               only here is Ray-integration (worker reuse / allocator
               retention / fusion), not the decoder.
  ray_s3       the same Ray read against the scratch S3 bucket. A loss that
               appears only here is the crate's S3 path or fetch behaviour.

The shapes (release test -> fixture, both readers each part):

  auto       M31 read_large_parquet_autoscaling: one ~69 MiB row group per task
             (bin 64 MiB), sub-second tasks. Release instrument could NOT
             measure this (1 Hz poll => end-of-task sample); every Ray cell
             here polls at 20 Hz (--mem-poll-s 0.05) so per-task USS is a real
             peak. The standalone part runs N sequential one-row-group "tasks"
             in ONE process and prints the RSS after each — the end-of-task
             retention curve itself.
  write      M32 write_parquet: fused read->write, ~1.2 GiB decode churn per
             task (bin 1342177280 on the bin_sweep fixture). Won on Linux+S3
             pre-#64985 (avg USS 0.83x, reader comment 186-196); regressed
             1.24x/1.64x with whole-file bins.
  tensorscp  M33 wide_schema tensors: 5000 cloudpickle-tagged tensor columns
             (bin 40894464 like the release yaml) — the crate's skip+realign
             path. This shape has lost OUTSIDE Ray before (T22: wall 5.4x
             pre-fix, 1.25x post), so its standalone part is the decisive leg
             the 2026-08-17 macOS matrix (M34) did not cover.

Every arrow-rs Ray cell also runs a MALLOC_ARENA_MAX=2 variant (drop with
--no-arena-sweep): if the ray_local/ray_s3 losses collapse under it, the
mechanism is glibc arena retention in long-lived workers and the fix is
allocator config / crate-side malloc_trim, not reader logic.

Usage (Linux box; fixtures + venv via run_loss_triage.sh, or piecemeal):

  python gen_local_fixtures.py --root ~/arrow_rs_repl_fixtures \
      --shapes auto_rg,bin_sweep,tensors_cp
  python loss_triage.py --fixture-root ~/arrow_rs_repl_fixtures
  # with the S3 part (scratch bucket you own; fixtures are synced up first):
  ARROW_RS_S3_BUCKET=s3://arrowrs-bench-xxxx python loss_triage.py \
      --fixture-root ~/arrow_rs_repl_fixtures
  python loss_triage.py --fixture-root ... --shapes write --parts ray_local

Results: <outdir>/summary.json + per-cell logs; the printed table is
R = arrow_rs / pyarrow per (shape, part), >1.00 = arrow-rs worse. Standalone
memory is peak RSS of the case subprocess; Ray memory is the 50 Hz worker
sampler's peak USS plus Ray's own per-task USS (20 Hz in-task poll).
"""
import argparse
import gc
import glob
import json
import os
import resource
import subprocess
import sys
import time

from run_matrix import _median, _num, median_cell, ratio

PY = sys.executable
HERE = os.path.dirname(os.path.abspath(__file__))
MiB = 1024 * 1024
# ru_maxrss is KiB on Linux, bytes on macOS.
_RU_UNIT = 1024 if sys.platform.startswith("linux") else 1

# Release-yaml bin sizes per shape (release/release_data_tests.yaml).
SHAPE_BINS = {
    "auto": 67_108_864,
    "write": 1_342_177_280,
    "tensorscp": 40_894_464,
    # M43's shape in-Ray: tensors with ~9.4x dictionary expansion (the release
    # wide_schema tensors regime). Same release bin as tensorscp. tensorscp
    # (~1.1x expansion) stays as the control.
    "tensorsdict": 40_894_464,
    # POSITIVE control (A/B #4 review): the aggregate_groups family is arrow-rs's
    # biggest per-task USS win (R 0.56-0.81, scaling with decoded bytes — pa's
    # scanner keeps the whole decoded task working set resident, we keep ~the
    # budget). Projected read -> groupby -> count, Ray parts only. Any batch-
    # sizing change must NOT regress this cell: the win comes from exactly the
    # small-resident-batch behaviour the fix candidate touches.
    "agg": 67_108_864,
}
SHAPE_FIXTURE = {
    "auto": "auto_rg",
    "write": "bin_sweep",
    "tensorscp": "tensors_cp",
    "tensorsdict": "tensors_dict",
    "agg": "auto_rg",
}
# Ray-only shapes: no standalone part (agg's point is the read-op USS inside a
# read->shuffle->aggregate pipeline, which has no standalone analogue).
RAY_ONLY_SHAPES = {"agg"}
# Shapes that decode through the crate's skip+realign path (cloudpickle
# ARROW:schema) — they share the env flag and the per-batch realign cast.
TENSOR_SHAPES = {"tensorscp", "tensorsdict"}
# Env every cell of a shape needs, standalone included (the cloudpickle flag
# must be set before ray.data is imported).
SHAPE_ENV = {
    s: {"RAY_DATA_AUTOLOAD_CLOUDPICKLE_TENSOR_METADATA": "1"} for s in TENSOR_SHAPES
}


# --------------------------------------------------------------------------
# Part 1: the standalone (no Ray, no S3) case, run as its own subprocess.
# --------------------------------------------------------------------------


def _reader_knobs():
    """Ray's shipped arrow-rs defaults, imported from the reader so this stays
    a single source of truth; falls back to the documented values when the
    reader isn't importable (e.g. running against a stock wheel)."""
    try:
        from ray.data._internal.datasource_v2.readers import (
            arrow_rs_parquet_file_reader as r,
        )

        return dict(
            budget=r._ARROW_RS_DECODE_BUDGET_BYTES,
            k=r._ARROW_RS_K,
            split=r._ARROW_RS_DEFAULT_SPLIT_THRESHOLD_BYTES,
            window=r._ARROW_RS_FETCH_WINDOW_MB,
            column=r._ARROW_RS_COLUMN_FETCH_MB,
            min_rows=r._ARROW_RS_MIN_DECODE_BATCH_ROWS,
        )
    except Exception:
        return dict(
            budget=32 * MiB, k=1, split=128 * MiB, window=16, column=16, min_rows=2048
        )


def _batch_size(md, knobs):
    total = sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups))
    bpr = max(1, total // max(1, md.num_rows))
    return max(knobs["min_rows"], int(knobs["budget"] // bpr))


def _pa_batches(path, batch_size):
    import pyarrow as pa
    import pyarrow.dataset as pds
    from pyarrow.fs import LocalFileSystem

    fmt = pds.ParquetFileFormat(
        default_fragment_scan_options=pds.ParquetFragmentScanOptions(pre_buffer=True)
    )
    frag = fmt.make_fragment(path, filesystem=LocalFileSystem())
    for b in frag.scanner(batch_size=batch_size, use_threads=True).to_batches():
        yield pa.Table.from_batches([b])


def _rs_batches(path, batch_size, knobs, realign_fields=None):
    import pyarrow as pa
    import ray_data_arrow_rs as rs

    handle = rs.open_parquet_file(path, page_index=False)
    stream = pa.RecordBatchReader.from_stream(
        handle.read_row_groups(
            row_groups=None,
            columns=None,
            batch_size=batch_size,
            decode_budget_bytes=knobs["budget"],
            k=knobs["k"],
            split_threshold_bytes=knobs["split"],
            predicate_json=None,
            fetch_window_mb=knobs["window"],
            column_fetch_mb=knobs["column"],
            prefetch_budget_mb=4 * max(knobs["window"], knobs["column"]),
        )
    )
    if realign_fields is None:
        for b in stream:
            yield pa.Table.from_batches([b])
    else:
        # The 1y skip+realign path: the crate decoded parquet STORAGE types
        # (it can't parse the cloudpickle ARROW:schema); cast each batch back
        # to the extension schema exactly like the reader does.
        from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (  # noqa: E501
            _cast_table_to,
        )

        for b in stream:
            yield _cast_table_to(pa.Table.from_batches([b]), realign_fields)


def _consume(tables, mode, out_path):
    """BlockOutputBuffer emulation: coalesce to ~128 MiB blocks; write mode
    feeds the blocks to a ParquetWriter like the fused Read->Write task."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    writer = None
    buf, buf_bytes, n_rows = [], 0, 0

    def flush():
        nonlocal buf, buf_bytes, n_rows, writer
        if not buf:
            return
        block = pa.concat_tables(buf)
        buf, buf_bytes = [], 0
        n_rows += block.num_rows
        if mode == "write":
            if writer is None:
                writer = pq.ParquetWriter(out_path, block.schema)
            writer.write_table(block)

    for t in tables:
        buf.append(t)
        buf_bytes += t.nbytes
        if buf_bytes >= 128 * MiB:
            flush()
    flush()
    if writer is not None:
        writer.close()
    return n_rows


def _cur_rss_mib():
    try:
        import psutil

        return psutil.Process().memory_info().rss / MiB
    except ImportError:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RU_UNIT / MiB


def run_case(a):
    import pyarrow.parquet as pq

    knobs = _reader_knobs()
    files = sorted(glob.glob(os.path.join(os.path.expanduser(a.path), "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet files under {a.path}")

    realign_fields = None
    if a.shape in TENSOR_SHAPES:
        # Both readers need Ray's tensor extension types registered (the
        # release run has them via ray.data); the rs leg additionally realigns
        # to the footer's extension schema, exactly like the reader.
        import ray.data  # noqa: F401

        if a.reader == "rs":
            realign_fields = list(pq.read_schema(files[0]))

    mode = "write" if a.shape == "write" else "decode"
    out_path = os.path.join(a.workdir, f"triage_out_{a.reader}.parquet")

    # `auto` = N sequential one-file (one row group) tasks in one process,
    # recording RSS after each: the end-of-task retention curve. `write` = one
    # release-task-sized unit: files until the bin budget is met (~1.25 GiB of
    # footer bytes, like one whole-file-bin fused task). Else one file.
    if a.shape == "auto":
        task_files = files[: a.tasks]
    elif a.shape == "write":
        task_files, cum = [], 0
        for path in files:
            task_files.append(path)
            md = pq.read_metadata(path)
            cum += sum(
                md.row_group(i).total_byte_size for i in range(md.num_row_groups)
            )
            if cum >= SHAPE_BINS["write"]:
                break
    else:
        task_files = files[:1]

    rss_after_task = []
    rows = 0
    t0 = time.perf_counter()
    for path in task_files:
        md = pq.read_metadata(path)
        bs = _batch_size(md, knobs)
        if a.reader == "pa":
            it = _pa_batches(path, bs)
        else:
            it = _rs_batches(path, bs, knobs, realign_fields)
        rows += _consume(it, mode, out_path)
        del it
        gc.collect()
        rss_after_task.append(round(_cur_rss_mib(), 1))
    wall = time.perf_counter() - t0

    if os.path.exists(out_path):
        os.unlink(out_path)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RU_UNIT
    print("=== CASE RESULT ===")
    print(
        json.dumps(
            dict(
                shape=a.shape,
                reader=a.reader,
                tasks=len(task_files),
                rows=rows,
                wall_s=round(wall, 3),
                peak_rss_mib=round(peak / MiB, 1),
                end_rss_mib=rss_after_task[-1],
                first_task_rss_mib=rss_after_task[0],
                rss_after_task=rss_after_task if a.shape == "auto" else None,
            )
        )
    )


# --------------------------------------------------------------------------
# The matrix: cells across parts x shapes x readers (x allocator arm).
# --------------------------------------------------------------------------


def run_standalone_cell(
    logdir, name, shape, reader, path, env_extra, tasks, repeat, warmup
):
    """Median-of-N standalone case, one fresh subprocess per run (peak RSS must
    not accumulate across repeats)."""
    workdir = os.path.join(logdir, "standalone_tmp")
    os.makedirs(workdir, exist_ok=True)
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
        "--tasks",
        str(tasks),
        "--workdir",
        workdir,
    ]
    env = dict(os.environ)
    env.update(env_extra)

    def one(tag):
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
        with open(os.path.join(logdir, f"{tag}.log"), "w") as fh:
            fh.write(f"# cmd: {' '.join(cmd)}\n# env_extra: {env_extra}\n")
            fh.write(f"# wall_including_startup_s: {time.perf_counter() - t0:.1f}\n")
            fh.write("# ---- STDOUT ----\n" + proc.stdout)
            fh.write("\n# ---- STDERR ----\n" + proc.stderr)
        for i, line in enumerate(proc.stdout.splitlines()):
            if "=== CASE RESULT ===" in line:
                try:
                    return json.loads(proc.stdout.splitlines()[i + 1])
                except (IndexError, json.JSONDecodeError):
                    break
        print(
            f"    !! {tag} CASE FAIL rc={proc.returncode} "
            f"(see {logdir}/{tag}.log)\n       {proc.stderr.strip()[-400:]}",
            flush=True,
        )
        return {}

    for i in range(warmup):
        one(f"{name}.w{i}")
    runs = [one(name if repeat == 1 else f"{name}.r{i}") for i in range(repeat)]
    good = [r for r in runs if r]
    if not good:
        return {}
    out = dict(good[0])
    samples = {}
    for key in list(out):
        vals = [_num({k: str(v) for k, v in r.items()}, key) for r in good]
        if any(v is None for v in vals):
            continue
        out[key] = _median(vals)
        samples[key] = vals
    out["_n"] = len(good)
    out["_samples"] = samples
    print(
        f"    {name:<40} wall={out.get('wall_s')} peak_rss_mib={out.get('peak_rss_mib')} "
        f"end_rss_mib={out.get('end_rss_mib')}",
        flush=True,
    )
    return out


def s3_sync(local_dir, s3_prefix):
    print(f"  aws s3 sync {local_dir} -> {s3_prefix}", flush=True)
    subprocess.run(
        ["aws", "s3", "sync", "--only-show-errors", local_dir, s3_prefix],
        check=True,
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("case", help="internal: one standalone case (fresh process)")
    c.add_argument("--shape", choices=list(SHAPE_BINS), required=True)
    c.add_argument("--reader", choices=["pa", "rs"], required=True)
    c.add_argument("--path", required=True)
    c.add_argument("--tasks", type=int, default=24)
    c.add_argument("--workdir", default="/tmp")

    p.add_argument("--fixture-root", default=None)
    p.add_argument("--outdir", default=None)
    p.add_argument("--shapes", default="auto,write,tensorscp")
    p.add_argument(
        "--parts",
        default=None,
        help=(
            "comma list of standalone,ray_local,ray_s3 (default: all three when "
            "ARROW_RS_S3_BUCKET / --s3-bucket is set, else the first two)"
        ),
    )
    p.add_argument("--s3-bucket", default=os.environ.get("ARROW_RS_S3_BUCKET"))
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--tasks", type=int, default=24, help="auto standalone task count")
    p.add_argument(
        "--no-arena-sweep",
        action="store_true",
        help="skip the MALLOC_ARENA_MAX=2 variant of each arrow_rs Ray cell",
    )
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
    if args.parts:
        parts = [x.strip() for x in args.parts.split(",") if x.strip()]
    else:
        parts = ["standalone", "ray_local"] + (["ray_s3"] if args.s3_bucket else [])
    if "ray_s3" in parts and not args.s3_bucket:
        p.error("ray_s3 needs --s3-bucket or ARROW_RS_S3_BUCKET")

    outdir = args.outdir or os.path.join(
        HERE, "loss_triage_runs", time.strftime("%Y%m%d_%H%M%S")
    )
    os.makedirs(outdir, exist_ok=True)
    print(f"parts={parts} shapes={shapes} outdir={outdir}", flush=True)

    summary = {"parts": parts, "shapes": shapes, "cells": {}}

    def fixture_path(shape):
        entry = manifest[SHAPE_FIXTURE[shape]]
        return entry["path"] if isinstance(entry, dict) else entry

    # Sync fixtures for the S3 part once, up front.
    s3_paths = {}
    if "ray_s3" in parts:
        bucket = args.s3_bucket.rstrip("/")
        for shape in shapes:
            s3_paths[shape] = f"{bucket}/loss_triage/{SHAPE_FIXTURE[shape]}"
            s3_sync(fixture_path(shape), s3_paths[shape])

    for shape in shapes:
        shape_env = dict(SHAPE_ENV.get(shape, {}))
        local_path = fixture_path(shape)

        for part in parts:
            if part == "standalone" and shape in RAY_ONLY_SHAPES:
                print(f"\n=== [{shape}] standalone — skipped (Ray-only shape)")
                continue
            print(f"\n=== [{shape}] {part} ===", flush=True)
            cells = {}

            if part == "standalone":
                for reader in ("pa", "rs"):
                    cells[reader] = run_standalone_cell(
                        outdir,
                        f"{shape}.standalone.{reader}",
                        shape,
                        reader,
                        local_path,
                        shape_env,
                        args.tasks,
                        args.repeat,
                        args.warmup,
                    )
            else:
                path = local_path if part == "ray_local" else s3_paths[shape]
                extra_args = ["--mem-poll-s", "0.05"]
                columns = None
                if shape == "write":
                    extra_args += ["--consume", "write_parquet"]
                elif shape == "agg":
                    # Projection pushdown (the release aggregates read 2-3
                    # columns of a wide table) + the shuffle/aggregate consume.
                    extra_args += ["--consume", "groupby", "--groupby-key", "s0"]
                    columns = ["id", "s0"]
                env = dict(shape_env)
                env["RAY_DATA_PARQUET_BIN_PACKING_BYTES"] = str(SHAPE_BINS[shape])
                arms = [("pyarrow", "pa", {}), ("arrow_rs", "rs", {})]
                if not args.no_arena_sweep:
                    arms.append(("arrow_rs", "rs_arena2", {"MALLOC_ARENA_MAX": "2"}))
                for reader, tag, arm_env in arms:
                    cells[tag] = median_cell(
                        outdir,
                        f"{shape}.{part}.{tag}",
                        args.repeat,
                        warmup=args.warmup,
                        path=path,
                        reader=reader,
                        concurrency=None,
                        columns=columns,
                        extra_env={**env, **arm_env},
                        extra_args=extra_args,
                    )

            summary["cells"][f"{shape}.{part}"] = cells

    # ---------------- ratio table ----------------
    print(
        "\n\n================ LOSS TRIAGE SUMMARY (R = arrow_rs/pyarrow) ================"
    )
    if sys.platform == "darwin":
        print("(macOS: peak_uss is None and wall is not certifiable — smoke run only)")
    header = (
        f"{'cell':<26} {'wall R':>8} {'peak mem R':>11} {'task USS R':>11} "
        f"{'arena2 mem R':>13} {'spill T/B GB':>13}"
    )
    print(header)
    print("-" * len(header))
    for key, cells in summary["cells"].items():
        pa_res = cells.get("pa") or cells.get("pyarrow") or {}
        rs_res = cells.get("rs") or {}
        ar_res = cells.get("rs_arena2") or {}
        if "standalone" in key:
            wall_r = ratio(_num(rs_res, "wall_s"), _num(pa_res, "wall_s"))
            mem_r = ratio(_num(rs_res, "peak_rss_mib"), _num(pa_res, "peak_rss_mib"))
            task_r = ratio(_num(rs_res, "end_rss_mib"), _num(pa_res, "end_rss_mib"))
            ar_r = None
        else:
            wall_r = ratio(_num(rs_res, "wall_s"), _num(pa_res, "wall_s"))
            mem_r = ratio(
                _num(rs_res, "peak_uss_gb"), _num(pa_res, "peak_uss_gb")
            ) or ratio(_num(rs_res, "peak_rss_gb"), _num(pa_res, "peak_rss_gb"))
            task_r = ratio(
                _num(rs_res, "read_avg_max_uss_gb"), _num(pa_res, "read_avg_max_uss_gb")
            )
            ar_r = ratio(_num(ar_res, "peak_uss_gb"), _num(pa_res, "peak_uss_gb"))
        fmt = lambda v: f"{v:.2f}" if v is not None else "—"  # noqa: E731
        sp_t, sp_b = _num(rs_res, "spilled_gb"), _num(pa_res, "spilled_gb")
        spill = (
            f"{sp_t:.1f}/{sp_b:.1f}" if sp_t is not None and sp_b is not None else "—"
        )
        print(
            f"{key:<26} {fmt(wall_r):>8} {fmt(mem_r):>11} {fmt(task_r):>11} "
            f"{fmt(ar_r):>13} {spill:>13}"
        )
    print(
        "\nRead it as: loss in standalone => decoder; only in ray_local => Ray worker/\n"
        "allocator (arena2 column collapsing confirms glibc arenas); only in ray_s3 =>\n"
        "crate S3 path. Full metrics: " + os.path.join(outdir, "summary.json")
    )

    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)


if __name__ == "__main__":
    main()
