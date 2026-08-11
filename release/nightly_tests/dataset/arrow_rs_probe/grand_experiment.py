#!/usr/bin/env python3
"""The grand tuning experiment: arrow-rs vs PyArrow on the NEW footer-based planner.

The #64985 series changed what there is to tune: footers are read up front, row
groups are statistics-pruned and bin-packed by UNCOMPRESSED size into read tasks
(env ``RAY_DATA_PARQUET_BIN_PACKING_BYTES``, default 128 MiB), and the PyArrow path
runs one fragment thread per fragment, unbounded. This experiment maps that new
tuning surface for both readers in one run, over the shapes from
gen_local_fixtures.py.

Stages (each cell = one read_probe.py subprocess: fresh Ray + fresh crate, env-injected
knobs, full log kept):

  A  headline   every shape x {pyarrow, arrow_rs}, all-default knobs.
                The number that matters: R/P wall and USS per shape on the new planner.
  B  bin        RAY_DATA_PARQUET_BIN_PACKING_BYTES in {32Mi, 512Mi} (128Mi = stage A)
                x both readers x {single_rg_files, tiny_rgs}. The NEW knob: how does
                read-task sizing interact with each reader?
  C  budget     RAY_DATA_ARROW_RS_DECODE_BUDGET_BYTES in {8Mi, 128Mi} (32Mi = stage A)
                x arrow_rs x {lone_big_rg, fat_col}. Does the decode budget still
                bound memory now that bins set task size?
  D  threads    RAY_DATA_READ_FILES_NUM_THREADS in {2, 4} x arrow_rs x
                {single_rg_files, tiny_rgs}. Re-decides the "1 fragment thread"
                default against the new unbounded-PyArrow baseline (old finding K6
                measured against a 4-thread baseline that no longer exists).
  E  s3         (only with --s3-bucket) sync fixtures up, rerun stage A on S3, plus
                RAY_DATA_ARROW_RS_FETCH_WINDOW_MB in {4, 64} (16 = default) x
                arrow_rs x {lone_big_rg, tiny_rgs}. The crate's byte-budgeted
                windowed decode only runs on the S3 path, so the memory-knob story
                is only visible here.

arrow_rs cells run with RAY_DATA_ARROW_RS_STRICT=1: if the reader silently falls
back to PyArrow the cell FAILS instead of quietly measuring the wrong engine.

Usage (venv active, after gen_local_fixtures.py):

  python grand_experiment.py --fixtures-root ~/arrow_rs_grand_fixtures
  python grand_experiment.py --fixtures-root ... --repeat 3          # median of 3
  python grand_experiment.py --fixtures-root ... --stages A,B        # subset
  python grand_experiment.py --fixtures-root ... --s3-bucket s3://arrowrs-bench-21f6c795

Outputs <outdir>/summary.json (all cells), summary.md (ratio tables), and one
.log per cell.
"""
import argparse
import json
import os
import subprocess
import sys
import time

PROBE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "read_probe.py")
PY = sys.executable
MiB = 1024 * 1024

# Metrics pulled from read_probe's RESULT block into the tables.
_METRICS = [
    "wall_s",
    "cpu_over_wall",
    "read_avg_max_uss_gb",
    "peak_uss_gb",
    "peak_rss_gb",
]
# Ratio direction: for these, ratio = arrow_rs / pyarrow and >1 means arrow_rs WORSE.
_RATIO_METRICS = ["wall_s", "read_avg_max_uss_gb", "peak_uss_gb"]


def run_cell(logdir, name, path, reader, extra_env, columns=None, repeat=1):
    """Run read_probe.py `repeat` times; return the median-wall run's RESULT dict."""
    runs = []
    for i in range(repeat):
        tag = name if repeat == 1 else f"{name}.r{i}"
        cmd = [PY, PROBE, "--path", path, "--reader", reader]
        if columns:
            cmd += ["--columns", *columns]
        env = dict(os.environ)
        env.update(extra_env)
        if reader == "arrow_rs":
            env["RAY_DATA_ARROW_RS_STRICT"] = "1"

        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
        dur = time.perf_counter() - t0

        with open(os.path.join(logdir, f"{tag}.log"), "w") as fh:
            fh.write(f"# cmd: {' '.join(cmd)}\n# extra_env: {extra_env}\n")
            fh.write(f"# wall_including_startup_s: {dur:.1f}\n# ---- STDOUT ----\n")
            fh.write(proc.stdout)
            fh.write("\n# ---- STDERR ----\n")
            fh.write(proc.stderr)

        res = {}
        in_result = False
        for line in proc.stdout.splitlines():
            if "=== RESULT ===" in line:
                in_result = True
                continue
            if in_result and ":" in line:
                k, v = line.strip().split(":", 1)
                res[k.strip()] = v.strip()
        if not res:
            print(
                f"    !! {tag} FAIL rc={proc.returncode} "
                f"(see {tag}.log)  {proc.stderr.strip()[-300:]}",
                flush=True,
            )
        else:
            print(
                f"    {tag:<44} wall={res.get('wall_s')} "
                f"cpu/wall={res.get('cpu_over_wall')} "
                f"uss={res.get('read_avg_max_uss_gb') or res.get('peak_uss_gb')}",
                flush=True,
            )
        runs.append(res)

    good = [r for r in runs if r]
    if not good:
        return {}
    good.sort(key=lambda r: _num(r, "wall_s") or float("inf"))
    return good[len(good) // 2]


def _num(res, key):
    try:
        return float(res.get(key))
    except (TypeError, ValueError):
        return None


def _ratio(a, b):
    if a is None or b is None or b == 0:
        return None
    return round(a / b, 3)


def _s3_sync(fixtures_root, bucket, shapes):
    """aws s3 sync each shape dir up; return shape -> s3 path."""
    out = {}
    for shape, info in shapes.items():
        dst = f"{bucket.rstrip('/')}/grand/{shape}"
        print(f"  syncing {shape} -> {dst}", flush=True)
        subprocess.run(
            ["aws", "s3", "sync", "--only-show-errors", info["path"], dst],
            check=True,
        )
        out[shape] = dst
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fixtures-root", required=True)
    p.add_argument("--outdir", default=None)
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--stages", default="A,B,C,D,E", help="comma subset of A,B,C,D,E")
    p.add_argument(
        "--s3-bucket",
        default=os.environ.get("ARROW_RS_S3_BUCKET"),
        help="s3://... scratch bucket; enables stage E (also via ARROW_RS_S3_BUCKET)",
    )
    args = p.parse_args()

    stages = {s.strip().upper() for s in args.stages.split(",") if s.strip()}
    root = os.path.expanduser(args.fixtures_root)
    with open(os.path.join(root, "manifest.json")) as fh:
        shapes = json.load(fh)

    ts = time.strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "grand_runs", ts
    )
    os.makedirs(outdir, exist_ok=True)
    print(f"shapes: {list(shapes)}   logs -> {outdir}\n", flush=True)

    rows = {}  # cell name -> RESULT dict

    def cell(name, path, reader, extra_env, columns=None):
        rows[name] = run_cell(
            outdir, name, path, reader, extra_env, columns=columns, repeat=args.repeat
        )

    # -------- A: headline, all defaults --------
    if "A" in stages:
        print(
            "=== [A] headline: every shape x both readers, default knobs ===",
            flush=True,
        )
        for shape, info in shapes.items():
            for reader in ("pyarrow", "arrow_rs"):
                cell(f"A.{shape}.{reader}", info["path"], reader, {})

    # -------- B: bin-packing size (the NEW planner knob) --------
    if "B" in stages:
        print(
            "=== [B] bin size sweep (RAY_DATA_PARQUET_BIN_PACKING_BYTES) ===",
            flush=True,
        )
        for shape in ("single_rg_files", "tiny_rgs"):
            if shape not in shapes:
                continue
            for bin_mib in (32, 512):
                for reader in ("pyarrow", "arrow_rs"):
                    cell(
                        f"B.{shape}.bin{bin_mib}Mi.{reader}",
                        shapes[shape]["path"],
                        reader,
                        {"RAY_DATA_PARQUET_BIN_PACKING_BYTES": str(bin_mib * MiB)},
                    )

    # -------- C: decode budget (arrow-rs) --------
    if "C" in stages:
        print(
            "=== [C] decode budget sweep (RAY_DATA_ARROW_RS_DECODE_BUDGET_BYTES) ===",
            flush=True,
        )
        for shape in ("lone_big_rg", "fat_col"):
            if shape not in shapes:
                continue
            for budget_mib in (8, 128):
                cell(
                    f"C.{shape}.budget{budget_mib}Mi.arrow_rs",
                    shapes[shape]["path"],
                    "arrow_rs",
                    {"RAY_DATA_ARROW_RS_DECODE_BUDGET_BYTES": str(budget_mib * MiB)},
                )

    # -------- D: fragment threads (arrow-rs; PyArrow is unbounded by design) --------
    if "D" in stages:
        print(
            "=== [D] fragment-thread sweep (RAY_DATA_READ_FILES_NUM_THREADS) ===",
            flush=True,
        )
        for shape in ("single_rg_files", "tiny_rgs"):
            if shape not in shapes:
                continue
            for threads in (2, 4):
                cell(
                    f"D.{shape}.threads{threads}.arrow_rs",
                    shapes[shape]["path"],
                    "arrow_rs",
                    {"RAY_DATA_READ_FILES_NUM_THREADS": str(threads)},
                )

    # -------- E: S3 (headline + fetch window) --------
    s3_paths = {}
    if "E" in stages and args.s3_bucket:
        print(f"=== [E] S3 stage (bucket {args.s3_bucket}) ===", flush=True)
        try:
            s3_paths = _s3_sync(root, args.s3_bucket, shapes)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            # No creds / no aws CLI must not crash the run AFTER stages A-D
            # produced results but BEFORE the summary was written.
            print(f"    !! S3 sync failed ({e}) — skipping stage E", flush=True)
            s3_paths = {}
        for shape, s3_path in s3_paths.items():
            for reader in ("pyarrow", "arrow_rs"):
                cell(f"E.{shape}.{reader}", s3_path, reader, {})
        for shape in ("lone_big_rg", "tiny_rgs"):
            if shape not in s3_paths:
                continue
            for window in (4, 64):
                cell(
                    f"E.{shape}.window{window}Mi.arrow_rs",
                    s3_paths[shape],
                    "arrow_rs",
                    {"RAY_DATA_ARROW_RS_FETCH_WINDOW_MB": str(window)},
                )
    elif "E" in stages:
        print("=== [E] skipped: no --s3-bucket / ARROW_RS_S3_BUCKET ===", flush=True)

    # -------- summary --------
    lines = ["# grand_experiment summary", ""]
    lines.append(f"run: {ts}   repeat={args.repeat}   fixtures={root}")
    lines.append("")
    lines.append("Ratios are arrow_rs / pyarrow; **> 1.00 means arrow-rs is worse**.")
    lines.append("")

    def emit(text=""):
        print(text, flush=True)
        lines.append(text)

    emit("\n=========== HEADLINE (stage A: default knobs, per shape) ===========")
    emit("| shape | wall R/P | USS R/P | wall (P -> R) | USS GB (P -> R) |")
    emit("|---|---|---|---|---|")
    for shape in shapes:
        pa_r = rows.get(f"A.{shape}.pyarrow", {})
        ar_r = rows.get(f"A.{shape}.arrow_rs", {})
        wall_p, wall_a = _num(pa_r, "wall_s"), _num(ar_r, "wall_s")
        # Ray's own per-task USS is the metric of record; sampler peak is backup.
        uss_key = (
            "read_avg_max_uss_gb"
            if _num(pa_r, "read_avg_max_uss_gb") is not None
            else "peak_uss_gb"
        )
        uss_p, uss_a = _num(pa_r, uss_key), _num(ar_r, uss_key)
        emit(
            f"| {shape} | {_ratio(wall_a, wall_p)} | {_ratio(uss_a, uss_p)} "
            f"| {wall_p} -> {wall_a} | {uss_p} -> {uss_a} |"
        )

    def sweep_table(title, prefix_fmt, axis_values, shapes_subset, readers):
        emit(f"\n=========== {title} ===========")
        header = "| shape | reader | " + " | ".join(str(v) for v in axis_values) + " |"
        emit(header)
        emit("|---" * (2 + len(axis_values)) + "|")
        for shape in shapes_subset:
            for reader in readers:
                cells = []
                for v in axis_values:
                    r = rows.get(prefix_fmt.format(shape=shape, v=v, reader=reader), {})
                    cells.append(
                        f"w={_num(r, 'wall_s')} u={_num(r, 'read_avg_max_uss_gb') or _num(r, 'peak_uss_gb')}"
                    )
                emit(f"| {shape} | {reader} | " + " | ".join(cells) + " |")

    if "B" in stages:
        sweep_table(
            "stage B: bin size (32Mi / 512Mi; 128Mi default = stage A)",
            "B.{shape}.bin{v}Mi.{reader}",
            [32, 512],
            [s for s in ("single_rg_files", "tiny_rgs") if s in shapes],
            ["pyarrow", "arrow_rs"],
        )
    if "C" in stages:
        sweep_table(
            "stage C: arrow-rs decode budget (8Mi / 128Mi; 32Mi default = stage A)",
            "C.{shape}.budget{v}Mi.{reader}",
            [8, 128],
            [s for s in ("lone_big_rg", "fat_col") if s in shapes],
            ["arrow_rs"],
        )
    if "D" in stages:
        sweep_table(
            "stage D: arrow-rs fragment threads (2 / 4; 1 default = stage A)",
            "D.{shape}.threads{v}.{reader}",
            [2, 4],
            [s for s in ("single_rg_files", "tiny_rgs") if s in shapes],
            ["arrow_rs"],
        )
    if s3_paths:
        emit("\n=========== stage E: S3 headline ===========")
        emit("| shape | wall R/P | USS R/P |")
        emit("|---|---|---|")
        for shape in shapes:
            pa_r = rows.get(f"E.{shape}.pyarrow", {})
            ar_r = rows.get(f"E.{shape}.arrow_rs", {})
            uss_key = (
                "read_avg_max_uss_gb"
                if _num(pa_r, "read_avg_max_uss_gb") is not None
                else "peak_uss_gb"
            )
            emit(
                f"| {shape} | {_ratio(_num(ar_r, 'wall_s'), _num(pa_r, 'wall_s'))} "
                f"| {_ratio(_num(ar_r, uss_key), _num(pa_r, uss_key))} |"
            )
        sweep_table(
            "stage E: S3 fetch window (4Mi / 64Mi; 16Mi default = E headline)",
            "E.{shape}.window{v}Mi.{reader}",
            [4, 64],
            [s for s in ("lone_big_rg", "tiny_rgs") if s in shapes],
            ["arrow_rs"],
        )

    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(rows, fh, indent=2)
    with open(os.path.join(outdir, "summary.md"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print(f"\nsummary.json + summary.md + per-cell logs in {outdir}")


if __name__ == "__main__":
    main()
