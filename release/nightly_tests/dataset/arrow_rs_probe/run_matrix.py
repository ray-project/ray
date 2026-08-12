#!/usr/bin/env python3
"""Run the full arrow_rs-vs-PyArrow A/B matrix against S3, log everything, tabulate.

One place to reproduce both release regressions instead of typing the README's A/B/C
blocks by hand. Each cell runs read_probe.py in its OWN process (Ray + the crate load
once per process, and the mem sampler must not see a stale Ray instance), captures full
stdout+stderr to a per-cell log file, parses the RESULT block, and prints a comparison
table with arrow_rs/pyarrow ratios so you can see at a glance what is slow / heavy.

The matrix:
  [diag]  concurrency=1, both layouts, both readers  -> cpu_over_wall
            imagenet ~1 => CPU-bound decode ; <<1 => I/O-waiting on S3 (prefetch is the fix)
  [mem]   wide fanned out, both readers               -> peak_uss_gb / read_avg_max_uss_gb
            the memory metric of record (Linux USS). expect arrow_rs at-or-below PyArrow.
  [alloc] wide arrow_rs, allocator variants           -> is any residual gap glibc arenas?
            baseline vs MALLOC_ARENA_MAX=2 vs LD_PRELOAD jemalloc (skipped if .so absent).

  python run_matrix.py --wide-path s3://.../wide_schema/primitives \\
                       --imagenet-path s3://.../imagenet/parquet
  python run_matrix.py --wide-path ... --imagenet-path ... --skip alloc   # subset
  python run_matrix.py --wide-path ... --imagenet-path ... --repeat 3     # median of N

Assumes: venv active, RAY_ADDRESS=local, AWS creds/region exported, box in bucket region.
"""
import argparse
import glob
import json
import os
import subprocess
import sys
import time

PROBE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "read_probe.py")
PY = sys.executable

# Common jemalloc locations on Debian/Ubuntu (Anyscale base image).
_JEMALLOC_GLOBS = [
    "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
    "/usr/lib/x86_64-linux-gnu/libjemalloc.so",
    "/usr/lib/libjemalloc.so.2",
]


def find_jemalloc():
    for g in _JEMALLOC_GLOBS:
        hits = glob.glob(g)
        if hits:
            return hits[0]
    return None


def run_cell(
    logdir, name, path, reader, concurrency, columns, extra_env, extra_args=None
):
    """Run one read_probe.py invocation; tee output to a log; return parsed RESULT dict."""
    cmd = [PY, PROBE, "--path", path, "--reader", reader]
    if concurrency is not None:
        cmd += ["--concurrency", str(concurrency)]
    if columns:
        cmd += ["--columns", *columns]
    if extra_args:
        cmd += list(extra_args)

    env = dict(os.environ)
    env.update(extra_env)

    logpath = os.path.join(logdir, f"{name}.log")
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    dur = time.perf_counter() - t0

    with open(logpath, "w") as fh:
        fh.write(f"# cmd: {' '.join(cmd)}\n")
        fh.write(f"# extra_env: {extra_env}\n")
        fh.write(f"# wall_including_startup_s: {dur:.1f}\n")
        fh.write("# ---- STDOUT ----\n")
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
            f"    !! {name} PROBE FAIL rc={proc.returncode} (see {logpath})\n"
            f"       {proc.stderr.strip()[-400:]}",
            flush=True,
        )
    else:
        print(
            f"    {name:<34} wall={res.get('wall_s')} "
            f"cpu/wall={res.get('cpu_over_wall')} "
            f"uss={res.get('peak_uss_gb')} rss={res.get('peak_rss_gb')}",
            flush=True,
        )
    return res


def _num(res, key):
    try:
        return float(res.get(key))
    except (TypeError, ValueError):
        return None


def median_cell(logdir, name, repeat, **kw):
    """Run a cell `repeat` times, return the run whose wall_s is the median."""
    runs = []
    for i in range(repeat):
        tag = name if repeat == 1 else f"{name}.r{i}"
        runs.append(run_cell(logdir, tag, **kw))
    good = [r for r in runs if r]
    if not good:
        return {}
    good.sort(key=lambda r: _num(r, "wall_s") or float("inf"))
    return good[len(good) // 2]


def ratio(a, b):
    if a is None or b is None or b == 0:
        return None
    return round(a / b, 3)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--wide-path", required=True)
    p.add_argument("--imagenet-path", required=True)
    p.add_argument("--imagenet-columns", nargs="+", default=["image", "label"])
    p.add_argument(
        "--outdir", default=None, help="log dir (default ./matrix_runs/<ts>)"
    )
    p.add_argument("--repeat", type=int, default=1, help="runs per cell, report median")
    p.add_argument(
        "--skip",
        default="",
        help="comma list of stages to skip: diag,mem,alloc",
    )
    args = p.parse_args()

    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    ts = time.strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "matrix_runs", ts
    )
    os.makedirs(outdir, exist_ok=True)
    print(f"logs -> {outdir}\n", flush=True)

    rows = {}  # name -> result dict

    def cell(name, **kw):
        rows[name] = median_cell(outdir, name, args.repeat, **kw)

    # -------- [diag] cpu_over_wall @ concurrency=1 --------
    if "diag" not in skip:
        print("=== [diag] cpu_over_wall @ concurrency=1 ===", flush=True)
        for reader in ("pyarrow", "arrow_rs"):
            cell(
                f"diag.imagenet.{reader}",
                path=args.imagenet_path,
                reader=reader,
                concurrency=1,
                columns=args.imagenet_columns,
                extra_env={},
            )
            cell(
                f"diag.wide.{reader}",
                path=args.wide_path,
                reader=reader,
                concurrency=1,
                columns=None,
                extra_env={},
            )

    # -------- [mem] wide fanned out --------
    if "mem" not in skip:
        print("=== [mem] wide fanned out (peak_uss_gb) ===", flush=True)
        for reader in ("pyarrow", "arrow_rs"):
            cell(
                f"mem.wide.{reader}",
                path=args.wide_path,
                reader=reader,
                concurrency=None,
                columns=None,
                extra_env={},
            )

    # -------- [alloc] allocator A/B on wide arrow_rs --------
    if "alloc" not in skip:
        print("=== [alloc] wide arrow_rs allocator A/B ===", flush=True)
        cell(
            "alloc.baseline",
            path=args.wide_path,
            reader="arrow_rs",
            concurrency=None,
            columns=None,
            extra_env={},
        )
        cell(
            "alloc.arena2",
            path=args.wide_path,
            reader="arrow_rs",
            concurrency=None,
            columns=None,
            extra_env={"MALLOC_ARENA_MAX": "2"},
        )
        jem = find_jemalloc()
        if jem:
            cell(
                "alloc.jemalloc",
                path=args.wide_path,
                reader="arrow_rs",
                concurrency=None,
                columns=None,
                extra_env={"LD_PRELOAD": jem},
            )
        else:
            print(
                "    (skip alloc.jemalloc: no libjemalloc.so found; "
                "`apt-get install -y libjemalloc2` to enable)",
                flush=True,
            )

    # -------- summary --------
    print("\n=================== SUMMARY (arrow_rs / pyarrow) ===================")

    def pair(prefix):
        pa_r, ar_r = rows.get(f"{prefix}.pyarrow", {}), rows.get(
            f"{prefix}.arrow_rs", {}
        )
        for metric, label in [
            ("wall_s", "wall_s"),
            ("cpu_over_wall", "cpu/wall"),
            ("peak_uss_gb", "peak_uss_gb"),
            ("read_avg_max_uss_gb", "read_uss_gb"),
            ("peak_rss_gb", "peak_rss_gb"),
        ]:
            a, b = _num(ar_r, metric), _num(pa_r, metric)
            if a is None and b is None:
                continue
            r = ratio(a, b)
            flag = ""
            if r is not None and metric in (
                "wall_s",
                "peak_uss_gb",
                "read_avg_max_uss_gb",
            ):
                flag = "  <-- WORSE" if r > 1.05 else ("  <-- win" if r < 0.95 else "")
            print(
                f"  {prefix:<16} {label:<14} "
                f"pyarrow={b} arrow_rs={a} ratio={r}{flag}"
            )

    if "diag" not in skip:
        print(
            "[diag imagenet]  cpu/wall<<1 on the S3 read => I/O-bound (prefetch is the fix)"
        )
        pair("diag.imagenet")
        print("[diag wide]")
        pair("diag.wide")
    if "mem" not in skip:
        print("[mem wide]  the metric of record: peak_uss_gb / read_uss_gb")
        pair("mem.wide")
    if "alloc" not in skip:
        print("[alloc wide arrow_rs]  is residual mem gap glibc arena retention?")
        base = _num(rows.get("alloc.baseline", {}), "peak_uss_gb")
        for variant in ("alloc.arena2", "alloc.jemalloc"):
            v = _num(rows.get(variant, {}), "peak_uss_gb")
            if v is not None:
                print(
                    f"  {variant:<16} peak_uss_gb={v} "
                    f"vs baseline={base} ratio={ratio(v, base)}"
                )

    with open(os.path.join(outdir, "summary.json"), "w") as fh:
        json.dump(rows, fh, indent=2)
    print(f"\nfull JSON + per-cell logs in {outdir}")


if __name__ == "__main__":
    main()
