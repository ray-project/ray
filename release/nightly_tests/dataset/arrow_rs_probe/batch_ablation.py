"""Batch-sizing ablation across the release loss shapes (M43 / item 1y).

WHAT THIS ANSWERS
-----------------
A/B #4 measured arrow-rs's largest yielded table at 288 MiB on a 32 MiB decode
budget for `wide_schema tensors` (M39). The mechanism (M43, verified standalone
2026-08-18): both batch-sizing layers divide by ENCODED footer bytes/row —

  * Python request:  ceil(budget / (enc_bpr * 5)) then max(_, 2048)
    (`_estimate_batch_size_from_*`, PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT=5,
    `_ARROW_RS_MIN_DECODE_BATCH_ROWS=2048`)
  * crate clamp:     byte_budget_rows = budget / enc_bpr, upper-clamped by the
    request (`byte_budget_rows`, src/lib.rs)

so on data that dictionary-encodes r×, the decoded batch lands at ~budget×r
(release tensors: r≈9 → 288 MiB). The 2048 floor is an AMPLIFIER on top (the
un-floored request would be ~410 rows ≈ 57 MiB), not the root.

This probe ablates the request policy — the one lever that upper-clamps the
crate — across multiple shapes, per the user's ask ("ablational test on those
values on multiple test cases like the ones we saw"):

  policies (rs):
    floor32 / floor128 / floor512 / floor2048   request = max(est5, N)
                                                 (floor2048 = shipping behavior)
    decoded                                      request = budget / MEASURED
                                                 decoded bytes-per-row (the fix
                                                 candidate: decoded-aware sizing)
  reference:
    pa                                           PyArrow fragment scanner at the
                                                 shipping estimate (its peak is
                                                 whole-decoded-row-group-bound
                                                 regardless — C9)

  shapes (all local fixtures, gen_local_fixtures.py):
    tensors_dict   release-faithful ~9.4x dictionary expansion — the M39 shape
    tensors_cp     same schema, ~1.1x expansion (control: budget should hold)
    fat_col        one fat utf8 column, ~256 KiB/row (T13's shape)
    auto_rg        thin rows (control: floor is irrelevant, est5 >> 2048)

Per cell (fresh subprocess): wall, peak RSS (ru_maxrss), yielded-batch dist
(max/p50 MiB, max rows), request used. Verdict logic:

  * tensors_dict floor2048 max-batch >> budget AND decoded max-batch ≈ budget
    ⇒ M43 confirmed end-to-end; decoded-aware sizing is the fix.
  * peak RSS should track max-batch on rs; pa's stays row-group-bound.
  * auto_rg rows/wall across floors shows what the floor actually buys on the
    thin shapes it was added for (T22: fewer batches was −34% wall there).

V2 (2026-08-18, "bigger and newer" per review): the matrix is now
shapes x policies x BUDGETS, the shapes cover an expansion SWEEP
(tensors_cp ~1.1x -> tensors_lo ~2-3x -> tensors_dict ~9.4x -> tensors_hi ~15x+)
plus the structural shapes (fat_col, auto_rg, wide, tiny_rgs, single_rg_files,
lone_big_rg), and the DECISION VARIABLES are outcome gates rather than raw
numbers:

  G1 overshoot   max yielded batch bytes / decode budget. The mechanism gate:
                 shipping (floor2048) overshoot ~= expansion ratio on dict
                 shapes (M43); a fix must hold overshoot <= 1.5 on EVERY shape.
  G2 memory      peak RSS <= 1.10x the pa reference cell.
  G3 wall        <= 1.25x pa, with pa decoding SINGLE-THREADED (use_threads=
                 False): the crate side is K=1 and in-Ray both arms take their
                 parallelism from tasks, so the gate compares CPU cost. A
                 threaded pa baseline fails every cell by ~cores (M35's
                 standalone artifact — bit the first Linux run). NB the static `decoded` policy is EXPECTED to
                 fail G3 on the 5000-col tensor shapes (per-batch realign cost,
                 T22/T23) — that failure is the argument that the real fix must
                 be crate-side mid-stream adaptation (size from the first
                 yielded batch, like pa's parquet_file_reader.py:385 refinement),
                 not a uniformly smaller static request.
  G4 rows        row-count parity with the pa cell (cheap correctness gate).

A candidate code change passes this suite when its policy row passes all four
gates on all shapes at every budget. Standalone-only by design: batch sizing is
transport-independent (same math local and S3); the in-Ray and S3 legs of the
same shapes live in loss_triage.py and the retention leg in soak_probe.py —
run_all.sh chains all of them.

Usage:
  python batch_ablation.py --fixtures-root DIR [--scale 0.25]
      [--shapes ...] [--policies pa,floor32,floor128,floor512,floor2048,decoded]
      [--budgets-mib 32] [--out .]
Missing fixtures are generated (gen_local_fixtures.py) automatically.
Results: table + gate verdict on stdout, ablation.json under --out.
"""

import argparse
import glob
import json
import math
import os
import resource
import subprocess
import sys
import time

MiB = 1024 * 1024
# ru_maxrss is KiB on Linux, bytes on macOS.
_RU_UNIT = 1024 if sys.platform.startswith("linux") else 1

_HERE = os.path.dirname(os.path.abspath(__file__))

DEFAULT_SHAPES = [
    # expansion sweep (the M43 axis), low -> high
    "tensors_cp",
    "tensors_lo",
    "tensors_dict",
    "tensors_hi",
    # structural shapes
    "fat_col",
    "auto_rg",
    "wide",
    "tiny_rgs",
    "single_rg_files",
    "lone_big_rg",
]
DEFAULT_POLICIES = ["pa", "floor32", "floor128", "floor512", "floor2048", "decoded"]
# Shapes whose files carry cloudpickle tensor metadata (need the autoload flag
# and the reader's skip+realign path).
_TENSOR_SHAPES = {"tensors_dict", "tensors_cp", "tensors_lo", "tensors_hi"}


def _enc_bpr(files):
    """Encoded (footer) bytes per row across the fixture — what BOTH shipping
    sizing layers divide by."""
    import pyarrow.parquet as pq

    enc = rows = 0
    for f in files:
        md = pq.read_metadata(f)
        enc += sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups))
        rows += md.num_rows
    return max(1, enc // max(1, rows)), rows


def _est5(files, budget):
    """Mirror the shipping request estimate BEFORE the floor:
    ceil(budget / (enc_bpr * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT)),
    capped at the row count like `_estimate_batch_size_from_*` caps at the
    row-group/chunk rows."""
    bpr, rows = _enc_bpr(files)
    return min(math.ceil(budget / (bpr * 5)), rows)


def _decoded_request(f, knobs, realign_fields):
    """The fix candidate: measure DECODED bytes/row from one small probe batch,
    then request budget/dec_bpr. (In-reader this would be the adaptive
    refinement the PyArrow path already has.)"""
    from loss_triage import _rs_batches

    for t in _rs_batches(f, 64, knobs, realign_fields=realign_fields):
        dec_bpr = max(1, t.nbytes // max(1, t.num_rows))
        return max(32, int(knobs["budget"] // dec_bpr))
    return 32


def run_cell(a):
    from loss_triage import _consume, _pa_batches, _reader_knobs, _rs_batches

    files = sorted(glob.glob(os.path.join(a.path, "*.parquet")))
    assert files, f"no parquet under {a.path}"
    knobs = _reader_knobs()
    if a.budget_mib:
        knobs["budget"] = a.budget_mib * MiB

    realign_fields = None
    if a.shape in _TENSOR_SHAPES:
        import pyarrow.parquet as pq

        realign_fields = list(pq.read_schema(files[0]))

    est5 = _est5(files, knobs["budget"])
    if a.policy == "pa":
        request = est5
    elif a.policy == "decoded":
        request = _decoded_request(files[0], knobs, realign_fields)
    else:
        request = max(est5, int(a.policy.removeprefix("floor")))

    batch_rows, batch_mib = [], []

    def observed(it):
        for t in it:
            batch_rows.append(t.num_rows)
            batch_mib.append(t.nbytes / MiB)
            yield t

    t0 = time.monotonic()
    rows = 0
    for f in files:
        if a.policy == "pa":
            # Single-threaded pa: G3 compares CPU cost, not thread-pool
            # fan-out — the crate side is K=1 and in-Ray both arms get their
            # parallelism from tasks (M35). Threaded pa here made every wall
            # gate fail by ~cores on Linux.
            it = _pa_batches(f, request, use_threads=False)
        else:
            it = _rs_batches(f, request, knobs, realign_fields=realign_fields)
        rows += _consume(observed(it), "decode", None)
    wall = time.monotonic() - t0
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * _RU_UNIT

    srt = sorted(batch_mib)
    print(
        "CELL_JSON "
        + json.dumps(
            dict(
                shape=a.shape,
                policy=a.policy,
                budget_mib=knobs["budget"] // MiB,
                overshoot=round(max(batch_mib) / (knobs["budget"] / MiB), 2)
                if batch_mib
                else 0,
                request=request,
                est5=est5,
                wall_s=round(wall, 2),
                peak_rss_mib=round(peak / MiB, 1),
                rows=rows,
                n_batches=len(batch_mib),
                batch_mib_max=round(max(batch_mib), 1) if batch_mib else 0,
                batch_mib_p50=round(srt[len(srt) // 2], 1) if srt else 0,
                batch_rows_max=max(batch_rows) if batch_rows else 0,
            )
        )
    )


def orchestrate(a):
    shapes = a.shapes.split(",")
    policies = a.policies.split(",")

    # Fixtures: generate any missing shape (version/scale-gated skip inside).
    subprocess.run(
        [
            sys.executable,
            os.path.join(_HERE, "gen_local_fixtures.py"),
            "--root",
            a.fixtures_root,
            "--shapes",
            ",".join(shapes),
            "--scale",
            str(a.scale),
        ],
        check=True,
    )
    with open(os.path.join(a.fixtures_root, "manifest.json")) as fh:
        manifest = json.load(fh)

    budgets = [int(b) for b in str(a.budgets_mib).split(",")]
    results = []
    for shape in shapes:
        for budget in budgets:
            for policy in policies:
                env = dict(os.environ)
                if shape in _TENSOR_SHAPES:
                    env["RAY_DATA_AUTOLOAD_CLOUDPICKLE_TENSOR_METADATA"] = "1"
                cmd = [
                    sys.executable,
                    os.path.abspath(__file__),
                    "cell",
                    "--shape",
                    shape,
                    "--policy",
                    policy,
                    "--path",
                    manifest[shape]["path"],
                    "--budget-mib",
                    str(budget),
                ]
                print(f"== {shape} / {policy} / {budget}MiB", flush=True)
                out = subprocess.run(
                    cmd, env=env, cwd=_HERE, capture_output=True, text=True
                )
                line = next(
                    (
                        ln
                        for ln in out.stdout.splitlines()
                        if ln.startswith("CELL_JSON ")
                    ),
                    None,
                )
                if line is None:
                    print(out.stdout[-2000:])
                    print(out.stderr[-2000:])
                    raise SystemExit(f"cell failed: {shape}/{policy}/{budget}")
                rec = json.loads(line[len("CELL_JSON ") :])
                rec["expansion"] = manifest[shape].get("enc_to_dec_ratio")
                results.append(rec)
                print(f"   {rec}", flush=True)

    out_path = os.path.join(a.out, "ablation.json")

    # Gate thresholds (the suite's decision variables — see docstring).
    G_OVERSHOOT, G_RSS, G_WALL = 1.5, 1.10, 1.25
    verdict = {}
    print("\n== ablation summary (R vs the pa row at the same budget) ==")
    for shape in shapes:
        for budget in budgets:
            rows = [
                r for r in results if r["shape"] == shape and r["budget_mib"] == budget
            ]
            if not rows:
                continue
            pa_row = next((r for r in rows if r["policy"] == "pa"), None)
            exp = rows[0].get("expansion")
            print(f"\n[{shape} @ {budget} MiB] enc->dec expansion: {exp}")
            hdr = (
                f"{'policy':>10} {'request':>8} {'maxbatch':>9} {'over':>6} "
                f"{'p50batch':>9} {'peakRSS':>8} {'wall':>6} {'rows':>9}"
                "  R_rss  R_wall  gates"
            )
            print(hdr)
            for r in rows:
                rr = rw = gates = ""
                if pa_row and r is not pa_row:
                    r_rss = r["peak_rss_mib"] / pa_row["peak_rss_mib"]
                    r_wall = r["wall_s"] / max(0.01, pa_row["wall_s"])
                    rr, rw = f"{r_rss:.2f}", f"{r_wall:.2f}"
                    g = [
                        "G1" if r["overshoot"] <= G_OVERSHOOT else "g1!",
                        "G2" if r_rss <= G_RSS else "g2!",
                        "G3" if r_wall <= G_WALL else "g3!",
                        "G4" if r["rows"] == pa_row["rows"] else "g4!",
                    ]
                    gates = " ".join(g)
                    verdict[f"{shape}@{budget}/{r['policy']}"] = dict(
                        overshoot=r["overshoot"],
                        r_rss=round(r_rss, 2),
                        r_wall=round(r_wall, 2),
                        rows_match=r["rows"] == pa_row["rows"],
                        passed=not any(x.endswith("!") for x in g),
                    )
                print(
                    f"{r['policy']:>10} {r['request']:>8} {r['batch_mib_max']:>8.1f}M "
                    f"{r['overshoot']:>6.2f} {r['batch_mib_p50']:>8.1f}M "
                    f"{r['peak_rss_mib']:>7.0f}M {r['wall_s']:>5.1f}s "
                    f"{r['rows']:>9}  {rr:>5} {rw:>6}  {gates}"
                )

    with open(out_path, "w") as fh:
        json.dump({"cells": results, "verdict": verdict}, fh, indent=2)
    fails = {k: v for k, v in verdict.items() if not v["passed"]}
    print(
        f"\n== gate verdict: {len(verdict) - len(fails)}/{len(verdict)} "
        f"policy-cells pass (G1 overshoot<={G_OVERSHOOT} G2 R_rss<={G_RSS} "
        f"G3 R_wall<={G_WALL} G4 rows==pa) =="
    )
    for k, v in sorted(fails.items()):
        print(f"  FAIL {k}: {v}")
    print(f"\nresults -> {out_path}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("cell", help="internal: one cell in a fresh process")
    c.add_argument("--shape", required=True)
    c.add_argument("--policy", required=True)
    c.add_argument("--path", required=True)
    c.add_argument("--budget-mib", type=int, default=0)

    a_ = p.add_argument
    a_("--fixtures-root", default=os.environ.get("FIXTURES_ROOT", ""))
    a_("--scale", type=float, default=0.25)
    a_("--shapes", default=",".join(DEFAULT_SHAPES))
    a_("--policies", default=",".join(DEFAULT_POLICIES))
    a_("--budgets-mib", default="32", help="comma list, e.g. 16,32,128")
    a_("--out", default=".")

    a = p.parse_args()
    if a.cmd == "cell":
        run_cell(a)
    else:
        assert a.fixtures_root, "--fixtures-root (or FIXTURES_ROOT) required"
        orchestrate(a)


if __name__ == "__main__":
    main()
