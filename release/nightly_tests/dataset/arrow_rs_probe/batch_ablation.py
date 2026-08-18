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

Usage:
  python batch_ablation.py --fixtures-root DIR [--scale 0.25]
      [--shapes tensors_dict,tensors_cp,fat_col,auto_rg]
      [--policies pa,floor32,floor128,floor512,floor2048,decoded]
Missing fixtures are generated (gen_local_fixtures.py) automatically.
Results: table on stdout + ablation.json under --out (default CWD).
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

DEFAULT_SHAPES = ["tensors_dict", "tensors_cp", "fat_col", "auto_rg"]
DEFAULT_POLICIES = ["pa", "floor32", "floor128", "floor512", "floor2048", "decoded"]
# Shapes whose files carry cloudpickle tensor metadata (need the autoload flag
# and the reader's skip+realign path).
_TENSOR_SHAPES = {"tensors_dict", "tensors_cp"}


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
            it = _pa_batches(f, request)
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

    results = []
    for shape in shapes:
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
            ]
            print(f"== {shape} / {policy}", flush=True)
            out = subprocess.run(
                cmd, env=env, cwd=_HERE, capture_output=True, text=True
            )
            line = next(
                (ln for ln in out.stdout.splitlines() if ln.startswith("CELL_JSON ")),
                None,
            )
            if line is None:
                print(out.stdout[-2000:])
                print(out.stderr[-2000:])
                raise SystemExit(f"cell failed: {shape}/{policy}")
            rec = json.loads(line[len("CELL_JSON ") :])
            rec["expansion"] = manifest[shape].get("enc_to_dec_ratio")
            results.append(rec)
            print(f"   {rec}", flush=True)

    out_path = os.path.join(a.out, "ablation.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=2)

    budget_mib = 32  # display only; cells report actuals
    print(f"\n== ablation summary (budget ~{budget_mib} MiB; R vs pa row) ==")
    for shape in shapes:
        rows = [r for r in results if r["shape"] == shape]
        pa_row = next((r for r in rows if r["policy"] == "pa"), None)
        exp = rows[0].get("expansion")
        print(f"\n[{shape}] enc->dec expansion: {exp}")
        hdr = f"{'policy':>10} {'request':>8} {'maxbatch':>9} {'p50batch':>9} {'peakRSS':>8} {'wall':>6}  R_rss  R_wall"
        print(hdr)
        for r in rows:
            rr = rw = ""
            if pa_row and r is not pa_row:
                rr = f"{r['peak_rss_mib'] / pa_row['peak_rss_mib']:.2f}"
                rw = f"{r['wall_s'] / max(0.01, pa_row['wall_s']):.2f}"
            print(
                f"{r['policy']:>10} {r['request']:>8} {r['batch_mib_max']:>8.1f}M "
                f"{r['batch_mib_p50']:>8.1f}M {r['peak_rss_mib']:>7.0f}M "
                f"{r['wall_s']:>5.1f}s  {rr:>5} {rw:>6}"
            )
    print(f"\nresults -> {out_path}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd")

    c = sub.add_parser("cell", help="internal: one cell in a fresh process")
    c.add_argument("--shape", required=True)
    c.add_argument("--policy", required=True)
    c.add_argument("--path", required=True)

    a_ = p.add_argument
    a_("--fixtures-root", default=os.environ.get("FIXTURES_ROOT", ""))
    a_("--scale", type=float, default=0.25)
    a_("--shapes", default=",".join(DEFAULT_SHAPES))
    a_("--policies", default=",".join(DEFAULT_POLICIES))
    a_("--out", default=".")

    a = p.parse_args()
    if a.cmd == "cell":
        run_cell(a)
    else:
        assert a.fixtures_root, "--fixtures-root (or FIXTURES_ROOT) required"
        orchestrate(a)


if __name__ == "__main__":
    main()
