#!/usr/bin/env python3
"""M39: is wide_schema tensors' bigger peak batch representation or batch sizing?

A/B #4 measured, at byte-identical per-task decoded bytes (ratio 1.00), an
arrow-rs largest-yielded-table of 288 MiB vs PyArrow's 236 (peak_batch 1.22x,
findings M39) on the 5000-tensor-column shape - the only loss whose decoder
distributions did NOT match. Two non-defect explanations, distinguishable
standalone with no Ray:

  (a) batch SIZING - the reader's floor (min 2048 rows) or its batch math makes
      arrow-rs yield fewer, bigger tables: same bytes/row, more rows per batch;
  (b) REPRESENTATION - the realigned tables genuinely carry more bytes per row
      (offset widths, non-shared validity buffers, per-chunk overhead), which
      would also inflate every downstream block and could explain the shape's
      worker-USS loss (M33) where retention doesn't.

Decodes the tensors_cp fixture through both readers exactly as loss_triage.py
does (same computed batch_size for both; the rs leg realigns to the extension
schema via the reader's own `_cast_table_to`, so tables are what do_read would
see), then compares rows/batch and bytes/row per batch plus a per-arrow-type
breakdown of the first batch. Equal bytes/row with bigger rs batches => (a),
close M39 as benign sizing. A bytes/row gap => (b), and the type table says
which column class carries it.

Representation is platform-independent - run anywhere the crate imports:

  python tensors_nbytes_probe.py --fixture-root ~/arrow_rs_repl_fixtures
"""
import argparse
import json
import os
from collections import defaultdict

# Must be set before ray.data is imported (loss_triage SHAPE_ENV, same rule).
os.environ.setdefault("RAY_DATA_AUTOLOAD_CLOUDPICKLE_TENSOR_METADATA", "1")

MiB = 1024 * 1024


def _batches(reader, path, batch_size, realign_fields):
    from loss_triage import _pa_batches, _reader_knobs, _rs_batches

    if reader == "pa":
        return _pa_batches(path, batch_size)
    return _rs_batches(path, batch_size, _reader_knobs(), realign_fields)


def _describe(tables):
    """Per-batch (rows, nbytes) + per-type nbytes breakdown of the 1st batch."""
    batches = []
    by_type = None
    for t in tables:
        batches.append((t.num_rows, t.nbytes))
        if by_type is None:
            by_type = defaultdict(lambda: [0, 0])  # type -> [n_cols, nbytes]
            for name, col in zip(t.schema.names, t.columns):
                key = str(t.schema.field(name).type)
                by_type[key][0] += 1
                by_type[key][1] += col.nbytes
    return batches, dict(sorted(by_type.items())) if by_type else {}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fixture-root", required=True)
    p.add_argument("--max-batches", type=int, default=None)
    args = p.parse_args()

    import pyarrow.parquet as pq
    import ray.data  # noqa: F401  (registers the tensor extension types)

    from loss_triage import _batch_size, _reader_knobs

    root = os.path.expanduser(args.fixture_root)
    with open(os.path.join(root, "manifest.json")) as fh:
        entry = json.load(fh)["tensors_cp"]
    fdir = entry["path"] if isinstance(entry, dict) else entry
    path = sorted(
        os.path.join(fdir, f) for f in os.listdir(fdir) if f.endswith(".parquet")
    )[0]

    md = pq.read_metadata(path)
    bs = _batch_size(md, _reader_knobs())
    realign = list(pq.read_schema(path))
    print(f"file={path} rows={md.num_rows} computed batch_size={bs}\n")

    out = {}
    for reader in ("pa", "rs"):
        it = _batches(reader, path, bs, realign if reader == "rs" else None)
        if args.max_batches:
            it = (t for i, t in enumerate(it) if i < args.max_batches)
        batches, by_type = _describe(it)
        rows = sum(r for r, _ in batches)
        nbytes = sum(b for _, b in batches)
        out[reader] = dict(batches=batches, by_type=by_type, rows=rows, nbytes=nbytes)
        print(f"--- {reader} ---")
        print(
            f"  batches: {len(batches)}  total rows: {rows}  total MiB: {nbytes / MiB:.1f}"
        )
        print(f"  bytes/row overall: {nbytes / rows:.1f}")
        print(
            f"  per-batch (rows, MiB): {[(r, round(b / MiB, 1)) for r, b in batches]}"
        )
        print(f"  max batch MiB: {max(b for _, b in batches) / MiB:.1f}\n")

    pa_r, rs_r = out["pa"], out["rs"]
    bpr_pa = pa_r["nbytes"] / pa_r["rows"]
    bpr_rs = rs_r["nbytes"] / rs_r["rows"]
    print("=== VERDICT INPUTS ===")
    print(f"bytes/row      pa={bpr_pa:.1f}  rs={bpr_rs:.1f}  R={bpr_rs / bpr_pa:.3f}")
    print(
        f"max batch MiB  pa={max(b for _, b in pa_r['batches']) / MiB:.1f}  "
        f"rs={max(b for _, b in rs_r['batches']) / MiB:.1f}"
    )
    print(
        "R~1.00 bytes/row with bigger rs batches => M39 is batch SIZING (benign);\n"
        "R>1.05 => REPRESENTATION - see the per-type rows below for the carrier."
    )
    print("\n=== PER-TYPE (first batch; type: n_cols, MiB, R) ===")
    keys = sorted(set(pa_r["by_type"]) | set(rs_r["by_type"]))
    for k in keys:
        pn, pb = pa_r["by_type"].get(k, (0, 0))
        rn, rb = rs_r["by_type"].get(k, (0, 0))
        r = round(rb / pb, 3) if pb else None
        print(
            f"  {k[:70]:<70} pa=({pn}, {pb / MiB:.1f})  rs=({rn}, {rb / MiB:.1f})  R={r}"
        )

    print("\n=== RESULT ===")
    print(
        json.dumps(
            dict(
                bytes_per_row_pa=round(bpr_pa, 1),
                bytes_per_row_rs=round(bpr_rs, 1),
                bytes_per_row_ratio=round(bpr_rs / bpr_pa, 4),
                max_batch_mib_pa=round(max(b for _, b in pa_r["batches"]) / MiB, 1),
                max_batch_mib_rs=round(max(b for _, b in rs_r["batches"]) / MiB, 1),
                n_batches_pa=len(pa_r["batches"]),
                n_batches_rs=len(rs_r["batches"]),
            )
        )
    )


if __name__ == "__main__":
    main()
