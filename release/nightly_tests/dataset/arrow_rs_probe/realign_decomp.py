"""Decompose the M51 wall loss (wide x high-expansion shapes): WHERE does the
per-batch cost live? (M52 on macOS; this script = the Linux confirmation.)

Four experiments on the tensors_dict fixture, all standalone (no Ray, no S3 --
transport is already acquitted: the loss reproduces in zero-IO local cells and
the S3 ratio is BETTER than local):

  1. layers   rs read timed in 4 layers x decode budgets:
                A = crate decode + FFI import only (discard batches)
                B = A + pa.Table.from_batches wrap
                C = B + _cast_table_to extension realign  (= the OLD path)
                D = crate schema override + zero-copy FFI relabel + wrap
                    (= the M53 FIX: with_schema_override makes the crate decode
                    the extension's storage layout, then each batch is re-typed
                    through the C Data Interface against a prebuilt extension
                    schema object -- no cast, no per-batch pickle deserialize)
              If A ~beats pa and C carries the loss, the decoder is innocent;
              D should sit on top of B (realign cost ~gone). D asserts value
              equality against C's result before timing.
  2. pa-ctrl  pyarrow forced to the SAME batch counts (batch_size in rows).
              pa emits extension-typed batches straight from C++ (it parses
              ARROW:schema once per file), so its per-batch cost is the
              C++ floor for constructing a 5000-col batch.
  3. variants one decoded batch: rebuild-schema+cast vs cached-schema+cast vs
              zero-copy Table.from_arrays re-wrap. If none differ, the cost is
              per-column construction churn, not the cast kernels.
  4. cols     cast cost vs column count (5/50/500/all) -> us/col linearity.

Usage:
  python realign_decomp.py [--fixtures-root ~/arrow_rs_repl_fixtures] [--shape tensors_dict]
"""
import argparse
import json
import os
import sys
import time

os.environ.setdefault("RAY_DATA_AUTOLOAD_CLOUDPICKLE_TENSOR_METADATA", "1")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MiB = 1024 * 1024


def _rs_stream(path, budget, knobs, override_schema=None):
    import pyarrow as pa
    import ray_data_arrow_rs as rs

    h = rs.open_parquet_file(path, page_index=False)
    if override_schema is not None:
        # M53: the crate decodes the extension's storage layout directly
        # (large_list offsets), so the per-batch realign is a pure relabel.
        h.with_schema_override(override_schema.__arrow_c_schema__())
    return pa.RecordBatchReader.from_stream(
        h.read_row_groups(
            row_groups=None,
            columns=None,
            batch_size=131072,
            decode_budget_bytes=budget,
            k=knobs["k"],
            split_threshold_bytes=knobs["split"],
            predicate_json=None,
            fetch_window_mb=knobs["window"],
            column_fetch_mb=knobs["column"],
            prefetch_budget_mb=4 * max(knobs["window"], knobs["column"]),
        )
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--fixtures-root", default=os.path.expanduser("~/arrow_rs_repl_fixtures")
    )
    ap.add_argument("--shape", default="tensors_dict")
    ap.add_argument("--budgets-mib", default="32,128,512")
    a = ap.parse_args()

    import pyarrow as pa
    import pyarrow.parquet as pq

    from loss_triage import _pa_batches, _reader_knobs
    from ray.data._internal.datasource_v2.readers.arrow_rs_parquet_file_reader import (
        _cast_table_to,
        _ffi_relabel_batch,
        _storage_override_schema,
    )

    man = json.load(open(os.path.join(a.fixtures_root, "manifest.json")))
    fix = man[a.shape]
    files = sorted(
        os.path.join(fix["path"], f)
        for f in os.listdir(fix["path"])
        if f.endswith(".parquet")
    )
    realign_fields = list(pq.read_schema(files[0]))
    knobs = _reader_knobs()
    budgets = [int(x) * MiB for x in a.budgets_mib.split(",")]
    print(
        f"shape={a.shape}: {fix['rows']} rows x {fix['columns']} cols, "
        f"expansion {fix.get('enc_to_dec_ratio', '?')}x, {len(files)} files"
    )

    # ---- 1. layers ------------------------------------------------------- #
    target_schema = pa.schema(realign_fields)
    import ray_data_arrow_rs as _rs_mod

    inferred = pa.schema(
        _rs_mod.open_parquet_file(files[0], page_index=False).metadata()
    )
    override = _storage_override_schema(inferred, target_schema)

    def run_layer(layer, budget, collect=False):
        t0 = time.perf_counter()
        nb = 0
        out = []
        for f in files:
            if layer == 3:
                # D: override'd decode + zero-copy relabel (the M53 fix).
                for b in _rs_stream(f, budget, knobs, override_schema=override):
                    nb += 1
                    t = pa.Table.from_batches([_ffi_relabel_batch(b, target_schema)])
                    if collect:
                        out.append(t)
                continue
            for b in _rs_stream(f, budget, knobs):
                nb += 1
                if layer >= 1:
                    t = pa.Table.from_batches([b])
                if layer >= 2:
                    t = _cast_table_to(t, realign_fields)
                if collect and layer >= 2:
                    out.append(t)
        return time.perf_counter() - t0, nb, out

    # Correctness first: D's tables must equal C's exactly.
    if override is not None:
        _, _, c_tabs = run_layer(2, budgets[0], collect=True)
        _, _, d_tabs = run_layer(3, budgets[0], collect=True)
        eq = pa.concat_tables(d_tabs).equals(pa.concat_tables(c_tabs))
        print(f"\n[1] D-vs-C equality @ {budgets[0] // MiB}Mi: {eq}")
        assert eq, "M53 fixed path diverges from the cast path"
        del c_tabs, d_tabs
    else:
        print("\n[1] WARNING: no storage override derivable -- D falls back to C")

    print("[1] rs layers (A crate+FFI / B +wrap / C +cast = old / D = M53 fix)")
    print(
        f"{'budget':>8} {'batches':>7} {'A':>7} {'B':>7} {'C':>7} {'D':>7}"
        "  C-B per batch  D-B per batch"
    )
    counts = {}
    for budget in budgets:
        la = min((run_layer(0, budget) for _ in range(3)), key=lambda x: x[0])
        lb = min((run_layer(1, budget) for _ in range(3)), key=lambda x: x[0])
        lc = min((run_layer(2, budget) for _ in range(3)), key=lambda x: x[0])
        ld = min((run_layer(3, budget) for _ in range(3)), key=lambda x: x[0])
        nb = lc[1]
        counts[budget] = nb
        print(
            f"{budget // MiB:>6}Mi {nb:>7} {la[0]:>6.2f}s {lb[0]:>6.2f}s "
            f"{lc[0]:>6.2f}s {ld[0]:>6.2f}s "
            f"{(lc[0] - lb[0]) / nb * 1000:7.1f} ms {(ld[0] - lb[0]) / nb * 1000:8.1f} ms"
        )

    # ---- 2. pa control at matched batch counts --------------------------- #
    print(
        "\n[2] pa control (single-thread, batch_size chosen to match rs batch counts)"
    )
    total_rows = fix["rows"]
    for budget in budgets:
        nb = counts[budget]
        bs = max(1, total_rows // nb)

        def run_pa():
            t0 = time.perf_counter()
            n = 0
            typ = None
            for f in files:
                for t in _pa_batches(f, bs, use_threads=False):
                    n += 1
                    if typ is None:
                        typ = str(t.schema.field(0).type)
            return time.perf_counter() - t0, n, typ

        w, n, typ = min((run_pa() for _ in range(3)), key=lambda x: x[0])
        print(f"  bs={bs:>6} rows: {w:5.2f}s  batches={n:>3}  col0={typ[:36]}")

    # ---- 3. cast variants on one batch ----------------------------------- #
    print(
        "\n[3] cast variants, one decoded batch @32Mi (per-batch ms; equal => churn, not kernels)"
    )
    b = next(iter(_rs_stream(files[0], 32 * MiB, knobs)))
    t = pa.Table.from_batches([b])
    cached = pa.schema(realign_fields, metadata=t.schema.metadata)

    def timeit(fn, n=10):
        fn()
        t0 = time.perf_counter()
        for _ in range(n):
            fn()
        return (time.perf_counter() - t0) / n * 1000

    print(
        f"  rebuild schema + cast : {timeit(lambda: t.cast(pa.schema(realign_fields, metadata=t.schema.metadata))):6.1f} ms"
    )
    print(f"  cached schema + cast  : {timeit(lambda: t.cast(cached)):6.1f} ms")
    print(
        f"  zero-copy from_arrays : {timeit(lambda: pa.Table.from_arrays(t.columns, schema=cached)):6.1f} ms"
    )

    # ---- 4. column linearity ---------------------------------------------- #
    print("\n[4] cast cost vs column count (expect ~linear us/col)")
    ncols_all = len(realign_fields)
    for ncols in [5, 50, 500, ncols_all]:
        sub = t.select(range(ncols))
        fields = realign_fields[:ncols]
        ms = timeit(lambda: sub.cast(pa.schema(fields, metadata=sub.schema.metadata)))
        print(
            f"  {ncols:>5} cols: {ms:8.2f} ms/batch  ({ms / ncols * 1000:5.1f} us/col)"
        )


if __name__ == "__main__":
    main()
