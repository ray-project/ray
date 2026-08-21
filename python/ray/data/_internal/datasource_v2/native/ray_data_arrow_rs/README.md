# `ray_data_arrow_rs` — experimental arrow-rs Parquet reader

Native PyO3 extension backing `ArrowRsParquetFileReader`
(`_internal/datasource_v2/readers/arrow_rs_parquet_file_reader.py`). Selected at
runtime by `DataContext.use_arrow_rs_parquet_reader` (only under
`use_datasource_v2=True`).

## Status

This source is a **reconstruction** from the surviving standalone benchmark
(`main.rs`). A prebuilt macOS/cpython-3.12 `.so` (v0.1.0) is already installed in
the dev venv and is the behavioral source of truth for local (macOS) work — the
integration and benchmark run against it today. This crate exists so the reader
can be:

1. **rebuilt for Linux/x86-64** (the "deciding experiment" in `Agents.md` §7 must
   run on Ray's Linux runtime with USS metrics + real S3), and
2. **evolved** to expose the tuning knobs the standalone benchmark had but the
   v0.1.0 API doesn't (K intra-row-group split, fetch window, byte budget).

It has **not** been compiled in-session. Before relying on it, build and validate:

```bash
cd python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs
maturin develop --release            # installs ray_data_arrow_rs into the venv
pytest ../../../../tests/datasource/test_arrow_rs_parquet_reader.py -v
```

If the parity tests pass, the reconstruction matches PyArrow. Then run the
benchmark (`release/nightly_tests/dataset/arrow_rs_read_benchmark.py`).

## API

```
read_row_groups(path, row_groups=None, columns=None, batch_size=131072)
read_row_groups_s3(bucket, key, region, anonymous,
                   row_groups=None, columns=None, batch_size=131072)
```

Both return an object implementing `__arrow_c_stream__`, consumed on the Python
side with `pa.RecordBatchReader.from_stream(...)`.

## Open items for the S3/Linux phase

- **Expose the knobs** (`decode_budget_bytes`, `k`, `fetch_window_mb`) and port
  `build_units` / `read_unit_windowed` / `read_all_async` from `main.rs` so a
  single big row group splits into K parallel range reads — the mechanism
  `Agents.md` credits for the 4–5× S3 speedup. The current `read_row_groups`
  does a single streaming pass (memory win only, no intra-fragment K).
- **Un-gate S3** in `_arrow_rs_supported` once `read_row_groups_s3` is validated.
- **Reconcile FFI/dep versions** on first build (arrow/parquet 59, object_store
  0.13, pyo3 0.22 — adjust to whatever resolves).
- **Avoid double parallelism**: with intra-fragment K in the crate, set
  `RAY_DATA_READ_FILES_NUM_THREADS=1` for the arrow-rs path (see the reader).
