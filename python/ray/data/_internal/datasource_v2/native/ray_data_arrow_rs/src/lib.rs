//! Experimental arrow-rs Parquet reader for Ray Data (PyO3 extension).
//!
//! RECONSTRUCTION STATUS
//! ---------------------
//! Reconstructed from the surviving standalone benchmark (`main.rs`). The local
//! path (`read_row_groups`) ports two of `main.rs`'s modes:
//!
//! 1. **Byte-budgeted per-group streaming** (`row_group_loop_bb`, threads==1):
//!    read one row group at a time with a batch size computed *by bytes* from the
//!    footer (`byte_budget_rows`), so the decoded working set stays flat across
//!    schemas (wide-string groups get few rows/batch, numeric groups many). A
//!    single reader streams each group in file order and drops each batch, so peak
//!    memory ~= one budget while row order is preserved.
//!
//! 2. **Intra-fragment K-split** (`build_units` / `read_range_fixed`): when a call
//!    covers a *single* row group larger than `split_threshold_bytes`, split its
//!    rows into K contiguous ranges decoded by K threads and merge them back in
//!    range order (`ParallelRangeReader`). This is the case Ray can't parallelize
//!    (a big row group is a lone fragment → thread pool of 1), so PyArrow decodes
//!    it ~single-threaded; K gives us parallel decode without regressing speed.
//!    Multi-row-group / small-row-group calls use path 1 (K=1) because Ray's
//!    fragment thread pool already parallelizes those — so the two parallelism
//!    layers never multiply.
//!
//! Unlike `main.rs` (which only sums a commutative checksum, so range order is
//! irrelevant), we return real data, so the K-split merge is strictly order
//! preserving: one bounded channel per range, drained in range order.
//!
//! The S3 path (`read_row_groups_s3`) ports `main.rs`'s windowed-async reader
//! (`read_all_async` / `read_unit_windowed`) but tuned **memory-first**: peak RSS
//! is `≈ (fetch window compressed) + (decode budget)`, both knobs, flat regardless
//! of row-group size — not `main.rs`'s always-K-way fan-out (which multiplies
//! in-flight memory by K for speed). We fan out to K concurrent GET streams ONLY
//! for a lone row group above `split_threshold_bytes` (the case Ray's fragment
//! pool can't parallelize) — exactly mirroring the local K-split rule — so crate-K
//! and Ray's 4-thread pool never multiply. Every other layout is a single windowed
//! stream (K=1). Output is order-preserving (per-unit channels drained in order),
//! and the decode batch is byte-budgeted just like the local path.
//!
//! Within a single stream, every read shape decomposes into prefetchable *units*
//! — row windows (`~fetch_window_mb` compressed of all projected columns) for
//! ordinary groups, column groups (`~column_fetch_mb` compressed of some columns,
//! all rows) for wide ones — and all units flow through ONE mechanism (`drive_s3`):
//! a byte-denominated semaphore ("the bucket", `prefetch_budget_mb`) admits ranged
//! GETs concurrently until the bucket is full, while a single decoder consumes the
//! prefetched bytes strictly in order; a decoded unit dropping its bytes releases
//! its permits and wakes the next fetch. Fetch concurrency thus self-adjusts to
//! the fetch:decode speed ratio and peak prefetch memory is the bucket size by
//! construction. This is orthogonal to K: K adds parallel streams (spatial
//! split), each with its own bucket (`≈ k * prefetch_budget` compressed in
//! flight).
//!
//! Public API (consumed by `ArrowRsParquetFileReader` on the Python side via
//! `pa.RecordBatchReader.from_stream(...)`):
//!
//!   read_row_groups(path, row_groups=None, columns=None, batch_size=131072,
//!                   decode_budget_bytes=2*1024*1024, k=1,
//!                   split_threshold_bytes=128*1024*1024)
//!   read_row_groups_s3(bucket, key, region, anonymous, endpoint=None, ...creds...,
//!                      row_groups=None, columns=None, batch_size=131072,
//!                      decode_budget_bytes=2*1024*1024, fetch_window_mb=16, k=1,
//!                      split_threshold_bytes=128*1024*1024, predicate_json=None,
//!                      column_fetch_mb=16, prefetch_budget_mb=64)
//!
//! Per-file handles (TODO 1r — open once, decode many; see the "Per-file native
//! handles" section for why):
//!
//!   connect_s3(bucket, region, anonymous, ...creds...) -> NativeS3Store
//!   NativeS3Store.open_file(key, page_index=True)      -> NativeParquetFile
//!   open_parquet_file(path, page_index=False)          -> NativeParquetFile
//!   NativeParquetFile.metadata()                       -> ParquetFileMetadata (no I/O)
//!   NativeParquetFile.read_row_groups(...)             -> Arrow C-stream

mod predicate;

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;

use crate::predicate::{can_match, ColStats, Pred, Value};
use parquet::basic::{ConvertedType, Encoding, LogicalType, Type as PhysicalType};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use tokio::sync::{mpsc, Semaphore};

// NOTE on allocators: earlier prototypes carried optional mimalloc/jemalloc
// global-allocator features to chase a suspected allocator-retention gap vs
// PyArrow. Measurement killed the theory (jemalloc LD_PRELOAD inert, per-worker
// high-water lower than PyArrow's on the same fixtures), and mimalloc as a
// cdylib global allocator segfaulted Ray workers across the Arrow C-stream FFI
// boundary. Both features were removed to keep the dependency tree minimal;
// the system allocator is correct here. A/B experiments can still use
// LD_PRELOAD without recompiling.

// --------------------------------------------------------------------------- //
// Shared tokio runtime
// --------------------------------------------------------------------------- //
/// One process-wide multi-thread runtime, lazily built, shared by every
/// `read_row_groups_s3` call. Previously each fragment read built and tore down
/// its own 2-thread runtime — churn that scales with the file count. The async
/// work is IO-bound (awaiting range GETs), so a small fixed worker pool drives
/// many concurrent fetches. `worker_threads(4)` matches Ray's per-worker fragment
/// pool so decode never oversubscribes cores: either 4 fragments × K=1 unit, or
/// 1 lone fragment × K units — never both at once.
fn shared_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("build shared tokio runtime")
    })
}

// --------------------------------------------------------------------------- //
// Byte-budget batch sizing (ported from main.rs `byte_budget_rows`)
// --------------------------------------------------------------------------- //
/// Absolute floor on batch rows: protects against degenerate few-row batches
/// (per-batch allocation overhead) without overriding the byte budget. The old
/// floor of 2048 silently voided `decode_budget_bytes` for any schema over
/// ~16 KiB/row (2048 × 16 KiB = the 32 MiB default budget — findings K8): a
/// 1 MiB/row fat-string group decoded 2 GiB batches no matter what the knob
/// said. At 32 the floor only overrides the budget above `budget/32` per row
/// (1 MiB/row at the default), where a 32-row batch is ~one budget anyway.
const MIN_BATCH_ROWS: usize = 32;

/// Choose a batch row count so `rows * bytes_per_row ~= budget_bytes`, using the
/// row group's uncompressed size / row count from the footer. `requested` is the
/// upper clamp (a narrow schema never grows past the caller's ask) and
/// [`MIN_BATCH_ROWS`] the lower clamp. This is what keeps the decoded working
/// set flat across schemas.
fn byte_budget_rows(
    uncompressed_bytes: i64,
    num_rows: i64,
    requested: usize,
    budget_bytes: u64,
) -> usize {
    if num_rows <= 0 {
        return requested;
    }
    let bpr = (uncompressed_bytes as f64 / num_rows as f64).max(1.0);
    let budget_rows = (budget_bytes as f64 / bpr) as usize;
    budget_rows.clamp(MIN_BATCH_ROWS, requested.max(MIN_BATCH_ROWS))
}

/// Estimate the row group's DECODED (in-memory Arrow) size from the footer
/// alone. The footer's `total_byte_size` is the *encoded*-uncompressed size:
/// for dictionary/RLE-encoded columns that is dict values + indices, and the
/// decoded batch is larger by the expansion ratio — sizing batches by it made
/// decoded batches ≈ budget × expansion (findings M43/M45/M49: 7.3× overshoot
/// on 9.4×-dictionary tensor data, 4× on RLE-heavy numerics, on BOTH readers'
/// estimators).
///
/// For fixed-width physical types the decoded size is exactly
/// `num_values × width`, no decoding needed, so take
/// `max(encoded, num_values × width)` per column chunk:
///   - dict/RLE fixed-width (the entire measured loss family — float tensors,
///     repeated ints): the fixed-width term wins → exact;
///   - PLAIN fixed-width: encoded ≥ decoded (rep/def levels) → unchanged;
///   - BYTE_ARRAY (strings/binary): no footer-exact decoded size (a dict
///     chunk's value lengths aren't recorded), width 0 → encoded fallback,
///     i.e. exactly today's behavior;
///   - BOOLEAN: bit-packed on both sides → encoded fallback.
///
/// The estimate can only grow, so batches can only shrink vs. the old sizing —
/// strictly safer against the decode budget.
fn decoded_estimate_bytes(rgm: &RowGroupMetaData) -> i64 {
    let mut total: i64 = 0;
    for col in rgm.columns() {
        let descr = col.column_descr();
        let width: i64 = match descr.physical_type() {
            PhysicalType::INT32 | PhysicalType::FLOAT => 4,
            PhysicalType::INT64 | PhysicalType::DOUBLE => 8,
            PhysicalType::INT96 => 12,
            PhysicalType::FIXED_LEN_BYTE_ARRAY => descr.type_length() as i64,
            PhysicalType::BOOLEAN | PhysicalType::BYTE_ARRAY => 0,
        };
        let mut decoded = col.num_values().saturating_mul(width);
        // Nested leaves also materialize one i32 offset buffer per repetition
        // level (~4 B/row/level at the outermost lists; inner fan-out is
        // ignored, keeping this a floor). Not noise: on a 5000-list-column
        // schema the offsets alone are ~20 KB/row, and omitting them left
        // measured batches at 1.5x the budget — exactly on the G1 gate line.
        if descr.max_rep_level() > 0 {
            decoded =
                decoded.saturating_add(4 * rgm.num_rows().max(0) * descr.max_rep_level() as i64);
        }
        total = total.saturating_add(col.uncompressed_size().max(decoded));
    }
    total
}

/// [`byte_budget_rows`] with the decoded-aware estimate for a whole row group:
/// the one batch-sizing entry point for every read path (local sequential,
/// K-split ranges, S3 windowed units).
fn group_batch_rows(rgm: &RowGroupMetaData, requested: usize, budget_bytes: u64) -> usize {
    byte_budget_rows(
        decoded_estimate_bytes(rgm),
        rgm.num_rows(),
        requested,
        budget_bytes,
    )
}

/// A row group is *estimator-blind* when its decoded size cannot be derived
/// from the footer: dictionary-encoded BYTE_ARRAY (strings/binary) chunks,
/// where [`decoded_estimate_bytes`] falls back to the encoded size and batches
/// sized from it overshoot the decode budget by the dictionary expansion
/// ratio (M50 residual: 4x on dict-string lone_big_rg — the one measured
/// shape the footer-exact estimator can't cover). Checked against ALL columns,
/// not the projection, mirroring `decoded_estimate_bytes`.
fn group_is_estimator_blind(rgm: &RowGroupMetaData) -> bool {
    rgm.columns().iter().any(|c| {
        c.column_descr().physical_type() == PhysicalType::BYTE_ARRAY
            && c
                .encodings()
                .any(|e| matches!(e, Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY))
    })
}

/// Running decoded-size measurement for mid-stream batch adaptation:
/// cumulative (bytes, rows) over every batch yielded from blind groups, so
/// `bytes_per_row` amortizes per-batch buffer-capacity rounding (a lone
/// [`MIN_BATCH_ROWS`]-row probe can overcount from allocator doubling; the
/// cumulative average self-corrects as full-size batches arrive).
#[derive(Default, Clone, Copy)]
struct BprTracker {
    bytes: u64,
    rows: u64,
}

impl BprTracker {
    fn record(&mut self, batch: &RecordBatch) {
        self.bytes = self
            .bytes
            .saturating_add(batch.get_array_memory_size() as u64);
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
    }
    fn bytes_per_row(&self) -> Option<f64> {
        (self.rows > 0).then(|| self.bytes as f64 / self.rows as f64)
    }
}

/// Re-size a batch row count from the MEASURED decoded bytes/row. Shrink-only:
/// the static estimate stays the upper clamp, so a non-expanding shape (or a
/// noisy low measurement) can never produce batches bigger than today's, and
/// [`MIN_BATCH_ROWS`] stays the floor. `None` (nothing measured yet) keeps the
/// static size.
fn adapted_rows(static_rows: usize, budget_bytes: u64, measured_bpr: Option<f64>) -> usize {
    match measured_bpr {
        Some(bpr) if bpr > 0.0 => ((budget_bytes as f64 / bpr) as usize)
            .clamp(MIN_BATCH_ROWS, static_rows.max(MIN_BATCH_ROWS)),
        _ => static_rows,
    }
}

// --------------------------------------------------------------------------- //
// Row-group statistics pruning (predicate pushdown, part 1)
// --------------------------------------------------------------------------- //
/// Whether an INT32/INT64 physical column is a *plain signed integer*, i.e. the
/// raw statistic value can be compared directly as a signed `i64`. Everything
/// else stored in an INT32/INT64 physical slot is rejected:
///   - UINT_8/16/32/64: Parquet orders these by *unsigned* comparison, so a
///     value ≥ 2^(bits-1) is stored with its high bit set and reads back as a
///     negative `i64` — min/max can invert (e.g. u32 max reads as -1). Comparing
///     as signed would prune row groups that actually match.
///   - DECIMAL: the stat is the *unscaled* integer, which doesn't match the
///     decimal literal a predicate carries.
///   - DATE / TIME / TIMESTAMP: the encoded units may not match the predicate
///     literal's encoding.
/// For any of these we return no bound → `can_match` keeps the row group.
fn is_plain_signed_int(descr: &ColumnDescriptor) -> bool {
    match descr.logical_type_ref() {
        Some(LogicalType::Integer(int)) => int.is_signed,
        // Decimal / Date / Time / Timestamp / etc. on an int physical type.
        Some(_) => false,
        // Legacy files predate LogicalType; fall back to the converted type.
        None => matches!(
            descr.converted_type(),
            ConvertedType::NONE
                | ConvertedType::INT_8
                | ConvertedType::INT_16
                | ConvertedType::INT_32
                | ConvertedType::INT_64
        ),
    }
}

/// Map one column chunk's Parquet statistics to `(min, max)` in `Value` terms.
/// Types we can't soundly order for pruning — Int96, fixed-len byte arrays,
/// non-UTF8 byte arrays, and INT32/INT64 columns that aren't plain signed
/// integers (unsigned / decimal / date-time; see [`is_plain_signed_int`]) —
/// return `None`, which `can_match` treats as "keep". `descr` supplies the
/// logical type that `Statistics` (keyed by physical type) can't.
fn stat_min_max(stats: &Statistics, descr: &ColumnDescriptor) -> (Option<Value>, Option<Value>) {
    match stats {
        Statistics::Boolean(v) => (
            v.min_opt().map(|b| Value::Bool(*b)),
            v.max_opt().map(|b| Value::Bool(*b)),
        ),
        Statistics::Int32(v) if is_plain_signed_int(descr) => (
            v.min_opt().map(|x| Value::Int(*x as i64)),
            v.max_opt().map(|x| Value::Int(*x as i64)),
        ),
        Statistics::Int64(v) if is_plain_signed_int(descr) => (
            v.min_opt().map(|x| Value::Int(*x)),
            v.max_opt().map(|x| Value::Int(*x)),
        ),
        Statistics::Float(v) => (
            v.min_opt().map(|x| Value::Float(*x as f64)),
            v.max_opt().map(|x| Value::Float(*x as f64)),
        ),
        Statistics::Double(v) => (
            v.min_opt().map(|x| Value::Float(*x)),
            v.max_opt().map(|x| Value::Float(*x)),
        ),
        Statistics::ByteArray(v) => (
            v.min_opt()
                .and_then(|b| b.as_utf8().ok().map(|s| Value::Str(s.to_string()))),
            v.max_opt()
                .and_then(|b| b.as_utf8().ok().map(|s| Value::Str(s.to_string()))),
        ),
        // Int96, FixedLenByteArray, and non-signed-int INT32/INT64 columns
        // (guards above fell through): not ordered here → keep.
        _ => (None, None),
    }
}

/// Build the per-column statistics map for one row group. Keyed by the leaf
/// column path string, which equals the field name for the flat columns
/// predicates push on; nested paths (dotted) simply won't match a top-level
/// predicate column and are left un-pruned (conservative).
fn row_group_col_stats(rg: &RowGroupMetaData) -> HashMap<String, ColStats> {
    let num_rows = rg.num_rows();
    let mut map = HashMap::with_capacity(rg.num_columns());
    for i in 0..rg.num_columns() {
        let col = rg.column(i);
        if let Some(stats) = col.statistics() {
            let (min, max) = stat_min_max(stats, col.column_descr());
            let null_count = stats.null_count_opt().map(|n| n as i64);
            map.insert(
                col.column_path().string(),
                ColStats {
                    min,
                    max,
                    null_count,
                    num_rows,
                },
            );
        }
    }
    map
}

/// Drop the row groups in `selected` that `pred` proves cannot contain a match.
/// Conservative by construction (see `predicate::can_match`): a row group is
/// removed only when provably empty for the predicate, so this never drops a
/// group that could have contributed rows.
fn prune_row_groups(meta: &ArrowReaderMetadata, selected: Vec<usize>, pred: &Pred) -> Vec<usize> {
    let md = meta.metadata();
    selected
        .into_iter()
        .filter(|&rg| can_match(pred, &row_group_col_stats(md.row_group(rg))))
        .collect()
}

/// Parse the optional predicate IR and prune `selected` in one step; `None`
/// (no pushdown) returns `selected` unchanged.
fn apply_predicate(
    meta: &ArrowReaderMetadata,
    selected: Vec<usize>,
    predicate_json: &Option<String>,
) -> Vec<usize> {
    match predicate_json {
        None => selected,
        Some(j) => prune_row_groups(meta, selected, &Pred::from_json(j)),
    }
}

// --------------------------------------------------------------------------- //
// Column projection helper
// --------------------------------------------------------------------------- //
/// Build a leaf-column ProjectionMask from column names using the parquet schema
/// descriptor. Names not present are ignored (Python already resolved the read
/// set). Flat schemas only — the Python `_arrow_rs_supported` gate rejects nested
/// columns before we get here.
fn projection_mask(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    columns: &Option<Vec<String>>,
) -> ProjectionMask {
    match columns {
        None => ProjectionMask::all(),
        Some(names) => {
            let root = parquet_schema.root_schema();
            let mut indices = Vec::new();
            for (i, f) in root.get_fields().iter().enumerate() {
                if names.iter().any(|n| n == f.name()) {
                    indices.push(i);
                }
            }
            ProjectionMask::roots(parquet_schema, indices)
        }
    }
}

/// Ordered ROOT (top-level field) indices for a projection. Mirrors
/// `projection_mask`'s name matching so the two always agree on which columns
/// are read. Note these are root indices, not leaf/column-chunk indices — use
/// [`leaves_under_roots`] to expand to the chunks a root projection touches
/// (identity for flat schemas, several leaves per root for structs/lists).
fn projected_root_indices(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    columns: &Option<Vec<String>>,
) -> Vec<usize> {
    let root = parquet_schema.root_schema();
    match columns {
        None => (0..root.get_fields().len()).collect(),
        Some(names) => root
            .get_fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| names.iter().any(|n| n == f.name()))
            .map(|(i, _)| i)
            .collect(),
    }
}

/// All leaf-column (column-chunk) indices under the given root fields, in
/// ascending leaf order — exactly the chunks the decoder requests for a root
/// projection. Flat schemas: identity.
fn leaves_under_roots(
    schema: &parquet::schema::types::SchemaDescriptor,
    roots: &[usize],
) -> Vec<usize> {
    (0..schema.num_columns())
        .filter(|&l| roots.contains(&schema.get_column_root_idx(l)))
        .collect()
}

/// `(root index, compressed size)` for the projected top-level columns of a row
/// group, in ascending root order — the input to `partition_columns_by_budget`.
/// A root's size is the sum of its leaf chunks, so a struct column is weighed
/// (and later fetched/hstacked) as one indivisible unit.
fn projected_root_sizes(
    schema: &parquet::schema::types::SchemaDescriptor,
    rgm: &RowGroupMetaData,
    roots: &[usize],
) -> Vec<(usize, u64)> {
    roots
        .iter()
        .map(|&r| {
            let sz = leaves_under_roots(schema, &[r])
                .iter()
                .map(|&l| rgm.column(l).compressed_size().max(0) as u64)
                .sum();
            (r, sz)
        })
        .collect()
}

/// Partition projected columns into contiguous groups whose per-group compressed
/// size stays under `budget_bytes`, so the S3 reader can fetch+decode ONE group at
/// a time and hold only that group's compressed chunks resident — the wide-schema
/// memory fix (the async reader's default `InMemoryRowGroup` otherwise fetches every
/// projected column chunk for the row group up front). `cols` is `(leaf, size)` in
/// ascending leaf order and groups preserve that order, so hstacking the groups
/// reproduces file/schema column order. `budget_bytes == 0` (or ≤1 column) => a
/// single group (disabled). A lone oversized column still gets its own group — a
/// column can't be split below itself (that would be row-windowing, handled
/// elsewhere).
fn partition_columns_by_budget(cols: &[(usize, u64)], budget_bytes: u64) -> Vec<Vec<usize>> {
    if budget_bytes == 0 || cols.len() <= 1 {
        return vec![cols.iter().map(|(i, _)| *i).collect()];
    }
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut cur: Vec<usize> = Vec::new();
    let mut acc: u64 = 0;
    for &(idx, sz) in cols {
        // Start a new group when the current one is non-empty and adding this column
        // would exceed the budget. A single column always fits (never split below 1).
        if !cur.is_empty() && acc.saturating_add(sz) > budget_bytes {
            groups.push(std::mem::take(&mut cur));
            acc = 0;
        }
        cur.push(idx);
        acc = acc.saturating_add(sz);
    }
    if !cur.is_empty() {
        groups.push(cur);
    }
    groups
}

/// Probe the projected output schema with an empty (zero row group) reader, so
/// `schema()` is available to the FFI stream before any batch is pulled.
fn probe_schema(
    path: &str,
    meta: &ArrowReaderMetadata,
    mask: &ProjectionMask,
) -> Result<SchemaRef, ParquetError> {
    Ok(
        ParquetRecordBatchReaderBuilder::new_with_metadata(File::open(path)?, meta.clone())
            .with_projection(mask.clone())
            .with_row_groups(vec![])
            .build()?
            .schema(),
    )
}

// --------------------------------------------------------------------------- //
// Arrow C-stream wrapper returned to Python
// --------------------------------------------------------------------------- //
/// Holds an FFI stream until Python pulls it out via `__arrow_c_stream__`.
#[pyclass]
struct ArrowStream {
    inner: Option<FFI_ArrowArrayStream>,
}

#[pymethods]
impl ArrowStream {
    /// PyCapsule protocol: PyArrow's `RecordBatchReader.from_stream` calls this.
    #[pyo3(signature = (_requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &mut self,
        py: Python<'py>,
        _requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let stream = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("stream already consumed"))?;
        let name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, stream, Some(name))
    }
}

fn into_py_stream(reader: Box<dyn RecordBatchReader + Send>) -> ArrowStream {
    ArrowStream {
        inner: Some(FFI_ArrowArrayStream::new(reader)),
    }
}

// --------------------------------------------------------------------------- //
// Footer metadata returned to Python (Track 1: arrow-rs owns the footer read)
// --------------------------------------------------------------------------- //
/// The parts of the Parquet footer the Python reader needs so PyArrow no longer
/// has to open the file for supported fragments: the full Arrow schema (exposed
/// zero-copy via the Arrow C-schema PyCapsule, so it round-trips extension/field
/// metadata for the UDT path) plus per-row-group row counts and byte sizes (for
/// chunking, row-offset bookkeeping, `count()`, and the split threshold). Column
/// statistics for row-group pruning are a follow-up on this same struct.
#[pyclass]
struct ParquetFileMetadata {
    schema: SchemaRef,
    #[pyo3(get)]
    num_rows: i64,
    #[pyo3(get)]
    num_row_groups: usize,
    #[pyo3(get)]
    row_group_num_rows: Vec<i64>,
    #[pyo3(get)]
    row_group_byte_sizes: Vec<i64>,
    // Per-row-group *compressed* (on-disk) byte size — the sum of each column
    // chunk's compressed size. This is what the Python chunker bundles by, so it
    // must match PyArrow's `sum(col.total_compressed_size)`. Distinct from
    // `row_group_byte_sizes`, which is the *uncompressed* `total_byte_size()`.
    #[pyo3(get)]
    row_group_compressed_sizes: Vec<i64>,
    // Root (top-level) column names that contain an INT96-physical leaf. The
    // support gate needs this because parquet-rs honors an embedded Arrow-schema
    // unit hint for INT96 (→ us/ms/s) whereas PyArrow always forces ns: a column
    // in this list whose decoded unit isn't ns would diverge from PyArrow, so the
    // gate must fall it back. INT96 columns that come out as ns already match.
    #[pyo3(get)]
    int96_columns: Vec<String>,
    // True when the embedded Arrow schema (`ARROW:schema` footer metadata) could
    // not be parsed and was skipped (see `load_meta_local`), so `schema` here is
    // the parquet-inferred *storage* schema rather than the Arrow logical schema.
    // The Python reader reconstructs any lost extension types (e.g. Ray's
    // cloudpickle-serialized tensor type) from the file's own footer schema.
    #[pyo3(get)]
    arrow_schema_skipped: bool,
}

#[pymethods]
impl ParquetFileMetadata {
    /// Arrow PyCapsule protocol: `pa.schema(obj)` / `Schema._import_from_c_capsule`
    /// pull the schema through this. Rebuilt each call (cheap, non-consuming).
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi = FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(to_py)?;
        let name = CString::new("arrow_schema").unwrap();
        PyCapsule::new_bound(py, ffi, Some(name))
    }
}

/// Reader options shared by *every* entry point (metadata reads, local decode,
/// S3 decode), so the schema a metadata read reports can never diverge from what
/// a decode produces. `page_index` varies by caller: pruning paths ask for the
/// page index (`Optional`); a bare footer/metadata read skips it (`Skip`).
///
/// INT96 note: parquet-rs decodes the legacy INT96 timestamp physical type to
/// `Timestamp(Nanosecond, None)` by default (arrow/schema/primitive.rs), which is
/// exactly what PyArrow produces for INT96 by default — so a Spark/Hive/Impala
/// INT96 file (the common producers, which embed no Arrow schema) decodes
/// byte-identically on both paths and takes the native path with no coercion.
/// The one divergence is a file that embeds an Arrow schema pinning a *non-ns*
/// unit (e.g. a PyArrow writer with `use_deprecated_int96_timestamps=True` over a
/// `timestamp[us]` column): parquet-rs honors that embedded hint (→ us) while
/// PyArrow forces ns. That mismatch is caught by the support gate's
/// per-file-vs-unified type check, which falls the file back to PyArrow — correct,
/// if not yet native. parquet 59 has no `with_coerce_int96`, so forcing ns there
/// would need a per-column `with_schema` override; deferred (narrow case, and the
/// fallback is already correct).
fn reader_options(page_index: PageIndexPolicy) -> ArrowReaderOptions {
    ArrowReaderOptions::new().with_page_index_policy(page_index)
}

/// arrow-rs's IPC verifier rejects an embedded `ARROW:schema` footer whose field
/// `custom_metadata` values aren't valid UTF-8. Ray files written by 2.49-2.54
/// store the tensor extension type's metadata as a cloudpickle blob (binary), so
/// `ArrowReaderMetadata::load` fails parsing that embedded schema
/// (`Unable to get root as message stored in ARROW:schema: Utf8Error`). Detect
/// exactly that failure so the loaders below can retry with the embedded arrow
/// schema skipped — decoding the parquet-inferred storage types instead — after
/// which the Python reader re-applies the extension type from the pinned dataset
/// schema (a `list<..>`→`extension<..>` cast). Any other load error propagates.
fn is_embedded_arrow_schema_error<E: std::fmt::Display>(e: &E) -> bool {
    e.to_string().contains("ARROW:schema")
}

/// Load footer metadata for a local file, retrying with the embedded arrow schema
/// skipped when (and only when) it fails to parse (see
/// [`is_embedded_arrow_schema_error`]). Files whose footer parses normally are
/// untouched, so INT96 hints and valid embedded schemas behave exactly as before.
/// The returned bool is `true` when the retry fired (embedded arrow schema
/// skipped → the reported schema is the parquet-inferred storage type), which the
/// Python reader uses to reconstruct the extension type from the pinned schema.
fn load_meta_local(
    file: &File,
    page_index: PageIndexPolicy,
) -> Result<(ArrowReaderMetadata, bool), ParquetError> {
    match ArrowReaderMetadata::load(file, reader_options(page_index)) {
        Err(e) if is_embedded_arrow_schema_error(&e) => ArrowReaderMetadata::load(
            file,
            reader_options(page_index).with_skip_arrow_metadata(true),
        )
        .map(|m| (m, true)),
        other => other.map(|m| (m, false)),
    }
}

/// S3 counterpart of [`load_meta_local`]: same targeted retry (and same
/// skipped-bool contract), building a fresh `ParquetObjectReader` for the second
/// attempt so no half-consumed reader state carries over.
async fn load_meta_s3(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    page_index: PageIndexPolicy,
) -> Result<(ArrowReaderMetadata, bool), ParquetError> {
    let mut probe = ParquetObjectReader::new(store.clone(), path.clone());
    match ArrowReaderMetadata::load_async(&mut probe, reader_options(page_index)).await {
        Err(e) if is_embedded_arrow_schema_error(&e) => {
            let mut retry = ParquetObjectReader::new(store, path);
            ArrowReaderMetadata::load_async(
                &mut retry,
                reader_options(page_index).with_skip_arrow_metadata(true),
            )
            .await
            .map(|m| (m, true))
        }
        other => other.map(|m| (m, false)),
    }
}

/// Pull the fields Python needs out of an already-loaded `ArrowReaderMetadata`.
/// Local and S3 both funnel through here so the shape is identical.
fn build_file_metadata(
    meta: &ArrowReaderMetadata,
    arrow_schema_skipped: bool,
) -> ParquetFileMetadata {
    let md = meta.metadata();
    let n = md.num_row_groups();
    let mut row_group_num_rows = Vec::with_capacity(n);
    let mut row_group_byte_sizes = Vec::with_capacity(n);
    let mut row_group_compressed_sizes = Vec::with_capacity(n);
    let mut num_rows = 0i64;
    for i in 0..n {
        let rg = md.row_group(i);
        row_group_num_rows.push(rg.num_rows());
        row_group_byte_sizes.push(rg.total_byte_size());
        row_group_compressed_sizes.push(rg.compressed_size());
        num_rows += rg.num_rows();
    }

    // Collect the root column names backing an INT96 leaf. Walk the flat leaf
    // descriptors and key by the first path component so a nested INT96 (e.g. a
    // struct field) still surfaces its top-level column to the gate.
    let mut int96_roots: HashSet<String> = HashSet::new();
    for col in md.file_metadata().schema_descr().columns() {
        if col.physical_type() == PhysicalType::INT96 {
            if let Some(root) = col.path().parts().first() {
                int96_roots.insert(root.clone());
            }
        }
    }

    ParquetFileMetadata {
        schema: meta.schema().clone(),
        num_rows,
        num_row_groups: n,
        row_group_num_rows,
        row_group_byte_sizes,
        row_group_compressed_sizes,
        int96_columns: int96_roots.into_iter().collect(),
        arrow_schema_skipped,
    }
}

// --------------------------------------------------------------------------- //
// Local read (sync): per-group byte-budgeted sequential reader (K=1 path)
// --------------------------------------------------------------------------- //
/// Streams the selected row groups in order, building one `ParquetRecordBatchReader`
/// per group with a byte-budgeted batch size. Row order is preserved (single
/// reader, groups in ascending order) and peak memory stays ~one decode budget
/// because each batch is dropped as Python pulls the next.
struct RowGroupSeqReader {
    path: String,
    meta: ArrowReaderMetadata,
    mask: ProjectionMask,
    budget_bytes: u64,
    batch_clamp: usize,
    row_groups: Vec<usize>,
    pos: usize,
    current: Option<ParquetRecordBatchReader>,
    schema: SchemaRef,
    /// Decoded bytes/row observed so far (recorded for blind groups only —
    /// see [`group_is_estimator_blind`]); carries across row groups.
    bpr: BprTracker,
    /// Whether `current` reads a blind group (= record its batches).
    cur_blind: bool,
    /// Rest-of-group continuation after a probe reader: (rg, rows to skip).
    pending: Option<(usize, usize)>,
}

impl RowGroupSeqReader {
    fn new(
        path: String,
        meta: ArrowReaderMetadata,
        mask: ProjectionMask,
        schema: SchemaRef,
        row_groups: Vec<usize>,
        batch_clamp: usize,
        budget_bytes: u64,
    ) -> Self {
        Self {
            path,
            meta,
            mask,
            budget_bytes,
            batch_clamp,
            row_groups,
            pos: 0,
            current: None,
            schema,
            bpr: BprTracker::default(),
            cur_blind: false,
            pending: None,
        }
    }

    /// Build a reader for rows `[skip, skip+take)` of `rg` at `batch_rows`.
    /// A partial range uses a `RowSelection`; skipping needs no page index —
    /// the parquet reader decode-skips leading rows, and every skip here is at
    /// most one probe's worth.
    fn build_group_reader(
        &self,
        rg: usize,
        skip: usize,
        take: usize,
        total: usize,
        batch_rows: usize,
    ) -> Result<ParquetRecordBatchReader, ParquetError> {
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            File::open(&self.path)?,
            self.meta.clone(),
        )
        .with_batch_size(batch_rows)
        .with_row_groups(vec![rg])
        .with_projection(self.mask.clone());
        if skip > 0 || skip + take < total {
            builder = builder.with_row_selection(RowSelection::from(vec![
                RowSelector::skip(skip),
                RowSelector::select(take),
            ]));
        }
        builder.build()
    }
}

impl Iterator for RowGroupSeqReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if self.cur_blind {
                            self.bpr.record(&batch);
                        }
                        return Some(Ok(batch));
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => self.current = None,
                }
            }
            // Next reader: the rest of a probed group, else the next group.
            let (rg, skip) = match self.pending.take() {
                Some(cont) => cont,
                None => {
                    if self.pos >= self.row_groups.len() {
                        return None;
                    }
                    let rg = self.row_groups[self.pos];
                    self.pos += 1;
                    (rg, 0)
                }
            };
            let (blind, static_eff, total) = {
                let rgm = self.meta.metadata().row_group(rg);
                (
                    group_is_estimator_blind(rgm),
                    group_batch_rows(rgm, self.batch_clamp, self.budget_bytes),
                    rgm.num_rows().max(0) as usize,
                )
            };
            self.cur_blind = blind;
            let remaining = total.saturating_sub(skip);
            if remaining == 0 {
                continue;
            }
            let (batch_rows, take) = if !blind {
                (static_eff, remaining)
            } else if self.bpr.bytes_per_row().is_some() {
                (
                    adapted_rows(static_eff, self.budget_bytes, self.bpr.bytes_per_row()),
                    remaining,
                )
            } else if remaining > MIN_BATCH_ROWS {
                // First blind group, nothing measured yet: open with a tiny
                // probe reader (bounded by the same argument that sets
                // MIN_BATCH_ROWS), then continue the group adapted.
                self.pending = Some((rg, skip + MIN_BATCH_ROWS));
                (MIN_BATCH_ROWS, MIN_BATCH_ROWS)
            } else {
                // Group no bigger than a probe: just read it; its batches
                // still feed the tracker for the groups after it.
                (static_eff, remaining)
            };
            match self.build_group_reader(rg, skip, take, total, batch_rows) {
                Ok(reader) => self.current = Some(reader),
                Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
            }
        }
    }
}

impl RecordBatchReader for RowGroupSeqReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// --------------------------------------------------------------------------- //
// Local read (sync): intra-fragment K-split for one big row group
// --------------------------------------------------------------------------- //
/// Splits one row group's rows into K contiguous ranges decoded by K threads,
/// merging them back in range order so output row order matches a sequential read.
/// Each range has its own bounded channel (backpressure), and the consumer drains
/// channels in ascending range order — so at most `k * channel_depth` batches are
/// resident and rows come out in file order. Requires the offset/page index so a
/// `RowSelection` fetches only its range's pages (else each worker would decode the
/// whole column chunk); the caller checks this before choosing this path.
struct ParallelRangeReader {
    schema: SchemaRef,
    receivers: Vec<Receiver<Result<RecordBatch, ArrowError>>>,
    cur: usize,
}

fn build_range_reader(
    path: &str,
    meta: &ArrowReaderMetadata,
    mask: &ProjectionMask,
    rg: usize,
    start: usize,
    len: usize,
    batch: usize,
) -> Result<ParquetRecordBatchReader, ParquetError> {
    let sel = RowSelection::from(vec![RowSelector::skip(start), RowSelector::select(len)]);
    ParquetRecordBatchReaderBuilder::new_with_metadata(File::open(path)?, meta.clone())
        .with_row_groups(vec![rg])
        .with_row_selection(sel)
        .with_batch_size(batch)
        .with_projection(mask.clone())
        .build()
}

impl ParallelRangeReader {
    fn spawn(
        path: String,
        meta: ArrowReaderMetadata,
        mask: ProjectionMask,
        schema: SchemaRef,
        rg: usize,
        total_rows: usize,
        k: usize,
        batch: usize,
    ) -> Self {
        let chunk = total_rows.div_ceil(k.max(1)).max(1);
        let mut receivers = Vec::new();
        let mut start = 0usize;
        while start < total_rows {
            let len = chunk.min(total_rows - start);
            // Depth 2: a worker may run one batch ahead of the consumer, no more.
            let (tx, rx) = sync_channel::<Result<RecordBatch, ArrowError>>(2);
            receivers.push(rx);
            let (path, meta, mask) = (path.clone(), meta.clone(), mask.clone());
            thread::spawn(move || {
                match build_range_reader(&path, &meta, &mask, rg, start, len, batch) {
                    Ok(reader) => {
                        for batch in reader {
                            if tx.send(batch).is_err() {
                                break; // consumer dropped
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(ArrowError::ExternalError(Box::new(e))));
                    }
                }
            });
            start += len;
        }
        Self {
            schema,
            receivers,
            cur: 0,
        }
    }
}

impl Iterator for ParallelRangeReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.cur < self.receivers.len() {
            match self.receivers[self.cur].recv() {
                Ok(item) => return Some(item),
                Err(_) => self.cur += 1, // this range's channel closed → next range
            }
        }
        None
    }
}

impl RecordBatchReader for ParallelRangeReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// --------------------------------------------------------------------------- //
// Local entry point: choose sequential vs K-split
// --------------------------------------------------------------------------- //
#[allow(clippy::too_many_arguments)]
fn open_local_reader(
    path: String,
    row_groups: Option<Vec<usize>>,
    columns: Option<Vec<String>>,
    batch_size: usize,
    budget_bytes: u64,
    k: usize,
    split_threshold_bytes: u64,
    predicate_json: Option<String>,
) -> Result<Box<dyn RecordBatchReader + Send>, ParquetError> {
    // Lean footer parse (#6): the page index is only needed for the K-split
    // RowSelection (to skip pages by byte range). K-split can only fire when k > 1,
    // so for the common k == 1 local path we Skip the page index entirely — a
    // cheaper footer parse that matters on many-row-group files. When k > 1 we load
    // it Optional so the lone-big-row-group split can use it if present.
    let policy = if k > 1 {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let (meta, _skipped) = load_meta_local(&File::open(&path)?, policy)?;
    build_local_reader(
        path,
        meta,
        row_groups,
        columns,
        batch_size,
        budget_bytes,
        k,
        split_threshold_bytes,
        predicate_json,
    )
}

/// The metadata-independent half of the local read: everything after the footer
/// load. Shared by [`open_local_reader`] (loads the footer per call — the
/// original API) and [`NativeParquetFile::read_row_groups`] (footer loaded once
/// at open, reused across calls — TODO 1r). Whether the K-split can fire depends
/// on the page-index policy the *caller* loaded `meta` under, exactly as before.
#[allow(clippy::too_many_arguments)]
fn build_local_reader(
    path: String,
    meta: ArrowReaderMetadata,
    row_groups: Option<Vec<usize>>,
    columns: Option<Vec<String>>,
    batch_size: usize,
    budget_bytes: u64,
    k: usize,
    split_threshold_bytes: u64,
    predicate_json: Option<String>,
) -> Result<Box<dyn RecordBatchReader + Send>, ParquetError> {
    let mask = projection_mask(meta.metadata().file_metadata().schema_descr(), &columns);
    let selected: Vec<usize> = match row_groups {
        Some(v) => v,
        None => (0..meta.metadata().num_row_groups()).collect(),
    };
    // Statistics-based row-group pruning (conservative — see predicate.rs). This
    // is the mechanism that replaces PyArrow's `fragment.subset(filter=...)` so
    // pruned groups are never fetched or decoded.
    let selected = apply_predicate(&meta, selected, &predicate_json);
    let schema = probe_schema(&path, &meta, &mask)?;

    // K-split only for a *single* row group above the threshold, and only when the
    // page index is present (else each range would decode the whole column chunk).
    // This is exactly the lone-big-fragment case Ray's pool can't parallelize; every
    // other layout uses the sequential path so crate-K and Ray's pool never multiply.
    let split = k > 1
        && selected.len() == 1
        && meta.metadata().row_group(selected[0]).total_byte_size() as u64 >= split_threshold_bytes
        && meta.metadata().offset_index().is_some();

    if split {
        let rg = selected[0];
        let (total_rows, static_eff, blind) = {
            let rgm = meta.metadata().row_group(rg);
            (
                rgm.num_rows().max(0) as usize,
                group_batch_rows(rgm, batch_size, budget_bytes),
                group_is_estimator_blind(rgm),
            )
        };
        let mut eff = static_eff;
        if blind && total_rows > MIN_BATCH_ROWS {
            // K range readers can't re-size mid-range, so measure BEFORE the
            // fan-out: a measure-only decode of the group's first
            // MIN_BATCH_ROWS rows (range 0 decodes them again — 32 rows,
            // negligible) gives the real decoded bytes/row.
            let mut tracker = BprTracker::default();
            let probe = build_range_reader(
                &path,
                &meta,
                &mask,
                rg,
                0,
                MIN_BATCH_ROWS,
                MIN_BATCH_ROWS,
            )?;
            for batch in probe {
                let batch = batch.map_err(|e| ParquetError::External(Box::new(e)))?;
                tracker.record(&batch);
            }
            eff = adapted_rows(static_eff, budget_bytes, tracker.bytes_per_row());
        }
        Ok(Box::new(ParallelRangeReader::spawn(
            path, meta, mask, schema, rg, total_rows, k, eff,
        )))
    } else {
        Ok(Box::new(RowGroupSeqReader::new(
            path,
            meta,
            mask,
            schema,
            selected,
            batch_size,
            budget_bytes,
        )))
    }
}

#[pyfunction]
#[pyo3(signature = (path, row_groups=None, columns=None, batch_size=131072, decode_budget_bytes=2*1024*1024, k=1, split_threshold_bytes=134217728, predicate_json=None))]
#[allow(clippy::too_many_arguments)]
fn read_row_groups(
    py: Python<'_>,
    path: String,
    row_groups: Option<Vec<usize>>,
    columns: Option<Vec<String>>,
    batch_size: usize,
    decode_budget_bytes: u64,
    k: usize,
    split_threshold_bytes: u64,
    // Optional predicate IR (JSON, built from the Ray `Expr`) for statistics
    // row-group pruning. None = no pushdown. Row-level filtering still happens
    // in Python post-decode, so this only avoids IO/decode, never changes rows.
    predicate_json: Option<String>,
) -> PyResult<ArrowStream> {
    // Footer read + reader construction is blocking file I/O; release the GIL so
    // sibling Python read threads (Ray's fragment pool) run in parallel.
    let reader = py
        .allow_threads(|| {
            open_local_reader(
                path,
                row_groups,
                columns,
                batch_size,
                decode_budget_bytes,
                k,
                split_threshold_bytes,
                predicate_json,
            )
        })
        .map_err(to_py)?;
    Ok(into_py_stream(reader))
}

// --------------------------------------------------------------------------- //
// S3 read (async, windowed, byte-budgeted, order-preserving)
// --------------------------------------------------------------------------- //
/// Number of decoded batches a unit task may run ahead of the consumer. Depth 2
/// bounds resident memory while still letting a task fetch/decode one batch ahead.
const S3_CHANNEL_DEPTH: usize = 2;

/// A sync `RecordBatchReader` fed by K background tokio tasks (one per row-range
/// unit), each draining its unit into a bounded async channel. The consumer drains
/// channels in ascending unit order — so at most `k * S3_CHANNEL_DEPTH` batches are
/// resident and rows come out in file order (K units are contiguous ascending
/// ranges). `blocking_recv` is called from the Python thread (outside the runtime),
/// which is exactly what tokio's mpsc supports.
struct S3ChannelReader {
    schema: SchemaRef,
    receivers: Vec<mpsc::Receiver<Result<RecordBatch, ArrowError>>>,
    cur: usize,
}

impl Iterator for S3ChannelReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.cur < self.receivers.len() {
            match self.receivers[self.cur].blocking_recv() {
                Some(item) => return Some(item),
                None => self.cur += 1, // this unit's channel closed → next unit
            }
        }
        None
    }
}

impl RecordBatchReader for S3ChannelReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Rows per fetch window from a byte budget over the row group's *compressed*
/// bytes/row — this bounds IN-FLIGHT NETWORK bytes (what we fetch before decode).
/// 0 means "whole range in one shot" (no window cap).
fn window_rows_for(rgm: &parquet::file::metadata::RowGroupMetaData, fetch_window_mb: u64) -> usize {
    if fetch_window_mb == 0 {
        return 0;
    }
    let comp = rgm.compressed_size().max(1) as f64;
    let rows = rgm.num_rows().max(1) as f64;
    let comp_bpr = (comp / rows).max(1.0);
    (((fetch_window_mb as f64) * 1024.0 * 1024.0) / comp_bpr).max(1.0) as usize
}

/// Largest single data page, in rows, across the projected columns of row group
/// `rg` (from the offset index). This is the floor a fetch window must not go
/// below: a `RowSelection` can only skip whole *pages*, never rows within a page,
/// so a window narrower than a column's page forces every window overlapping that
/// page to decode the whole page — re-decoding the row group once per window (the
/// wide/short-row-group pathology: few rows → one page/column → N-way re-decode).
///
/// Returns the row group's total row count when there is no offset index (windowing
/// can't skip pages without it anyway) or the index is empty — either way the
/// caller then collapses to a single window (no split), which is correct.
fn max_page_rows(md: &parquet::file::metadata::ParquetMetaData, rg: usize) -> usize {
    let num_rows = md.row_group(rg).num_rows().max(0);
    let rg_oi = match md.offset_index().and_then(|oi| oi.get(rg)) {
        Some(rg_oi) if !rg_oi.is_empty() => rg_oi,
        _ => return num_rows as usize,
    };
    let mut max_rows: i64 = 0;
    for col in rg_oi {
        let locs = col.page_locations();
        for (i, loc) in locs.iter().enumerate() {
            let end = locs
                .get(i + 1)
                .map(|next| next.first_row_index)
                .unwrap_or(num_rows);
            max_rows = max_rows.max(end - loc.first_row_index);
        }
    }
    if max_rows <= 0 {
        num_rows as usize
    } else {
        max_rows as usize
    }
}

/// The row step a fetch window should advance by, given the byte-budget
/// `window_rows` (0 = no cap) and the coarsest column's `max_page_rows`. Clamping
/// the window up to at least one full page means each page is decoded by ~one
/// window instead of every overlapping window; a group whose largest page spans
/// the whole range (wide/short) collapses to `len` (a single window == parity with
/// no windowing), while a tall multi-page group still splits and keeps the
/// bounded-working-set win. Pure so it is unit-tested without a Parquet fixture.
fn effective_window_step(window_rows: usize, max_page_rows: usize, len: usize) -> usize {
    if window_rows == 0 {
        return len.max(1);
    }
    window_rows.max(max_page_rows).max(1)
}

/// One prefetched unit's compressed bytes, plus the byte-budget permits they
/// hold. A unit is either a ROW WINDOW (`sel = Some((skip, take))`, all projected
/// columns) or a COLUMN GROUP (`sel = None`, a slice of the projection over all
/// rows) — the decode side reassembles accordingly; the fetch side treats both
/// identically. The permits travel with the bytes (into the decode stream's
/// `PrefetchedReader`), so dropping the decoded stream frees the bytes AND
/// releases the permits — which is what wakes the admission loop to launch the
/// next unit's fetch. That drop-to-wake handoff is the whole backpressure
/// mechanism: memory pressure stays ~constant at `prefetch_budget` compressed
/// bytes without any explicit signalling code. The permit is `Arc`-shared:
/// a column-group episode's n units are admitted under ONE summed permit
/// (their streams are all held open for the lockstep hstack, so per-unit
/// permits would deadlock the admission loop), released when the last of the
/// group's streams drops; a row window is an episode of one, so its Arc is
/// sole-owner and drops exactly as before.
struct PrefetchedUnit {
    mask: ProjectionMask,
    sel: Option<(usize, usize)>,
    ranges: Vec<Range<u64>>,
    data: Vec<Bytes>,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}

/// Permits (KiB) one admission episode (a row window, or a column-group set
/// admitted together) may hold from the prefetch bucket: its compressed
/// size, capped at HALF the budget so a single oversized episode can never
/// drain the whole semaphore. When one unit held the full budget, no other fetch
/// could be admitted while it decoded — fetch stopped overlapping decode and
/// the read went strictly serial (findings T6 measured this at up to 5× wall).
/// Capping at half guarantees at least two units can be in flight, restoring
/// the overlap; the cost is that the bucket under-accounts a unit whose real
/// size exceeds half the budget (its full bytes are fetched regardless — a
/// unit can't be split below a page/column), so peak in-flight compressed
/// bytes is bounded by `budget + 2 * max_oversized_unit_excess` rather than
/// `budget` exactly. Oversized units are rare after the windows-first planning
/// rule (a lone column bigger than `colwindow_budget` in a genuinely wide
/// group, or a no-offset-index chunk fallback). A 0/1-KiB budget still
/// degrades to strict fetch→decode→fetch as before.
fn unit_permit_kib(kib: u64, budget_kib: u64) -> u32 {
    kib.clamp(1, (budget_kib / 2).max(1)) as u32
}

/// Find `want` inside one of the prefetched `ranges` and return the matching
/// slice of its `Bytes` (refcounted view, no copy). `None` if no prefetched
/// range fully contains it. Pure so it unit-tests without an object store.
fn slice_prefetched(ranges: &[Range<u64>], data: &[Bytes], want: &Range<u64>) -> Option<Bytes> {
    for (r, b) in ranges.iter().zip(data) {
        if want.start >= r.start && want.end <= r.end {
            let s = (want.start - r.start) as usize;
            let e = (want.end - r.start) as usize;
            return Some(b.slice(s..e));
        }
    }
    None
}

/// `AsyncFileReader` that serves a unit's page reads from its prefetched
/// buffers instead of S3. The decode stream built on top of this never touches
/// the network — the fetch already happened, budget-gated, in the admission
/// loop. Requests are always sub-ranges of the prefetched ranges (page reads
/// within a chunk, or within a window's page span), so containment lookup
/// suffices. Owning the permit ties the budget release to the stream's drop.
struct PrefetchedReader {
    ranges: Vec<Range<u64>>,
    data: Vec<Bytes>,
    meta: Arc<ParquetMetaData>,
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl AsyncFileReader for PrefetchedReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let res = slice_prefetched(&self.ranges, &self.data, &range).ok_or_else(|| {
            ParquetError::General(format!(
                "column-prefetch: byte range {range:?} was not prefetched"
            ))
        });
        Box::pin(futures::future::ready(res))
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        // Never hit in practice (streams are built with `new_with_metadata`),
        // but the trait requires it.
        let meta = self.meta.clone();
        Box::pin(futures::future::ready(Ok(meta)))
    }
}

/// Byte ranges + compressed KiB of one column group's chunks, straight from
/// the footer metadata — the "conservative estimate from parquet metadata"
/// that sizes each fetch exactly (chunk offsets/lengths are exact, not
/// estimates, so the budget accounting is exact too).
fn group_fetch_plan(
    rgm: &parquet::file::metadata::RowGroupMetaData,
    group: &[usize],
) -> (Vec<Range<u64>>, u64) {
    let mut ranges = Vec::with_capacity(group.len());
    let mut bytes = 0u64;
    for &leaf in group {
        let cc = rgm.column(leaf);
        let (start, len) = cc.byte_range();
        ranges.push(start..start + len);
        bytes += len;
    }
    (ranges, bytes.div_ceil(1024))
}

/// Exact byte ranges a ROW-WINDOW decode will request from the store: for each
/// projected leaf column of row group `rg`, the dictionary page (when present)
/// plus the contiguous span of data pages overlapping rows `[w, w+wlen)` — read
/// off the same offset index the decoder's `RowSelection` uses to skip pages,
/// so prefetch and decode always agree on which bytes are needed. A column
/// without an offset index, or a window covering the whole group, falls back to
/// the whole column chunk — also exactly what the decoder requests in that
/// case. Returns the ranges plus their compressed KiB (exact, from the footer)
/// for budget accounting, like [`group_fetch_plan`].
fn window_fetch_plan(
    md: &parquet::file::metadata::ParquetMetaData,
    rg: usize,
    leaves: &[usize],
    w: usize,
    wlen: usize,
) -> (Vec<Range<u64>>, u64) {
    let rgm = md.row_group(rg);
    let num_rows = rgm.num_rows().max(0);
    let rg_oi = md.offset_index().and_then(|oi| oi.get(rg));
    let whole = w == 0 && wlen as i64 >= num_rows;
    let (w0, w1) = (w as i64, (w + wlen) as i64);
    let mut ranges = Vec::with_capacity(leaves.len());
    let mut bytes = 0u64;
    for &leaf in leaves {
        let cc = rgm.column(leaf);
        let (chunk_start, chunk_len) = cc.byte_range();
        let locs = rg_oi
            .and_then(|oi| oi.get(leaf))
            .map(|c| c.page_locations())
            .filter(|l| !l.is_empty());
        let locs = match (whole, locs) {
            (false, Some(l)) => l,
            _ => {
                bytes += chunk_len;
                ranges.push(chunk_start..chunk_start + chunk_len);
                continue;
            }
        };
        // Dictionary page: sits between the chunk start and the first data
        // page, and every window of a dictionary-encoded chunk needs it.
        let first_data = locs[0].offset.max(0) as u64;
        if cc.dictionary_page_offset().is_some() && first_data > chunk_start {
            bytes += first_data - chunk_start;
            ranges.push(chunk_start..first_data);
        }
        // Contiguous span of the data pages overlapping [w, w+wlen). Pages are
        // laid out back-to-back within a chunk, so one range covers the run.
        let mut span: Option<(u64, u64)> = None;
        for (i, loc) in locs.iter().enumerate() {
            let rows_end = locs
                .get(i + 1)
                .map(|next| next.first_row_index)
                .unwrap_or(num_rows);
            if loc.first_row_index < w1 && rows_end > w0 {
                let s = loc.offset.max(0) as u64;
                let e = s + loc.compressed_page_size.max(0) as u64;
                span = Some(match span {
                    None => (s, e),
                    Some((a, _)) => (a, e),
                });
            }
        }
        if let Some((s, e)) = span {
            bytes += e - s;
            ranges.push(s..e);
        }
    }
    (ranges, bytes.div_ceil(1024))
}

/// How one row group's prefetched units reassemble into output batches.
enum RgDecode {
    /// N row-window units: decode each in order, stream every batch straight out.
    Windows(usize),
    /// N column-group units: decode all N in LOCKSTEP (their batch boundaries
    /// are row-aligned by construction), hstacking one batch-slice at a time to
    /// full width and emitting it before the next slice is decoded.
    Hstack(usize),
}

/// Decode structure of one (row group, sub-range), in output order.
struct RgPlan {
    rg: usize,
    batch_rows: usize,
    decode: RgDecode,
}

/// Fetch plan of one unit, in decode order: what to project, which rows
/// (`None` = all), which bytes, and their compressed KiB for budget accounting.
struct UnitFetch {
    mask: ProjectionMask,
    sel: Option<(usize, usize)>,
    ranges: Vec<Range<u64>>,
    kib: u64,
}

/// Plan every `(row group, start, len)` sub-range into prefetchable units.
/// The split axis is chosen per row group by its shape:
///   * ROW WINDOWS whenever they can actually split the range (the effective
///     window step is smaller than the range): each unit is ~`fetch_window_mb`
///     compressed bytes of ALL projected columns, page-aligned via the offset
///     index (see `effective_window_step`). Windows stream batches straight
///     out, so decoded retention is ~one decode budget regardless of group
///     size — which is why they are preferred whenever possible.
///   * COLUMN GROUPS only when row-windowing is inert (the step covers the
///     whole range — a wide/short group whose every column is a single page, a
///     missing offset index, or a group smaller than one fetch window) AND the
///     projected roots' compressed bytes partition into >1 group under
///     `colwindow_budget`: each unit is a slice of the projection over all the
///     group's rows. The async reader's `InMemoryRowGroup` otherwise stages
///     every projected chunk of the group at once, which for a 5000-column
///     schema was the entire S3 memory regression; fetching group-sized units
///     bounds each unit to ~`colwindow_budget`. The decode side
///     (`RgDecode::Hstack`) holds all the groups' COMPRESSED bytes and decodes
///     them in lockstep, one row-aligned batch-slice at a time, so decoded
///     retention is ~one full-width batch — it no longer parks whole decoded
///     groups (the old TODO-1u behavior, which degenerated to PyArrow's
///     whole-group retention). Tall-fat-column groups (few projected columns,
///     each over the budget) used to mis-select this axis and retain the
///     entire decoded row group (findings M20: 3.47 GB pinned,
///     `fetch_window_mb`-inert); the windows-first rule above fixed the
///     selection — they window instead.
/// Units are returned as admission EPISODES: each inner `Vec` is admitted
/// under one summed byte-budget permit in [`drive_s3`]. A row window is an
/// episode of one; an Hstack group's n units form one episode because the
/// decoder holds all n streams open for the lockstep hstack — admitting them
/// under per-unit permits would deadlock the admission loop once the bucket
/// ran dry mid-group. Planning globally (not per row group) lets the
/// prefetcher run ahead across row-group boundaries.
#[allow(clippy::too_many_arguments)]
fn plan_s3_units(
    meta: &ArrowReaderMetadata,
    full_mask: &ProjectionMask,
    roots: &[usize],
    subranges: &[(usize, usize, usize)],
    batch_clamp: usize,
    decode_budget: u64,
    fetch_window_mb: u64,
    colwindow_budget: u64,
) -> (Vec<RgPlan>, Vec<Vec<UnitFetch>>) {
    let md = meta.metadata();
    let schema_descr = md.file_metadata().schema_descr();
    let leaves = leaves_under_roots(schema_descr, roots);
    let mut rg_plans: Vec<RgPlan> = Vec::with_capacity(subranges.len());
    let mut units: Vec<Vec<UnitFetch>> = Vec::new();
    for &(rg, start, len) in subranges {
        let rgm = md.row_group(rg);
        let batch_rows = group_batch_rows(rgm, batch_clamp, decode_budget);
        // Never window below the coarsest column's largest page (see
        // max_page_rows): a sub-page window re-decodes that page in every
        // window it overlaps. Wide/short groups (one page per column) collapse
        // to a single window; tall multi-page groups still split.
        let step = effective_window_step(
            window_rows_for(rgm, fetch_window_mb),
            max_page_rows(md, rg),
            len,
        );
        // Column groups only apply to whole-group reads (a K-split sub-range is
        // by definition a tall group being split by rows, not columns) and only
        // when row windows can't split the range (step covers it whole) — see
        // the doc comment above for why windows always win when they can fire.
        let whole = start == 0 && len == rgm.num_rows().max(0) as usize;
        let groups = if whole && step >= len && colwindow_budget > 0 {
            partition_columns_by_budget(
                &projected_root_sizes(schema_descr, rgm, roots),
                colwindow_budget,
            )
        } else {
            Vec::new()
        };
        if groups.len() > 1 {
            let n = groups.len();
            let mut episode = Vec::with_capacity(n);
            for g in groups {
                let (ranges, kib) = group_fetch_plan(rgm, &leaves_under_roots(schema_descr, &g));
                episode.push(UnitFetch {
                    mask: ProjectionMask::roots(schema_descr, g),
                    sel: None,
                    ranges,
                    kib,
                });
            }
            units.push(episode);
            rg_plans.push(RgPlan {
                rg,
                batch_rows,
                decode: RgDecode::Hstack(n),
            });
        } else {
            let (mut w, end) = (start, start + len);
            let mut n = 0usize;
            while w < end {
                let wlen = step.min(end - w);
                let (ranges, kib) = window_fetch_plan(md, rg, &leaves, w, wlen);
                units.push(vec![UnitFetch {
                    mask: full_mask.clone(),
                    sel: Some((w, wlen)),
                    ranges,
                    kib,
                }]);
                w += wlen;
                n += 1;
            }
            rg_plans.push(RgPlan {
                rg,
                batch_rows,
                decode: RgDecode::Windows(n),
            });
        }
    }
    (rg_plans, units)
}

/// Receive the next prefetched unit (in plan order) and build its decode
/// stream, which serves every page read from the prefetched bytes — never the
/// network. The unit's budget permits ride inside the stream's
/// `PrefetchedReader`, so dropping the stream is what releases them.
async fn next_unit_stream(
    hrx: &mut mpsc::Receiver<tokio::task::JoinHandle<Result<PrefetchedUnit, ArrowError>>>,
    meta: &ArrowReaderMetadata,
    rg: usize,
    batch_rows: usize,
) -> Result<parquet::arrow::async_reader::ParquetRecordBatchStream<PrefetchedReader>, ArrowError> {
    let handle = hrx.recv().await.ok_or_else(|| {
        ArrowError::ExternalError(Box::new(ParquetError::General(
            "prefetch: admission loop ended early".to_string(),
        )))
    })?;
    let unit = match handle.await {
        Ok(Ok(u)) => u,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(ArrowError::ExternalError(Box::new(e))), // task panicked
    };
    let PrefetchedUnit {
        mask,
        sel,
        ranges,
        data,
        permit,
    } = unit;
    let reader = PrefetchedReader {
        ranges,
        data,
        meta: Arc::clone(meta.metadata()),
        _permit: permit,
    };
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
        .with_row_groups(vec![rg])
        .with_batch_size(batch_rows)
        .with_projection(mask);
    if let Some((skip, take)) = sel {
        builder = builder.with_row_selection(RowSelection::from(vec![
            RowSelector::skip(skip),
            RowSelector::select(take),
        ]));
    }
    builder
        .build()
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
}

/// THE single S3 decode driver: every read shape — row-windowed streaming,
/// wide-schema column groups, and each K-split row partition — flows through
/// here. Two halves, connected by a byte-denominated semaphore ("the bucket"):
///
///   * admission loop (spawned): for each planned unit IN ORDER, acquire
///     permits equal to its compressed size (capped at half the budget so one
///     oversized unit never drains the bucket and serializes fetch behind
///     decode — see [`unit_permit_kib`]), then spawn its ranged GET. Fetches
///     whose permits fit run CONCURRENTLY — that's what overlaps S3 latency
///     with decode — while `acquire` blocks the loop the moment the bucket is
///     spent.
///   * decoder (this task): strictly one unit at a time (bounds decode
///     scratch), served entirely from the prefetched bytes. Dropping a decoded
///     unit's stream frees its bytes AND releases its permits, waking the
///     admission loop: constant memory pressure, fetch concurrency
///     self-adjusting to the fetch:decode speed ratio, no rate estimation
///     anywhere.
///
/// `prefetch_budget_mb == 0` degrades to strict fetch→decode→fetch (every
/// acquire is for the full 1-permit budget). Output batches are sent to `tx`
/// in row order.
#[allow(clippy::too_many_arguments)]
async fn drive_s3(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    meta: ArrowReaderMetadata,
    out_schema: SchemaRef,
    full_mask: ProjectionMask,
    roots: Vec<usize>,
    subranges: Vec<(usize, usize, usize)>,
    batch_clamp: usize,
    decode_budget: u64,
    fetch_window_mb: u64,
    colwindow_budget: u64,
    prefetch_budget_mb: u64,
    tx: mpsc::Sender<Result<RecordBatch, ArrowError>>,
) {
    // Send an error downstream and stop.
    macro_rules! send_err {
        ($e:expr) => {{
            let _ = tx.send(Err($e)).await;
            return;
        }};
    }

    let (rg_plans, units) = plan_s3_units(
        &meta,
        &full_mask,
        &roots,
        &subranges,
        batch_clamp,
        decode_budget,
        fetch_window_mb,
        colwindow_budget,
    );

    // --- admission loop: budget-gated concurrent prefetch ---
    let budget_kib = prefetch_budget_mb.saturating_mul(1024).max(1);
    let budget_kib = budget_kib.min(u32::MAX as u64 / 2);
    let sem = Arc::new(Semaphore::new(budget_kib as usize));
    // Handles are tiny; the byte budget is what actually bounds prefetch. The
    // channel only keeps the admission loop from racing unboundedly far ahead
    // in *task count* when units are small.
    let (htx, mut hrx) =
        mpsc::channel::<tokio::task::JoinHandle<Result<PrefetchedUnit, ArrowError>>>(64);
    {
        let store = store.clone();
        let path = path.clone();
        tokio::spawn(async move {
            for episode in units {
                // One summed permit per episode: a window is an episode of
                // one (identical accounting to per-unit admission), while an
                // Hstack group's units MUST be co-admitted — the decoder holds
                // all their streams open for the lockstep hstack, so per-unit
                // permits would deadlock once the bucket ran dry mid-group.
                let total_kib: u64 = episode.iter().map(|u| u.kib).sum();
                let want = unit_permit_kib(total_kib, budget_kib);
                let permit = match sem.clone().acquire_many_owned(want).await {
                    Ok(p) => Arc::new(p),
                    Err(_) => return, // semaphore closed = consumer gone
                };
                for UnitFetch {
                    mask,
                    sel,
                    ranges,
                    kib: _,
                } in episode
                {
                    let store = store.clone();
                    let path = path.clone();
                    let permit = Arc::clone(&permit);
                    let handle = tokio::spawn(async move {
                        match store.get_ranges(&path, &ranges).await {
                            Ok(data) => Ok(PrefetchedUnit {
                                mask,
                                sel,
                                ranges,
                                data,
                                permit,
                            }),
                            Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                        }
                    });
                    if htx.send(handle).await.is_err() {
                        return; // decoder dropped (error path) — stop admitting
                    }
                }
            }
        });
    }

    // --- decoder: strictly one unit at a time (bounds decode scratch;
    // concurrency lives ONLY on the fetch side above) ---
    // Mid-stream batch adaptation (see `group_is_estimator_blind`): measured
    // decoded bytes/row carries across units and row groups, re-sizing each
    // window unit's batch rows at stream build time. Residual: the FIRST unit
    // of a blind file still decodes at the static (encoded-fallback) size —
    // adapting inside an already-built stream would need re-buildable
    // prefetched bytes. The Hstack path is excluded: its units are column
    // groups (partial rows), so their bytes/row is not comparable across
    // units or with window units.
    let mut bpr = BprTracker::default();
    for plan in rg_plans {
        let blind = group_is_estimator_blind(meta.metadata().row_group(plan.rg));
        match plan.decode {
            RgDecode::Windows(n) => {
                for _ in 0..n {
                    let batch_rows = if blind {
                        adapted_rows(plan.batch_rows, decode_budget, bpr.bytes_per_row())
                    } else {
                        plan.batch_rows
                    };
                    let mut stream =
                        match next_unit_stream(&mut hrx, &meta, plan.rg, batch_rows).await {
                            Ok(s) => s,
                            Err(e) => send_err!(e),
                        };
                    while let Some(item) = stream.next().await {
                        if blind {
                            if let Ok(b) = &item {
                                bpr.record(b);
                            }
                        }
                        let is_err = item.is_err();
                        let msg = item.map_err(|e| ArrowError::ExternalError(Box::new(e)));
                        if tx.send(msg).await.is_err() {
                            return; // consumer dropped
                        }
                        if is_err {
                            return;
                        }
                    }
                    // `stream` drops here -> window bytes freed, permits
                    // released, next fetch admitted.
                }
            }
            RgDecode::Hstack(n) => {
                // Incremental hstack (the TODO-1u fix): open ALL n column-group
                // streams at once — their COMPRESSED bytes stay resident,
                // co-admitted under one shared permit — and decode in lockstep:
                // batch-slice i of every group is glued to full width and
                // emitted before slice i+1 is decoded. Every stream is built
                // with the same batch_rows over the same whole-group rows with
                // no row selection, so batch boundaries align by construction.
                // Peak decoded retention is ~one full-width batch instead of
                // the whole decoded row group the old parked-groups hstack
                // held (which degenerated to PyArrow's whole-group retention).
                let mut streams = Vec::with_capacity(n);
                for _ in 0..n {
                    match next_unit_stream(&mut hrx, &meta, plan.rg, plan.batch_rows).await {
                        Ok(s) => streams.push(s),
                        Err(e) => send_err!(e),
                    }
                }
                loop {
                    let mut slices: Vec<RecordBatch> = Vec::with_capacity(n);
                    let mut ended = 0usize;
                    for stream in &mut streams {
                        match stream.next().await {
                            Some(Ok(b)) => slices.push(b),
                            Some(Err(e)) => send_err!(ArrowError::ExternalError(Box::new(e))),
                            None => ended += 1,
                        }
                    }
                    if ended == n {
                        break; // all groups exhausted together
                    }
                    if ended != 0 {
                        send_err!(ArrowError::ComputeError(format!(
                            "column-window batch-count mismatch in row group {}: \
                             {ended} of {n} groups ended early",
                            plan.rg
                        )));
                    }
                    let mut cols: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
                    for s in &slices {
                        cols.extend(s.columns().iter().cloned());
                    }
                    // try_new re-checks row alignment: unequal column lengths
                    // across groups fail here rather than emitting a torn batch.
                    match RecordBatch::try_new(out_schema.clone(), cols) {
                        Ok(b) => {
                            if tx.send(Ok(b)).await.is_err() {
                                return; // consumer dropped
                            }
                        }
                        Err(e) => send_err!(e),
                    }
                }
                // Streams drop here -> the group's bytes are freed and the
                // shared permit is released, waking the admission loop.
            }
        }
    }
}

/// Build an S3 `ObjectStore` from the config recovered from the pyarrow
/// `S3FileSystem` on the Python side (`fs.__reduce__()[1][0]`) so credentialed /
/// custom-endpoint (MinIO, moto) / anonymous buckets all connect identically to
/// PyArrow. Empty/None fields are treated as unset. Shared by `read_row_groups_s3`
/// and `read_metadata_s3`.
#[allow(clippy::too_many_arguments)]
fn build_s3_store(
    bucket: &str,
    region: &str,
    anonymous: bool,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    allow_http: bool,
    virtual_hosted_style: bool,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let mut sb = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_virtual_hosted_style_request(virtual_hosted_style);
    if let Some(ep) = endpoint.filter(|s| !s.is_empty()) {
        sb = sb.with_endpoint(ep);
    }
    if allow_http {
        sb = sb.with_allow_http(true);
    }
    if anonymous {
        // No signing — public buckets. Any creds are irrelevant.
        sb = sb.with_skip_signature(true);
    } else {
        // Explicit static creds if the S3FileSystem carried them; otherwise the
        // builder falls back to the AWS credential chain (env / IMDS role).
        if let Some(kid) = access_key_id.filter(|s| !s.is_empty()) {
            sb = sb.with_access_key_id(kid);
        }
        if let Some(s) = secret_access_key.filter(|s| !s.is_empty()) {
            sb = sb.with_secret_access_key(s);
        }
        if let Some(t) = session_token.filter(|s| !s.is_empty()) {
            sb = sb.with_token(t);
        }
    }
    Ok(Arc::new(sb.build()?))
}

#[pyfunction]
#[pyo3(signature = (bucket, key, region, anonymous, endpoint=None, access_key_id=None,
                    secret_access_key=None, session_token=None, allow_http=false,
                    virtual_hosted_style=false, row_groups=None, columns=None,
                    batch_size=131072, decode_budget_bytes=2*1024*1024,
                    fetch_window_mb=16, k=1, split_threshold_bytes=134217728,
                    predicate_json=None, column_fetch_mb=16,
                    prefetch_budget_mb=64))]
#[allow(clippy::too_many_arguments)]
fn read_row_groups_s3(
    py: Python<'_>,
    bucket: String,
    key: String,
    region: String,
    anonymous: bool,
    // Full S3 config, recovered from the pyarrow S3FileSystem on the Python side
    // (fs.__reduce__()[1][0]) so credentialed / custom-endpoint (MinIO, moto) /
    // anonymous buckets all decode identically to PyArrow. Empty/None → unset.
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    allow_http: bool,
    virtual_hosted_style: bool,
    row_groups: Option<Vec<usize>>,
    columns: Option<Vec<String>>,
    batch_size: usize,
    decode_budget_bytes: u64,
    fetch_window_mb: u64,
    k: usize,
    split_threshold_bytes: u64,
    // See `read_row_groups`: statistics row-group pruning only.
    predicate_json: Option<String>,
    // Compressed-byte budget per column group for the wide-schema column-windowing
    // split axis (0 disables it). See `plan_s3_units`.
    column_fetch_mb: u64,
    // Compressed bytes the prefetcher may hold in flight/buffered ahead of the
    // (single) decoder — "the bucket", shared by every unit kind. Bounds memory
    // by construction while letting enough GETs run concurrently to keep the
    // decoder fed regardless of the fetch:decode speed ratio. 0 = strictly
    // sequential (no overlap). See `drive_s3`.
    prefetch_budget_mb: u64,
) -> PyResult<ArrowStream> {
    let store = build_s3_store(
        &bucket,
        &region,
        anonymous,
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        allow_http,
        virtual_hosted_style,
    )
    .map_err(to_py)?;
    let obj_path = ObjPath::from(key);

    // Load footer + page index ONCE (Optional so a window's RowSelection can skip
    // unselected pages by byte range). Blocking async footer fetch; release the
    // GIL so sibling Python read threads (Ray's fragment pool) issue their own
    // S3 requests in parallel.
    let reader = py
        .allow_threads(|| {
            let (meta, _skipped) = shared_runtime().block_on(load_meta_s3(
                store.clone(),
                obj_path.clone(),
                PageIndexPolicy::Optional,
            ))?;
            build_s3_reader(
                store,
                obj_path,
                meta,
                row_groups,
                columns,
                batch_size,
                decode_budget_bytes,
                fetch_window_mb,
                k,
                split_threshold_bytes,
                predicate_json,
                column_fetch_mb,
                prefetch_budget_mb,
            )
        })
        .map_err(to_py)?;
    Ok(into_py_stream(Box::new(reader)))
}

/// The metadata-independent half of the S3 read: everything after the store
/// construction and footer load. Shared by [`read_row_groups_s3`] (fresh client
/// + footer per call — the original API) and
/// [`NativeParquetFile::read_row_groups`] (client and parsed footer opened once
/// and reused across a read task's calls — TODO 1r, the fix for the
/// per-file S3 setup cost on multi-file bins). Builds the projected output
/// schema up front from an empty stream (no network); reporting the projected
/// schema is what keeps it matching the projected batches at the FFI boundary.
#[allow(clippy::too_many_arguments)]
fn build_s3_reader(
    store: Arc<dyn ObjectStore>,
    obj_path: ObjPath,
    meta: ArrowReaderMetadata,
    row_groups: Option<Vec<usize>>,
    columns: Option<Vec<String>>,
    batch_size: usize,
    decode_budget_bytes: u64,
    fetch_window_mb: u64,
    k: usize,
    split_threshold_bytes: u64,
    predicate_json: Option<String>,
    column_fetch_mb: u64,
    prefetch_budget_mb: u64,
) -> Result<S3ChannelReader, ParquetError> {
    let rt = shared_runtime();
    let mask = projection_mask(meta.metadata().file_metadata().schema_descr(), &columns);
    let schema = ParquetRecordBatchStreamBuilder::new_with_metadata(
        ParquetObjectReader::new(store.clone(), obj_path.clone()),
        meta.clone(),
    )
    .with_projection(mask.clone())
    .with_row_groups(vec![])
    .build()?
    .schema()
    .clone();

    let selected: Vec<usize> = match row_groups {
        Some(v) => v,
        None => (0..meta.metadata().num_row_groups()).collect(),
    };
    // Statistics-based pruning (conservative) before any range GET is issued, so
    // pruned groups cost no S3 traffic. Same mechanism as the local path.
    let selected = apply_predicate(&meta, selected, &predicate_json);

    // K-split ONLY for a lone row group above the threshold with a page index —
    // the case Ray's fragment pool can't parallelize. Mirrors the local rule so
    // crate-K and Ray's pool never multiply. Otherwise a single driver (K=1)
    // over all selected groups in order; Ray's pool parallelizes files. Each of
    // the K streams gets its own prefetch bucket (decode parallelism is the
    // point of the split), so ~`k * prefetch_budget` compressed may be in
    // flight for this one deliberately-parallel shape.
    let split = k > 1
        && selected.len() == 1
        && meta.metadata().row_group(selected[0]).total_byte_size() as u64 >= split_threshold_bytes
        && meta.metadata().offset_index().is_some();

    // Build the per-stream sub-range lists (each becomes one drive_s3 task +
    // one channel, drained in order). How a sub-range further splits into
    // prefetchable units (row windows vs column groups) is decided per row
    // group inside the driver — see `plan_s3_units`.
    let stream_ranges: Vec<Vec<(usize, usize, usize)>> = if split {
        let rg = selected[0];
        let total_rows = meta.metadata().row_group(rg).num_rows().max(0) as usize;
        let chunk = total_rows.div_ceil(k.max(1)).max(1);
        let mut ranges = Vec::new();
        let mut start = 0usize;
        while start < total_rows {
            let len = chunk.min(total_rows - start);
            ranges.push(vec![(rg, start, len)]);
            start += len;
        }
        ranges
    } else {
        // One stream: every selected group, whole, in order.
        let subranges = selected
            .iter()
            .map(|&rg| {
                (
                    rg,
                    0usize,
                    meta.metadata().row_group(rg).num_rows().max(0) as usize,
                )
            })
            .collect();
        vec![subranges]
    };

    let roots = projected_root_indices(meta.metadata().file_metadata().schema_descr(), &columns);
    let colwindow_budget = column_fetch_mb.saturating_mul(1024 * 1024);

    // Spawn one driver per stream on the shared runtime; collect receivers in order.
    let mut receivers = Vec::with_capacity(stream_ranges.len());
    for subranges in stream_ranges {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ArrowError>>(S3_CHANNEL_DEPTH);
        receivers.push(rx);
        rt.spawn(drive_s3(
            store.clone(),
            obj_path.clone(),
            meta.clone(),
            schema.clone(),
            mask.clone(),
            roots.clone(),
            subranges,
            batch_size,
            decode_budget_bytes,
            fetch_window_mb,
            colwindow_budget,
            prefetch_budget_mb,
            tx,
        ));
    }

    Ok(S3ChannelReader {
        schema,
        receivers,
        cur: 0,
    })
}

// --------------------------------------------------------------------------- //
// Per-file native handles (TODO 1r): open once, decode many
// --------------------------------------------------------------------------- //
// The original entry points above pay a fixed setup cost on EVERY call: a fresh
// `AmazonS3Builder` client (new connection pool, no TLS session reuse) plus a
// footer — and, for decode calls, page-index — fetch. PyArrow reads one footer
// per file and shares one HTTP client across the whole read. On the #64985
// planner a read task's fragment is a multi-file *bin*, so the reader makes
// 2 calls per file (metadata at plan time, decode at read time): a 16-file S3
// bin paid 32 client builds and 32 footer round trips, measured as a 3.5×
// read-op loss vs PyArrow (findings T10). The handles below restore parity of
// mechanism: `connect_s3` builds ONE client per (bucket, config) for the whole
// task, `open_file` fetches the footer+page index ONCE per file, and
// `read_row_groups` / `metadata` reuse both.
//
// Deliberately NO global/process-level cache behind these: the handle's
// lifetime is owned by the Python caller (one read task), so there is no
// staleness (rotated credentials, replaced objects) and no unbounded growth in
// a long-lived reused worker.

/// Where a [`NativeParquetFile`]'s bytes live. Local files re-open the path per
/// reader (cheap, and `File` isn't shareable across the K-split threads anyway);
/// S3 files hold the shared client.
enum FileSource {
    Local(String),
    S3 {
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
    },
}

/// One S3 client (connection pool + credentials) for one bucket, shared across
/// every file opened through it. Construct via [`connect_s3`].
#[pyclass]
struct NativeS3Store {
    store: Arc<dyn ObjectStore>,
}

#[pymethods]
impl NativeS3Store {
    /// Open one object as a [`NativeParquetFile`]: fetches and parses the
    /// footer (and, when `page_index` — the page index) exactly once, on this
    /// store's shared client. `page_index=true` is what the S3 decode path
    /// needs (row windows skip pages via the offset index); pass `false` for
    /// metadata-only handles.
    #[pyo3(signature = (key, page_index=true))]
    fn open_file(
        &self,
        py: Python<'_>,
        key: String,
        page_index: bool,
    ) -> PyResult<NativeParquetFile> {
        let policy = if page_index {
            PageIndexPolicy::Optional
        } else {
            PageIndexPolicy::Skip
        };
        let store = self.store.clone();
        let path = ObjPath::from(key);
        // Blocking async footer fetch; release the GIL for sibling read threads.
        let (meta, skipped) = py
            .allow_threads(|| {
                shared_runtime().block_on(load_meta_s3(store.clone(), path.clone(), policy))
            })
            .map_err(to_py)?;
        Ok(NativeParquetFile {
            source: FileSource::S3 { store, path },
            meta,
            arrow_schema_skipped: skipped,
        })
    }
}

/// Build the per-bucket S3 client once. Same config contract as
/// [`read_row_groups_s3`] (recovered from the pyarrow `S3FileSystem` on the
/// Python side); the returned store is what every `open_file` shares.
#[pyfunction]
#[pyo3(signature = (bucket, region, anonymous, endpoint=None, access_key_id=None,
                    secret_access_key=None, session_token=None, allow_http=false,
                    virtual_hosted_style=false))]
#[allow(clippy::too_many_arguments)]
fn connect_s3(
    bucket: String,
    region: String,
    anonymous: bool,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    allow_http: bool,
    virtual_hosted_style: bool,
) -> PyResult<NativeS3Store> {
    let store = build_s3_store(
        &bucket,
        &region,
        anonymous,
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        allow_http,
        virtual_hosted_style,
    )
    .map_err(to_py)?;
    Ok(NativeS3Store { store })
}

/// Local counterpart of [`NativeS3Store::open_file`]: parse the footer once and
/// reuse it across `metadata()` and every `read_row_groups` call. `page_index`
/// mirrors the lean-footer-parse rule of [`open_local_reader`]: only the K-split
/// needs it, so callers pass `k > 1`.
#[pyfunction]
#[pyo3(signature = (path, page_index=false))]
fn open_parquet_file(
    py: Python<'_>,
    path: String,
    page_index: bool,
) -> PyResult<NativeParquetFile> {
    let policy = if page_index {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    // Blocking file I/O; release the GIL for sibling read threads.
    let (meta, skipped) = py
        .allow_threads(|| load_meta_local(&File::open(&path)?, policy))
        .map_err(to_py)?;
    Ok(NativeParquetFile {
        source: FileSource::Local(path),
        meta,
        arrow_schema_skipped: skipped,
    })
}

/// One opened Parquet file: the parsed footer plus (for S3) the shared client.
/// `metadata()` is free (no I/O); `read_row_groups` skips the footer fetch the
/// original entry points pay per call.
#[pyclass]
struct NativeParquetFile {
    source: FileSource,
    meta: ArrowReaderMetadata,
    arrow_schema_skipped: bool,
}

#[pymethods]
impl NativeParquetFile {
    /// The same footer summary `read_metadata` / `read_metadata_s3` return,
    /// built from the already-parsed footer — zero I/O.
    fn metadata(&self) -> ParquetFileMetadata {
        build_file_metadata(&self.meta, self.arrow_schema_skipped)
    }

    /// Replace this handle's Arrow output schema with a caller-supplied one
    /// (an Arrow C schema capsule, i.e. `pa.Schema.__arrow_c_schema__()`),
    /// rebuilding the reader metadata against the already-parsed footer —
    /// zero additional I/O on either transport.
    ///
    /// Purpose (findings M52): when the embedded arrow schema was skipped
    /// (non-UTF8 extension metadata, `arrow_schema_skipped`), the inferred
    /// storage types can differ from the extension types' storage
    /// (`list<element>` vs `large_list<item>`), forcing the Python reader
    /// into a per-batch per-column `Table.cast` (~7 µs/col/batch). Supplying
    /// the exact storage schema here makes the crate decode directly into
    /// those types, so Python re-attaches the extension labels with one
    /// zero-copy C-interface import per batch instead.
    ///
    /// The supplied schema must be plain storage types with no binary field
    /// metadata (the cloudpickle label bytes stay in Python — Rust never
    /// holds them). parquet-rs validates the schema against the parquet
    /// footer and errors on any mismatch, in which case the caller keeps
    /// today's cast path; `self.meta` is only replaced on success.
    fn with_schema_override(&mut self, schema_capsule: Bound<'_, PyCapsule>) -> PyResult<()> {
        let valid_name = schema_capsule
            .name()
            .map_err(to_py)?
            .map(|n| n.to_bytes() == b"arrow_schema")
            .unwrap_or(false);
        if !valid_name {
            return Err(PyRuntimeError::new_err(
                "with_schema_override expects an 'arrow_schema' PyCapsule \
                 (pa.Schema.__arrow_c_schema__())",
            ));
        }
        let ptr = schema_capsule.pointer() as *const FFI_ArrowSchema;
        if ptr.is_null() {
            return Err(PyRuntimeError::new_err("null arrow_schema capsule"));
        }
        // Borrowed read of the C struct: the capsule keeps ownership and will
        // run its own release callback; try_from copies into Rust types.
        let schema = Schema::try_from(unsafe { &*ptr }).map_err(to_py)?;
        let options = ArrowReaderOptions::new().with_schema(Arc::new(schema));
        self.meta = ArrowReaderMetadata::try_new(Arc::clone(self.meta.metadata()), options)
            .map_err(to_py)?;
        Ok(())
    }

    /// Decode row groups through the held footer + client. Argument semantics
    /// are identical to `read_row_groups` / `read_row_groups_s3`; the S3-only
    /// knobs (`fetch_window_mb`, `column_fetch_mb`, `prefetch_budget_mb`) are
    /// inert on a local handle, so one uniform signature serves both.
    #[pyo3(signature = (row_groups=None, columns=None, batch_size=131072,
                        decode_budget_bytes=2*1024*1024, k=1,
                        split_threshold_bytes=134217728, predicate_json=None,
                        fetch_window_mb=16, column_fetch_mb=16,
                        prefetch_budget_mb=64))]
    #[allow(clippy::too_many_arguments)]
    fn read_row_groups(
        &self,
        py: Python<'_>,
        row_groups: Option<Vec<usize>>,
        columns: Option<Vec<String>>,
        batch_size: usize,
        decode_budget_bytes: u64,
        k: usize,
        split_threshold_bytes: u64,
        predicate_json: Option<String>,
        fetch_window_mb: u64,
        column_fetch_mb: u64,
        prefetch_budget_mb: u64,
    ) -> PyResult<ArrowStream> {
        match &self.source {
            FileSource::Local(path) => {
                let (path, meta) = (path.clone(), self.meta.clone());
                let reader = py
                    .allow_threads(|| {
                        build_local_reader(
                            path,
                            meta,
                            row_groups,
                            columns,
                            batch_size,
                            decode_budget_bytes,
                            k,
                            split_threshold_bytes,
                            predicate_json,
                        )
                    })
                    .map_err(to_py)?;
                Ok(into_py_stream(reader))
            }
            FileSource::S3 { store, path } => {
                let (store, path, meta) = (store.clone(), path.clone(), self.meta.clone());
                let reader = py
                    .allow_threads(|| {
                        build_s3_reader(
                            store,
                            path,
                            meta,
                            row_groups,
                            columns,
                            batch_size,
                            decode_budget_bytes,
                            fetch_window_mb,
                            k,
                            split_threshold_bytes,
                            predicate_json,
                            column_fetch_mb,
                            prefetch_budget_mb,
                        )
                    })
                    .map_err(to_py)?;
                Ok(into_py_stream(Box::new(reader)))
            }
        }
    }
}

// --------------------------------------------------------------------------- //
// Footer-only metadata reads (Track 1): one footer parse, no data decode.
// --------------------------------------------------------------------------- //
/// Read just the Parquet footer of a local file and return schema + per-row-group
/// counts. Page index is skipped (not needed for metadata). Lets the Python reader
/// stop building a PyArrow dataset to learn the schema / row-group layout.
#[pyfunction]
fn read_metadata(py: Python<'_>, path: String) -> PyResult<ParquetFileMetadata> {
    // Blocking footer read; release the GIL for sibling Python threads.
    let (meta, skipped) = py
        .allow_threads(|| load_meta_local(&File::open(&path)?, PageIndexPolicy::Skip))
        .map_err(to_py)?;
    Ok(build_file_metadata(&meta, skipped))
}

/// S3 counterpart of [`read_metadata`]: one async footer fetch via `object_store`,
/// same connection config recovery as `read_row_groups_s3`.
#[pyfunction]
#[pyo3(signature = (bucket, key, region, anonymous, endpoint=None, access_key_id=None,
                    secret_access_key=None, session_token=None, allow_http=false,
                    virtual_hosted_style=false))]
#[allow(clippy::too_many_arguments)]
fn read_metadata_s3(
    py: Python<'_>,
    bucket: String,
    key: String,
    region: String,
    anonymous: bool,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    allow_http: bool,
    virtual_hosted_style: bool,
) -> PyResult<ParquetFileMetadata> {
    let store = build_s3_store(
        &bucket,
        &region,
        anonymous,
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        allow_http,
        virtual_hosted_style,
    )
    .map_err(to_py)?;
    let obj_path = ObjPath::from(key);
    let rt = shared_runtime();
    // Blocking async footer fetch; release the GIL for sibling Python threads.
    let (meta, skipped) = py
        .allow_threads(|| {
            rt.block_on(load_meta_s3(
                store.clone(),
                obj_path.clone(),
                PageIndexPolicy::Skip,
            ))
        })
        .map_err(to_py)?;
    Ok(build_file_metadata(&meta, skipped))
}

/// Return the row-group ids of `path` that survive `predicate_json`'s statistics
/// pruning (all of them when it's None). This is the exact selection
/// `read_row_groups` would decode; exposed so callers (and tests) can observe
/// pruning without decoding, and so the pyarrow-free reader can learn the read
/// set up front. Page index is skipped (stats live in the footer).
#[pyfunction]
#[pyo3(signature = (path, predicate_json=None))]
fn select_row_groups(
    py: Python<'_>,
    path: String,
    predicate_json: Option<String>,
) -> PyResult<Vec<usize>> {
    // Blocking footer read; release the GIL for sibling Python threads.
    let (meta, _skipped) = py
        .allow_threads(|| load_meta_local(&File::open(&path)?, PageIndexPolicy::Skip))
        .map_err(to_py)?;
    let all: Vec<usize> = (0..meta.metadata().num_row_groups()).collect();
    Ok(apply_predicate(&meta, all, &predicate_json))
}

fn to_py<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

#[pymodule]
fn ray_data_arrow_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_row_groups, m)?)?;
    m.add_function(wrap_pyfunction!(read_row_groups_s3, m)?)?;
    m.add_function(wrap_pyfunction!(read_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(read_metadata_s3, m)?)?;
    m.add_function(wrap_pyfunction!(select_row_groups, m)?)?;
    m.add_function(wrap_pyfunction!(connect_s3, m)?)?;
    m.add_function(wrap_pyfunction!(open_parquet_file, m)?)?;
    m.add_class::<ParquetFileMetadata>()?;
    m.add_class::<NativeS3Store>()?;
    m.add_class::<NativeParquetFile>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::{ColumnPath, Type};
    use std::sync::Arc;

    /// Build a leaf `ColumnDescriptor` for one physical/logical/converted type
    /// combo (the only inputs `is_plain_signed_int` reads).
    fn descr(
        physical: PhysicalType,
        logical: Option<LogicalType>,
        converted: ConvertedType,
    ) -> ColumnDescriptor {
        let ty = Type::primitive_type_builder("c", physical)
            .with_logical_type(logical)
            .with_converted_type(converted)
            .build()
            .unwrap();
        ColumnDescriptor::new(Arc::new(ty), 0, 0, ColumnPath::new(vec!["c".to_string()]))
    }

    #[test]
    fn plain_signed_ints_are_comparable() {
        // Plain INT32/INT64 (no logical/converted type).
        assert!(is_plain_signed_int(&descr(
            PhysicalType::INT64,
            None,
            ConvertedType::NONE
        )));
        assert!(is_plain_signed_int(&descr(
            PhysicalType::INT32,
            None,
            ConvertedType::NONE
        )));
        // Explicit signed-integer logical type.
        assert!(is_plain_signed_int(&descr(
            PhysicalType::INT32,
            Some(LogicalType::integer(32, true)),
            ConvertedType::NONE
        )));
        // Legacy signed converted type.
        assert!(is_plain_signed_int(&descr(
            PhysicalType::INT32,
            None,
            ConvertedType::INT_16
        )));
    }

    #[test]
    fn window_step_clamps_up_to_a_full_page() {
        // The wide/short pathology: byte budget wants a 40-row window but the
        // (single) page spans all 200 rows -> clamp up to 200 so the whole range
        // is one window (no per-window re-decode of that page).
        assert_eq!(effective_window_step(40, 200, 200), 200);
        // step >= len => the caller emits exactly one window (parity, no split).
        assert!(effective_window_step(40, 200, 200) >= 200);
    }

    #[test]
    fn window_step_keeps_splitting_a_tall_group() {
        // Tall multi-page group: window (4096 rows) already spans several 512-row
        // pages, so it is left as-is and the range still splits into windows.
        assert_eq!(effective_window_step(4096, 512, 1_000_000), 4096);
        // A window smaller than the page still gets clamped up to the page, so a
        // page is never split across windows (bounds boundary re-decode to O(1)).
        assert_eq!(effective_window_step(256, 512, 1_000_000), 512);
    }

    #[test]
    fn column_partition_splits_wide_group_under_budget() {
        // 5 columns of 100 bytes each, budget 250 -> groups of [0,1],[2,3],[4].
        let cols: Vec<(usize, u64)> = (0..5).map(|i| (i, 100)).collect();
        assert_eq!(
            partition_columns_by_budget(&cols, 250),
            vec![vec![0, 1], vec![2, 3], vec![4]]
        );
        // Ascending leaf order is preserved within and across groups (so hstack
        // reproduces schema order).
        let flat: Vec<usize> = partition_columns_by_budget(&cols, 250)
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(flat, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn column_partition_disabled_or_narrow_is_one_group() {
        let cols: Vec<(usize, u64)> = (0..5).map(|i| (i, 100)).collect();
        // budget 0 disables -> single group.
        assert_eq!(
            partition_columns_by_budget(&cols, 0),
            vec![vec![0, 1, 2, 3, 4]]
        );
        // budget larger than the total -> single group (narrow/small reads).
        assert_eq!(
            partition_columns_by_budget(&cols, 10_000),
            vec![vec![0, 1, 2, 3, 4]]
        );
        // <=1 column -> single group regardless of budget.
        assert_eq!(partition_columns_by_budget(&[(7, 999)], 1), vec![vec![7]]);
    }

    #[test]
    fn column_partition_never_splits_below_one_column() {
        // A single oversized column exceeds the budget but still gets its own group
        // (can't split a column below itself). Neighbours don't merge into it.
        let cols = vec![(0, 10), (1, 500), (2, 10)];
        assert_eq!(
            partition_columns_by_budget(&cols, 100),
            vec![vec![0], vec![1], vec![2]]
        );
    }

    #[test]
    fn slice_prefetched_serves_contained_subranges() {
        // Two prefetched chunk ranges; page reads are sub-ranges of a chunk.
        let ranges = vec![100u64..200, 300u64..350];
        let data = vec![
            Bytes::from((0..100u8).collect::<Vec<u8>>()),
            Bytes::from((0..50u8).collect::<Vec<u8>>()),
        ];
        // Exact chunk.
        assert_eq!(
            slice_prefetched(&ranges, &data, &(100..200)).unwrap(),
            data[0]
        );
        // Interior page of the first chunk: bytes at offsets 10..15 within it.
        assert_eq!(
            slice_prefetched(&ranges, &data, &(110..115))
                .unwrap()
                .as_ref(),
            &[10, 11, 12, 13, 14]
        );
        // Sub-range of the second chunk.
        assert_eq!(
            slice_prefetched(&ranges, &data, &(340..350))
                .unwrap()
                .as_ref(),
            &(40..50u8).collect::<Vec<u8>>()[..]
        );
    }

    #[test]
    fn slice_prefetched_rejects_uncached_or_straddling_ranges() {
        let ranges = vec![100u64..200, 300u64..350];
        let data = vec![Bytes::from(vec![0u8; 100]), Bytes::from(vec![0u8; 50])];
        // Not prefetched at all.
        assert!(slice_prefetched(&ranges, &data, &(0..10)).is_none());
        // Straddles the gap between the two chunks -> contained in neither.
        assert!(slice_prefetched(&ranges, &data, &(150..320)).is_none());
        // Runs past the end of a chunk.
        assert!(slice_prefetched(&ranges, &data, &(190..201)).is_none());
    }

    #[test]
    fn window_step_zero_means_whole_range() {
        // fetch_window_mb == 0 -> window_rows == 0 -> one window over the range,
        // regardless of page size.
        assert_eq!(effective_window_step(0, 512, 8192), 8192);
        assert_eq!(effective_window_step(0, 0, 1), 1);
    }

    /// Write a 2-column, 1000-row parquet (data pages capped at ~100 rows, page
    /// index on) into memory and load its metadata WITH the page index — the
    /// fixture for the window/unit planning tests below.
    fn windowed_fixture() -> (ArrowReaderMetadata, Bytes) {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let a = Int64Array::from((0..1000i64).collect::<Vec<_>>());
        let b = StringArray::from((0..1000).map(|i| format!("row-{i}")).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(100)
            .set_write_batch_size(100)
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let buf = Bytes::from(buf);
        let meta =
            ArrowReaderMetadata::load(&buf, reader_options(PageIndexPolicy::Required)).unwrap();
        (meta, buf)
    }

    #[test]
    fn window_plan_whole_group_equals_whole_chunks() {
        let (meta, _buf) = windowed_fixture();
        let md = meta.metadata();
        let leaves = vec![0usize, 1];
        let (ranges, kib) = window_fetch_plan(md, 0, &leaves, 0, 1000);
        let (want_ranges, want_kib) = group_fetch_plan(md.row_group(0), &leaves);
        assert_eq!(ranges, want_ranges);
        assert_eq!(kib, want_kib);
    }

    #[test]
    fn window_plan_subwindow_fetches_less_than_the_chunk() {
        let (meta, _buf) = windowed_fixture();
        let md = meta.metadata();
        let leaves = vec![0usize, 1];
        let (_, whole_kib) = window_fetch_plan(md, 0, &leaves, 0, 1000);
        // A 100-row window out of 1000 (10 pages/column) must fetch strictly
        // less than the whole chunks...
        let (ranges, kib) = window_fetch_plan(md, 0, &leaves, 400, 100);
        assert!(kib < whole_kib, "window kib {kib} >= whole {whole_kib}");
        // ...and every planned range must sit inside one of the column chunks.
        for r in &ranges {
            let contained = leaves.iter().any(|&l| {
                let (s, len) = md.row_group(0).column(l).byte_range();
                r.start >= s && r.end <= s + len
            });
            assert!(contained, "range {r:?} escapes the column chunks");
        }
    }

    /// The load-bearing guarantee: decode served ONLY from a window's planned
    /// ranges must succeed (a request outside the plan is a hard
    /// "not prefetched" error) and reproduce exactly the window's rows — for
    /// windows that tile the group at non-page-aligned offsets.
    #[test]
    fn window_plans_serve_every_decoder_request() {
        use arrow::array::Int64Array;

        let (meta, buf) = windowed_fixture();
        let md = meta.metadata();
        let leaves = vec![0usize, 1];
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let sem = Arc::new(Semaphore::new(usize::MAX >> 3));
        let mut next_val = 0i64;
        for (w, wlen) in [(0usize, 250usize), (250, 250), (500, 400), (900, 100)] {
            let (ranges, _kib) = window_fetch_plan(md, 0, &leaves, w, wlen);
            let data: Vec<Bytes> = ranges
                .iter()
                .map(|r| buf.slice(r.start as usize..r.end as usize))
                .collect();
            let permit = rt.block_on(sem.clone().acquire_many_owned(1)).unwrap();
            let reader = PrefetchedReader {
                ranges,
                data,
                meta: Arc::clone(meta.metadata()),
                _permit: Arc::new(permit),
            };
            let batches: Vec<RecordBatch> = rt.block_on(async {
                let mut stream =
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
                        .with_row_groups(vec![0])
                        .with_batch_size(97)
                        .with_projection(ProjectionMask::all())
                        .with_row_selection(RowSelection::from(vec![
                            RowSelector::skip(w),
                            RowSelector::select(wlen),
                        ]))
                        .build()
                        .unwrap();
                let mut out = Vec::new();
                while let Some(b) = stream.next().await {
                    out.push(b.unwrap());
                }
                out
            });
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, wlen, "window ({w},{wlen}) yielded {rows} rows");
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..col.len() {
                    assert_eq!(col.value(i), next_val);
                    next_val += 1;
                }
            }
        }
        assert_eq!(next_val, 1000);
    }

    /// Write a single-column Utf8 parquet of `n` rows cycling through 4
    /// distinct 4 KiB values — dictionary-encoded by default, so the footer's
    /// encoded size (~dict + indices) understates the decoded size ~500x: the
    /// estimator-blind expansion shape for the adaptation tests below.
    fn dict_string_file(n: usize, rows_per_group: usize, dictionary: bool) -> Vec<u8> {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let vals: Vec<String> = (0..4u8).map(|i| i.to_string().repeat(4096)).collect();
        let col: Vec<&str> = (0..n).map(|i| vals[i % 4].as_str()).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(col))]).unwrap();
        let props = WriterProperties::builder()
            .set_dictionary_enabled(dictionary)
            .set_max_row_group_size(rows_per_group)
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    }

    fn write_temp(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("rrs_{}_{}.parquet", name, std::process::id()));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// Every value is 4096 repeats of one ASCII digit; check each row against
    /// its expected cycle position so parity failures point at a row index.
    fn assert_dict_string_content(batches: &[RecordBatch], n: usize) {
        use arrow::array::{Array, StringArray};
        let mut row = 0usize;
        for b in batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                let v = col.value(i);
                assert_eq!(v.len(), 4096, "row {row}: bad length");
                assert_eq!(
                    v.as_bytes()[0],
                    b'0' + (row % 4) as u8,
                    "row {row}: wrong value"
                );
                row += 1;
            }
        }
        assert_eq!(row, n, "row count mismatch");
    }

    #[test]
    fn estimator_blind_detects_dict_byte_array_only() {
        // Dict strings -> blind.
        let buf = Bytes::from(dict_string_file(1000, 1_000_000, true));
        let meta = ArrowReaderMetadata::load(&buf, reader_options(PageIndexPolicy::Skip)).unwrap();
        assert!(group_is_estimator_blind(meta.metadata().row_group(0)));
        // Same data with dictionary encoding off -> not blind.
        let buf = Bytes::from(dict_string_file(1000, 1_000_000, false));
        let meta = ArrowReaderMetadata::load(&buf, reader_options(PageIndexPolicy::Skip)).unwrap();
        assert!(!group_is_estimator_blind(meta.metadata().row_group(0)));
    }

    #[test]
    fn adapted_rows_clamps_both_ways() {
        let mib = 1024 * 1024u64;
        // 4 KiB/row measured, 1 MiB budget -> 256 rows.
        assert_eq!(adapted_rows(131_072, mib, Some(4096.0)), 256);
        // Measurement smaller than the static estimate implies -> upper clamp.
        assert_eq!(adapted_rows(500, mib, Some(1.0)), 500);
        // Fat rows -> floor.
        assert_eq!(adapted_rows(131_072, mib, Some(mib as f64)), MIN_BATCH_ROWS);
        // Nothing measured -> static.
        assert_eq!(adapted_rows(777, mib, None), 777);
    }

    /// M50 residual (a), the fix under test: on a dict-string group the static
    /// estimator falls back to encoded bytes (~8 B/row here), so it would
    /// decode ALL rows as one ~80 MiB batch against a 1 MiB budget. Adaptation
    /// must instead yield one MIN_BATCH_ROWS probe, then budget-sized batches.
    #[test]
    fn seq_reader_adapts_blind_dict_string_batches() {
        let n = 20_000usize;
        let path = write_temp("adapt1", &dict_string_file(n, 1_000_000, true));
        let budget = 1024 * 1024u64;
        let reader = open_local_reader(
            path.to_str().unwrap().to_string(),
            None,
            None,
            131_072,
            budget,
            1,
            u64::MAX,
            None,
        )
        .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        std::fs::remove_file(&path).ok();

        assert_eq!(batches[0].num_rows(), MIN_BATCH_ROWS, "probe first");
        for (i, b) in batches[1..].iter().enumerate() {
            let sz = b.get_array_memory_size() as u64;
            assert!(
                sz <= budget + budget / 2,
                "batch {}: {} bytes > 1.5x budget",
                i + 1,
                sz
            );
        }
        // ...and not degenerate: ~budget/4KiB = 256 rows, not another probe.
        assert!(batches[1].num_rows() >= 128, "over-shrunk: {}", batches[1].num_rows());
        assert_dict_string_content(&batches, n);
    }

    /// The measurement carries across row groups: only the very first blind
    /// group pays a probe; later groups open already adapted.
    #[test]
    fn seq_reader_probe_carries_across_groups() {
        let n = 20_000usize;
        let path = write_temp("adapt2", &dict_string_file(n, 5_000, true)); // 4 groups
        let budget = 1024 * 1024u64;
        let reader = open_local_reader(
            path.to_str().unwrap().to_string(),
            None,
            None,
            131_072,
            budget,
            1,
            u64::MAX,
            None,
        )
        .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        std::fs::remove_file(&path).ok();

        let probes = batches
            .iter()
            .filter(|b| b.num_rows() == MIN_BATCH_ROWS)
            .count();
        assert_eq!(probes, 1, "exactly one probe across 4 groups");
        for (i, b) in batches[1..].iter().enumerate() {
            let sz = b.get_array_memory_size() as u64;
            assert!(
                sz <= budget + budget / 2,
                "batch {}: {} bytes > 1.5x budget",
                i + 1,
                sz
            );
        }
        assert_dict_string_content(&batches, n);
    }

    /// Non-blind groups keep today's static sizing exactly: with dictionary
    /// encoding off the encoded size ~= decoded size, one reader per group,
    /// no probe batch.
    #[test]
    fn seq_reader_leaves_plain_groups_alone() {
        let n = 2_000usize;
        let path = write_temp("adapt3", &dict_string_file(n, 1_000_000, false));
        let budget = 1024 * 1024u64;
        let reader = open_local_reader(
            path.to_str().unwrap().to_string(),
            None,
            None,
            131_072,
            budget,
            1,
            u64::MAX,
            None,
        )
        .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        std::fs::remove_file(&path).ok();
        // Static sizing: ~4 KiB/row encoded -> 256-row batches, and the first
        // batch is NOT a 32-row probe.
        assert!(batches[0].num_rows() > MIN_BATCH_ROWS, "no probe on plain groups");
        assert_dict_string_content(&batches, n);
    }

    #[test]
    fn byte_budget_rows_floor_yields_to_the_budget() {
        // The K8 defect: a 1 MiB/row fat-string group under the 32 MiB default
        // budget must decode ~32-row batches — the old 2048-row floor made this
        // 2048 rows (a 2 GiB batch), silently voiding the knob.
        let mib = 1024 * 1024;
        assert_eq!(
            byte_budget_rows(1000 * mib, 1000, 131072, 32 * mib as u64),
            32
        );
        // 64 KiB/row, 1 MiB budget -> 16 rows... clamped up to the 32-row floor
        // (per-batch overhead guard), the only place the floor still binds.
        assert_eq!(
            byte_budget_rows(1000 * 64 * 1024, 1000, 131072, mib as u64),
            32
        );
        // Narrow schema: budget_rows huge -> clamped to the caller's ask.
        assert_eq!(
            byte_budget_rows(8000, 1000, 131072, 32 * mib as u64),
            131072
        );
        // Unknown rows -> requested (unchanged behavior).
        assert_eq!(byte_budget_rows(0, 0, 4096, 1), 4096);
    }

    /// M43/M49: the footer's encoded size understates decoded size by the
    /// dictionary expansion ratio, so batches sized from it overshot the
    /// decode budget by that ratio. For fixed-width types the decoded size is
    /// exactly `num_values * width` from the footer — verify the estimator
    /// uses it, and that batch sizing shrinks accordingly.
    #[test]
    fn decoded_estimate_expands_dict_encoded_fixed_width() {
        use arrow::array::Float64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ArrowReaderMetadata;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        // 100k float64 rows drawn from a 4-value pool: dictionary-encodes to
        // ~indices (2 bits/row RLE) + a 32-byte dict, so encoded-uncompressed
        // is tiny while decoded is exactly 800 KB.
        let rows = 100_000usize;
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
        let vals = Float64Array::from((0..rows).map(|i| (i % 4) as f64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
        let mut buf = Vec::new();
        let mut w =
            ArrowWriter::try_new(&mut buf, schema, Some(WriterProperties::builder().build()))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let meta =
            ArrowReaderMetadata::load(&Bytes::from(buf), reader_options(PageIndexPolicy::Skip))
                .unwrap();
        let rgm = meta.metadata().row_group(0);

        let decoded = rows as i64 * 8;
        assert!(
            rgm.total_byte_size() < decoded / 4,
            "fixture not dict-compressed: encoded {} vs decoded {decoded}",
            rgm.total_byte_size()
        );
        assert!(decoded_estimate_bytes(rgm) >= decoded);

        // Budget = 1/10 of decoded -> ~rows/10 per batch. The old encoded-based
        // sizing would have clamped to `requested` (expansion-sized batches).
        let eff = group_batch_rows(rgm, 1 << 20, (decoded / 10) as u64);
        assert!(
            (rows / 20..=rows / 5).contains(&eff),
            "eff {eff} not within 2x of rows/10"
        );
        assert!(
            byte_budget_rows(
                rgm.total_byte_size(),
                rgm.num_rows(),
                1 << 20,
                (decoded / 10) as u64
            ) > 4 * eff
        );
    }

    /// M53: the schema-override path (`with_schema_override`) relies on two
    /// parquet-rs behaviors — (1) a supplied schema may widen a list column to
    /// `LargeList` (i64 offsets) and the decode honors it, and (2) the supplied
    /// schema's nested field names must match the parquet-inferred ones
    /// (`element`), which is why the override keeps the crate's names and the
    /// Python side relabels afterwards (names live in the schema, not the
    /// buffers). If a parquet upgrade changes either, this fails loudly.
    #[test]
    fn schema_override_widens_list_to_large_list() {
        use arrow::array::{Array, Float32Array, LargeListArray, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        // A list<element: float32> column — the parquet-inferred storage of a
        // Ray tensor column whose embedded arrow schema was skipped.
        let child = Arc::new(Field::new("element", DataType::Float32, true));
        let values = Float32Array::from((0..40).map(|i| i as f32).collect::<Vec<_>>());
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat(4).take(10));
        let list = ListArray::new(child.clone(), offsets, Arc::new(values), None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::List(child.clone()),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)]).unwrap();
        let mut buf = Vec::new();
        let mut w =
            ArrowWriter::try_new(&mut buf, schema, Some(WriterProperties::builder().build()))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let bytes = Bytes::from(buf);

        // Load like the tensor path does: embedded arrow schema skipped.
        let meta = ArrowReaderMetadata::load(
            &bytes,
            reader_options(PageIndexPolicy::Skip).with_skip_arrow_metadata(true),
        )
        .unwrap();
        assert_eq!(
            meta.schema().field(0).data_type(),
            &DataType::List(child.clone())
        );

        // (1) Supplied storage schema: same layout, i64 offsets, crate's child
        // name. Decode must emit LargeList with the values intact.
        let big = Schema::new(vec![Field::new(
            "t",
            DataType::LargeList(child.clone()),
            false,
        )]);
        let meta2 = ArrowReaderMetadata::try_new(
            Arc::clone(meta.metadata()),
            ArrowReaderOptions::new().with_schema(Arc::new(big)),
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), meta2)
            .build()
            .unwrap();
        let out: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::LargeList(child.clone())
        );
        let ll = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();
        assert_eq!(ll.len(), 10);
        let vals = ll.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(vals.len(), 40);
        assert_eq!(vals.value(7), 7.0);

        // (2) A child field name differing from the inferred one ("item", the
        // pyarrow extension-storage name) is rejected at some stage of the
        // decode — the exact stage moved across parquet versions, so accept
        // any of try_new / build / first-batch failing.
        let renamed = Arc::new(Field::new("item", DataType::Float32, true));
        let bad = Schema::new(vec![Field::new("t", DataType::LargeList(renamed), false)]);
        let failed = ArrowReaderMetadata::try_new(
            Arc::clone(meta.metadata()),
            ArrowReaderOptions::new().with_schema(Arc::new(bad)),
        )
        .map(|m| {
            ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), m)
                .build()
                .map(|r| {
                    r.collect::<Vec<_>>()
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                })
        });
        let ok = match failed {
            Err(_) => true,
            Ok(Err(_)) => true,
            Ok(Ok(Err(_))) => true,
            Ok(Ok(Ok(_))) => false,
        };
        assert!(ok, "child-name mismatch unexpectedly accepted");
    }

    #[test]
    fn oversized_unit_never_drains_the_bucket() {
        // T6: a unit >= the budget used to take the whole semaphore, serializing
        // fetch behind decode. It must now cap at half so a second unit fits.
        assert_eq!(unit_permit_kib(500_000, 64 * 1024), 32 * 1024);
        assert_eq!(unit_permit_kib(64 * 1024, 64 * 1024), 32 * 1024);
        // A normal-sized unit takes exactly its size.
        assert_eq!(unit_permit_kib(16 * 1024, 64 * 1024), 16 * 1024);
        // Zero-size unit still needs one permit to be ordered by the bucket.
        assert_eq!(unit_permit_kib(0, 64 * 1024), 1);
        // Degenerate budget (prefetch disabled) -> strict one-at-a-time, as before.
        assert_eq!(unit_permit_kib(500, 1), 1);
    }

    /// Fixture for the M20 mis-selection: a TALL row group with one fat string
    /// column (1 KiB/row, uncompressed, multi-page) and one small int column.
    /// Every projected root exceeds a small column budget, so the old planner
    /// column-grouped it (Hstack = whole decoded group retained); the fixed
    /// planner must row-window it because the windows actually split.
    fn tall_fat_col_fixture() -> ArrowReaderMetadata {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_writer::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("fat", DataType::Utf8, false),
        ]));
        let n = 2000usize;
        let a = Int64Array::from((0..n as i64).collect::<Vec<_>>());
        let payload = "x".repeat(1024);
        let fat = StringArray::from((0..n).map(|_| payload.clone()).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(fat)]).unwrap();
        let props = WriterProperties::builder()
            .set_data_page_row_count_limit(100)
            .set_write_batch_size(100)
            // Dictionary encoding would collapse the repeated payload to a
            // few KiB on disk; plain encoding keeps the column fat COMPRESSED
            // too, which is what the byte-denominated window math sees.
            .set_dictionary_enabled(false)
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let buf = Bytes::from(buf);
        ArrowReaderMetadata::load(&buf, reader_options(PageIndexPolicy::Required)).unwrap()
    }

    #[test]
    fn tall_fat_columns_row_window_instead_of_hstack() {
        // The M20 regression test: ~2 MiB of 1 KiB rows, column budget 1 byte
        // (every root its own group — the strongest Hstack trigger), fetch
        // window 1 MiB. Windows split the range, so they must win.
        let meta = tall_fat_col_fixture();
        let mask = ProjectionMask::all();
        let roots = vec![0usize, 1];
        let (plans, units) =
            plan_s3_units(&meta, &mask, &roots, &[(0, 0, 2000)], 131072, 2 << 20, 1, 1);
        assert_eq!(plans.len(), 1);
        match plans[0].decode {
            RgDecode::Windows(n) => assert!(n > 1, "expected a real split, got {n} window(s)"),
            RgDecode::Hstack(_) => panic!("tall fat-column group mis-selected Hstack again"),
        }
        // Every unit is a row window over ALL projected columns, each its own
        // single-unit admission episode.
        assert!(units.iter().all(|e| e.len() == 1));
        assert!(units.iter().flatten().all(|u| u.sel.is_some()));

        // Escape hatch: windowing explicitly disabled (fetch_window_mb == 0)
        // makes windows inert -> the column-group axis may fire again.
        let (plans, _units) =
            plan_s3_units(&meta, &mask, &roots, &[(0, 0, 2000)], 131072, 2 << 20, 0, 1);
        assert!(matches!(plans[0].decode, RgDecode::Hstack(2)));
    }

    #[test]
    fn plan_chooses_windows_for_narrow_and_hstack_for_wide() {
        let (meta, _buf) = windowed_fixture();
        let roots = vec![0usize, 1];
        let mask = ProjectionMask::all();
        // Generous column budget -> not wide -> row windows (single window here:
        // the fixture is tiny, so the byte-budget window covers all rows).
        let (plans, units) = plan_s3_units(
            &meta,
            &mask,
            &roots,
            &[(0, 0, 1000)],
            131072,
            2 << 20,
            16,
            1 << 30,
        );
        assert_eq!(plans.len(), 1);
        assert!(matches!(plans[0].decode, RgDecode::Windows(1)));
        assert_eq!(units[0][0].sel, Some((0, 1000)));
        // 1-byte column budget -> every root its own group -> hstack of 2.
        let (plans, units) = plan_s3_units(
            &meta,
            &mask,
            &roots,
            &[(0, 0, 1000)],
            131072,
            2 << 20,
            16,
            1,
        );
        assert!(matches!(plans[0].decode, RgDecode::Hstack(2)));
        // The 2 column-group units form ONE admission episode (co-admitted
        // under a single summed permit for the lockstep hstack).
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].len(), 2);
        assert!(units.iter().flatten().all(|u| u.sel.is_none() && u.kib > 0));
        // A K-split style partial sub-range must NEVER column-window (it is a
        // tall group split by rows), even under a tiny budget.
        let (plans, units) = plan_s3_units(
            &meta,
            &mask,
            &roots,
            &[(0, 500, 500)],
            131072,
            2 << 20,
            16,
            1,
        );
        assert!(matches!(plans[0].decode, RgDecode::Windows(_)));
        assert_eq!(units[0][0].sel, Some((500, 500)));
    }

    #[test]
    fn unsigned_and_nonint_logical_types_are_not_comparable() {
        // Unsigned: a u32 max is stored as the i32 bit pattern -1, so reading
        // the stat as signed inverts min/max and would wrongly prune. Reject.
        assert!(!is_plain_signed_int(&descr(
            PhysicalType::INT32,
            Some(LogicalType::integer(32, false)),
            ConvertedType::NONE
        )));
        assert!(!is_plain_signed_int(&descr(
            PhysicalType::INT64,
            Some(LogicalType::integer(64, false)),
            ConvertedType::NONE
        )));
        // Legacy unsigned converted type (logical absent).
        assert!(!is_plain_signed_int(&descr(
            PhysicalType::INT32,
            None,
            ConvertedType::UINT_32
        )));
        // Date is INT32-backed but not a plain integer value — reject.
        assert!(!is_plain_signed_int(&descr(
            PhysicalType::INT32,
            Some(LogicalType::Date),
            ConvertedType::NONE
        )));
    }
}
