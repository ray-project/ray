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
//! Within a single stream, consecutive fetch windows are pipelined by
//! `prefetch_windows` (default 2): window N+1's GET is issued while window N
//! decodes, hiding S3 first-byte latency behind decode without staging the whole
//! row group. depth=1 restores the strictly-serial windows. This is orthogonal to
//! K: K adds parallel streams (spatial split), prefetch overlaps fetch/decode
//! within one stream. Both stay memory-bounded knobs
//! (`≈ k * prefetch_windows * fetch_window` compressed in flight).
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
//!                      split_threshold_bytes=128*1024*1024, prefetch_windows=2)

mod predicate;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::sync::mpsc::{sync_channel, Receiver};
use std::thread;

use crate::predicate::{can_match, ColStats, Pred, Value};
use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use parquet::schema::types::ColumnDescriptor;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
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
/// Choose a batch row count so `rows * bytes_per_row ~= budget_bytes`, using the
/// row group's uncompressed size / row count from the footer. `requested` is the
/// upper clamp (a narrow schema never grows past the caller's ask) and 2048 is the
/// lower clamp (a very wide schema never collapses to a pathologically tiny batch).
/// This is what keeps the decoded working set flat across schemas.
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
    budget_rows.clamp(2048, requested.max(2048))
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

/// Ordered leaf-column indices for a projection (flat schema: leaf index == root
/// field index == column-chunk index). Mirrors `projection_mask`'s name matching so
/// the two always agree on which columns are read.
fn projected_leaf_indices(
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

/// `(leaf index, compressed size)` for the projected columns of a row group, in
/// ascending leaf order — the input to `partition_columns_by_budget`.
fn projected_col_sizes(rgm: &RowGroupMetaData, leaves: &[usize]) -> Vec<(usize, u64)> {
    leaves
        .iter()
        .map(|&i| (i, rgm.column(i).compressed_size().max(0) as u64))
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
fn build_file_metadata(meta: &ArrowReaderMetadata, arrow_schema_skipped: bool) -> ParquetFileMetadata {
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
        }
    }

    fn build_group_reader(&self, rg: usize) -> Result<ParquetRecordBatchReader, ParquetError> {
        let rgm = self.meta.metadata().row_group(rg);
        let eff = byte_budget_rows(
            rgm.total_byte_size(),
            rgm.num_rows(),
            self.batch_clamp,
            self.budget_bytes,
        );
        ParquetRecordBatchReaderBuilder::new_with_metadata(
            File::open(&self.path)?,
            self.meta.clone(),
        )
        .with_batch_size(eff)
        .with_row_groups(vec![rg])
        .with_projection(self.mask.clone())
        .build()
    }
}

impl Iterator for RowGroupSeqReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next() {
                    Some(batch) => return Some(batch),
                    None => self.current = None,
                }
            }
            if self.pos >= self.row_groups.len() {
                return None;
            }
            let rg = self.row_groups[self.pos];
            self.pos += 1;
            match self.build_group_reader(rg) {
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
        let rgm = meta.metadata().row_group(rg);
        let total_rows = rgm.num_rows().max(0) as usize;
        let eff = byte_budget_rows(
            rgm.total_byte_size(),
            rgm.num_rows(),
            batch_size,
            budget_bytes,
        );
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

/// Per-window decoded buffer, used by the `prefetch_windows` pipeline. Kept at 1:
/// the cross-window prefetch (fetching window N+1 while N decodes) is what hides S3
/// latency; we do NOT also want each in-flight window buffering several decoded
/// batches, which would multiply the decode transient. So each window may hold at
/// most one decoded batch, and `prefetch_windows` controls how many windows are in
/// flight at once.
const S3_WINDOW_CHANNEL_DEPTH: usize = 1;

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

/// Spawn a background task that fetches + decodes ONE fetch window — rows
/// `[w, w+wlen)` within row group `rg` — streaming its batches into a bounded
/// channel, and return the receiver. Building the stream is cheap (metadata is
/// already loaded); the S3 fetch for this window's pages is issued when the task
/// first polls the stream. So the moment this returns, that fetch is in flight on
/// the runtime — which is what lets the caller prefetch window N+1 while draining
/// window N. `S3_WINDOW_CHANNEL_DEPTH` bounds how far ahead a single window decodes.
fn spawn_window(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    meta: ArrowReaderMetadata,
    mask: ProjectionMask,
    rg: usize,
    w: usize,
    wlen: usize,
    batch_rows: usize,
) -> mpsc::Receiver<Result<RecordBatch, ArrowError>> {
    let (tx, rx) = mpsc::channel::<Result<RecordBatch, ArrowError>>(S3_WINDOW_CHANNEL_DEPTH);
    tokio::spawn(async move {
        // Select rows [w, w+wlen) WITHIN this row group (we restrict to `rg`), so
        // only this window's pages are fetched (page index skips the rest by byte
        // range).
        let sel = RowSelection::from(vec![RowSelector::skip(w), RowSelector::select(wlen)]);
        let reader = ParquetObjectReader::new(store, path);
        let built = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta)
            .with_row_groups(vec![rg])
            .with_row_selection(sel)
            .with_batch_size(batch_rows)
            .with_projection(mask)
            .build();
        let mut stream = match built {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(ArrowError::ExternalError(Box::new(e)))).await;
                return;
            }
        };
        while let Some(item) = stream.next().await {
            let is_err = item.is_err();
            let msg = item.map_err(|e| ArrowError::ExternalError(Box::new(e)));
            if tx.send(msg).await.is_err() {
                return; // consumer dropped
            }
            if is_err {
                return;
            }
        }
    });
    rx
}

/// Drive one unit (a list of contiguous `(rg, start, len)` sub-ranges, in order)
/// over the async object store, sending decoded batches to `tx` in row order.
///
/// The unit's rows are sliced into fetch windows, each holding ~`fetch_window_mb`
/// of compressed bytes (knob 2); the decode batch inside a window is byte-budgeted
/// (knob 1). `prefetch_windows` (knob 3) is the pipeline depth: we keep that many
/// window decoders in flight at once via [`spawn_window`], draining their channels
/// in window order. depth=1 is the old strictly-serial behavior (fetch W0, decode
/// W0, fetch W1, ...); depth=2 issues window N+1's S3 fetch while window N decodes,
/// hiding the fetch latency behind the decode. Resident memory stays bounded to
/// ~`prefetch_windows` windows of compressed bytes (plus one decoded batch each) —
/// far below a whole-row-group prefetch, and still a knob.
#[allow(clippy::too_many_arguments)]
async fn drive_unit(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    meta: ArrowReaderMetadata,
    mask: ProjectionMask,
    subranges: Vec<(usize, usize, usize)>,
    budget_bytes: u64,
    batch_clamp: usize,
    fetch_window_mb: u64,
    prefetch_windows: usize,
    tx: mpsc::Sender<Result<RecordBatch, ArrowError>>,
) {
    // Flatten every (row group, window) this unit will read into one ordered list
    // of specs. We enumerate here only — no fetch happens until a window is spawned.
    let mut specs: Vec<(usize, usize, usize, usize)> = Vec::new(); // (rg, w, wlen, batch_rows)
    for (rg, start, len) in subranges {
        let rgm = meta.metadata().row_group(rg);
        let batch_rows = byte_budget_rows(
            rgm.total_byte_size(),
            rgm.num_rows(),
            batch_clamp,
            budget_bytes,
        );
        let window_rows = window_rows_for(rgm, fetch_window_mb);
        let end = start + len;
        // Never window below the coarsest column's largest page (see max_page_rows):
        // a sub-page window re-decodes that page in every window it overlaps. This
        // collapses wide/short row groups (one page per column) to a single window
        // — parity with no windowing — while tall multi-page groups still split.
        let step = effective_window_step(window_rows, max_page_rows(meta.metadata(), rg), len);
        let mut w = start;
        while w < end {
            let wlen = step.min(end - w);
            specs.push((rg, w, wlen, batch_rows));
            w += wlen;
        }
    }

    // Bounded look-ahead: keep at most `depth` window decoders in flight, draining
    // their channels in spec order so output stays row-ordered while up to `depth`
    // windows fetch+decode concurrently.
    let depth = prefetch_windows.max(1);
    let mut in_flight: VecDeque<mpsc::Receiver<Result<RecordBatch, ArrowError>>> = VecDeque::new();
    let mut next = 0usize;
    // Prime the pipeline: start `depth` window fetches concurrently.
    while next < specs.len() && in_flight.len() < depth {
        let (rg, w, wlen, br) = specs[next];
        in_flight.push_back(spawn_window(
            store.clone(),
            path.clone(),
            meta.clone(),
            mask.clone(),
            rg,
            w,
            wlen,
            br,
        ));
        next += 1;
    }
    while let Some(mut rx) = in_flight.pop_front() {
        while let Some(item) = rx.recv().await {
            let is_err = item.is_err();
            if tx.send(item).await.is_err() {
                return; // consumer dropped
            }
            if is_err {
                return;
            }
        }
        // Front window exhausted — top the pipeline back up to `depth`, so window
        // N+depth's fetch starts as soon as window N's finishes draining.
        if next < specs.len() {
            let (rg, w, wlen, br) = specs[next];
            in_flight.push_back(spawn_window(
                store.clone(),
                path.clone(),
                meta.clone(),
                mask.clone(),
                rg,
                w,
                wlen,
                br,
            ));
            next += 1;
        }
    }
}

/// Drive selected row groups for a WIDE projection by **column-windowing**: within
/// each row group, read the projected columns in sequential groups (each under the
/// compressed-byte budget), collect every group's batches, then hstack them
/// position-wise into full-width batches sent in row order.
///
/// Why sequential groups: the async reader's `InMemoryRowGroup` fetches *every*
/// projected column chunk for a row group into memory before decoding and holds them
/// all resident. For a wide schema that whole-group compressed buffer is the entire
/// S3 memory regression (PyArrow releases column chunks as it decodes; we didn't).
/// Reading one column group at a time — dropping its stream before the next fetches —
/// bounds resident compressed bytes to ~`budget` instead of the whole group. Peak =
/// `budget` + the fully decoded row group, and the decoded row group is the output we
/// must produce anyway (PyArrow holds it too), so this asymptotes to PyArrow parity.
///
/// Columns are independent, so unlike row-windowing this NEVER re-decodes a page.
/// All groups read the same rows with the same `batch_size`, so their batches align
/// 1:1 and hstack by position; ascending-leaf group order reproduces schema order.
#[allow(clippy::too_many_arguments)]
/// One column group's compressed bytes, prefetched from S3, plus the
/// byte-budget permits it holds. Dropping this (after the group is decoded)
/// frees the bytes AND releases the permits — which is what wakes the
/// admission loop to launch the next group's fetch. That drop-to-wake handoff
/// is the whole backpressure mechanism: memory pressure stays ~constant at
/// `prefetch_budget` compressed bytes without any explicit signalling code.
struct PrefetchedGroup {
    mask: ProjectionMask,
    ranges: Vec<Range<u64>>,
    data: Vec<Bytes>,
    _permit: tokio::sync::OwnedSemaphorePermit,
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

/// `AsyncFileReader` that serves a column group's page reads from its
/// prefetched buffers instead of S3. The decode stream built on top of this
/// never touches the network — the fetch already happened, budget-gated, in
/// the admission loop. Requests are always sub-ranges of whole column chunks
/// (page reads within a chunk), so containment lookup suffices.
struct PrefetchedReader {
    ranges: Vec<Range<u64>>,
    data: Vec<Bytes>,
    meta: Arc<ParquetMetaData>,
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

async fn drive_colwindowed(
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    meta: ArrowReaderMetadata,
    out_schema: SchemaRef,
    leaves: Vec<usize>,
    selected: Vec<usize>,
    budget_bytes: u64,
    batch_clamp: usize,
    decode_budget: u64,
    prefetch_budget_mb: u64,
    tx: mpsc::Sender<Result<RecordBatch, ArrowError>>,
) {
    // Send an error downstream and stop (mirrors spawn_window's error contract).
    macro_rules! send_err {
        ($e:expr) => {{
            let _ = tx.send(Err(ArrowError::ExternalError(Box::new($e)))).await;
            return;
        }};
    }

    // --- sync prelude: plan EVERY row group's column partition + fetch ranges
    // up front (borrows `meta`, so it must finish before the first `.await`).
    // Planning globally (not per row group) lets the prefetcher run ahead
    // across row-group boundaries too. ---
    struct RgPlan {
        rg: usize,
        batch_rows: usize,
        n_groups: usize,
    }
    let mut rg_plans: Vec<RgPlan> = Vec::with_capacity(selected.len());
    // (mask, chunk byte ranges, compressed KiB) per column group, in decode order.
    let mut fetch_plans: Vec<(ProjectionMask, Vec<Range<u64>>, u64)> = Vec::new();
    {
        let schema_descr = meta.metadata().file_metadata().schema_descr();
        for &rg in &selected {
            let rgm = meta.metadata().row_group(rg);
            let batch_rows = byte_budget_rows(
                rgm.total_byte_size(),
                rgm.num_rows(),
                batch_clamp,
                decode_budget,
            );
            let groups =
                partition_columns_by_budget(&projected_col_sizes(rgm, &leaves), budget_bytes);
            rg_plans.push(RgPlan {
                rg,
                batch_rows,
                n_groups: groups.len(),
            });
            for g in groups {
                let (ranges, kib) = group_fetch_plan(rgm, &g);
                fetch_plans.push((ProjectionMask::roots(schema_descr, g), ranges, kib));
            }
        }
    }

    // --- admission loop: budget-gated concurrent prefetch ---
    // A semaphore holds `prefetch_budget` in KiB-denominated permits. For each
    // column group IN ORDER: acquire permits equal to its compressed size
    // (clamped to the whole budget so an oversized group can still run, alone),
    // then spawn its ranged GET. Fetches whose permits fit run CONCURRENTLY —
    // that's what overlaps S3 latency with decode — while acquire() blocks the
    // loop the moment the budget is spent. The decoder dropping a finished
    // group releases its permits and un-blocks the loop: constant memory
    // pressure with zero idle gaps, no rate estimation anywhere.
    // `prefetch_budget_mb == 0` degrades to one-group-at-a-time (the old
    // strictly-sequential behavior) because every acquire is for the full
    // 1-permit budget.
    let budget_kib = prefetch_budget_mb.saturating_mul(1024).max(1);
    let budget_kib = budget_kib.min(u32::MAX as u64 / 2);
    let sem = Arc::new(Semaphore::new(budget_kib as usize));
    // Handles are tiny; the byte budget is what actually bounds prefetch. The
    // channel only needs to keep the admission loop from racing unboundedly
    // far ahead in *task count* when groups are small.
    let (htx, mut hrx) =
        mpsc::channel::<tokio::task::JoinHandle<Result<PrefetchedGroup, ArrowError>>>(64);
    {
        let store = store.clone();
        let path = path.clone();
        let plans = std::mem::take(&mut fetch_plans);
        tokio::spawn(async move {
            for (mask, ranges, kib) in plans {
                let want = kib.clamp(1, budget_kib) as u32;
                let permit = match sem.clone().acquire_many_owned(want).await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore closed = consumer gone
                };
                let store = store.clone();
                let path = path.clone();
                let handle = tokio::spawn(async move {
                    match store.get_ranges(&path, &ranges).await {
                        Ok(data) => Ok(PrefetchedGroup {
                            mask,
                            ranges,
                            data,
                            _permit: permit,
                        }),
                        Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                    }
                });
                if htx.send(handle).await.is_err() {
                    return; // decoder dropped (error path) — stop admitting
                }
            }
        });
    }

    // --- decoder: strictly one column group at a time (bounds decode scratch;
    // concurrency lives ONLY on the fetch side above) ---
    for plan in rg_plans {
        let mut group_batches: Vec<Vec<RecordBatch>> = Vec::with_capacity(plan.n_groups);
        for _ in 0..plan.n_groups {
            let handle = match hrx.recv().await {
                Some(h) => h,
                None => send_err!(ParquetError::General(
                    "column-prefetch: admission loop ended early".to_string()
                )),
            };
            let group = match handle.await {
                Ok(Ok(g)) => g,
                Ok(Err(e)) => send_err!(e),
                Err(e) => send_err!(e), // task panicked/cancelled
            };
            let reader = PrefetchedReader {
                ranges: group.ranges,
                data: group.data,
                meta: Arc::clone(meta.metadata()),
            };
            let built = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta.clone())
                .with_row_groups(vec![plan.rg])
                .with_batch_size(plan.batch_rows)
                .with_projection(group.mask)
                .build();
            let mut stream = match built {
                Ok(s) => s,
                Err(e) => send_err!(e),
            };
            let mut batches = Vec::new();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => batches.push(b),
                    Err(e) => send_err!(e),
                }
            }
            group_batches.push(batches);
            // `stream` (owning the PrefetchedReader) and `group._permit` drop
            // here -> this group's compressed bytes are freed and its budget
            // permits released -> the admission loop's pending acquire wakes
            // and the next fetch launches. Decoded batches (the output) are
            // retained for the hstack below, same as before.
        }

        // --- hstack the row-aligned batches into full-width batches ---
        let nbatches = group_batches.first().map(|g| g.len()).unwrap_or(0);
        if group_batches.iter().any(|g| g.len() != nbatches) {
            send_err!(ArrowError::ComputeError(format!(
                "column-window batch-count mismatch in row group {}: {:?}",
                plan.rg,
                group_batches.iter().map(|g| g.len()).collect::<Vec<_>>()
            )));
        }
        for i in 0..nbatches {
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
            for g in &group_batches {
                cols.extend(g[i].columns().iter().cloned());
            }
            match RecordBatch::try_new(out_schema.clone(), cols) {
                Ok(b) => {
                    if tx.send(Ok(b)).await.is_err() {
                        return; // consumer dropped
                    }
                }
                Err(e) => send_err!(e),
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
                    prefetch_windows=2, predicate_json=None, column_fetch_mb=256,
                    column_prefetch_budget_mb=64))]
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
    prefetch_windows: usize,
    // See `read_row_groups`: statistics row-group pruning only.
    predicate_json: Option<String>,
    // Compressed-byte budget per column group for the wide-schema column-windowing
    // path (0 disables it). See `drive_colwindowed`.
    column_fetch_mb: u64,
    // Compressed bytes the column-window prefetcher may hold in flight/buffered
    // ahead of the (single) decoder. Bounds memory by construction while letting
    // enough GETs run concurrently to keep the decoder fed regardless of the
    // fetch:decode speed ratio. 0 = strictly sequential (no overlap).
    column_prefetch_budget_mb: u64,
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

    let rt = shared_runtime();

    // Load footer + page index ONCE (Optional so a window's RowSelection can skip
    // unselected pages by byte range), and build the projected output schema up
    // front from an empty stream (no network). Reporting the projected schema is
    // what keeps it matching the projected batches at the FFI boundary.
    // Blocking async footer fetch; release the GIL so sibling Python read
    // threads (Ray's fragment pool) issue their own S3 requests in parallel.
    let (meta, mask, schema) = py
        .allow_threads(|| {
            rt.block_on(async {
                let (meta, _skipped) =
                    load_meta_s3(store.clone(), obj_path.clone(), PageIndexPolicy::Optional)
                        .await?;
                let mask =
                    projection_mask(meta.metadata().file_metadata().schema_descr(), &columns);
                let schema = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    ParquetObjectReader::new(store.clone(), obj_path.clone()),
                    meta.clone(),
                )
                .with_projection(mask.clone())
                .with_row_groups(vec![])
                .build()?
                .schema()
                .clone();
                Ok::<_, parquet::errors::ParquetError>((meta, mask, schema))
            })
        })
        .map_err(to_py)?;

    let selected: Vec<usize> = match row_groups {
        Some(v) => v,
        None => (0..meta.metadata().num_row_groups()).collect(),
    };
    // Statistics-based pruning (conservative) before any range GET is issued, so
    // pruned groups cost no S3 traffic. Same mechanism as the local path.
    let selected = apply_predicate(&meta, selected, &predicate_json);

    // K-split ONLY for a lone row group above the threshold with a page index —
    // the case Ray's fragment pool can't parallelize. Mirrors the local rule so
    // crate-K and Ray's pool never multiply. Otherwise a single windowed stream
    // (K=1) over all selected groups in order; Ray's pool parallelizes files.
    let split = k > 1
        && selected.len() == 1
        && meta.metadata().row_group(selected[0]).total_byte_size() as u64 >= split_threshold_bytes
        && meta.metadata().offset_index().is_some();

    // Column-windowing (wide-schema S3 memory fix): if any selected group's projected
    // columns exceed the compressed budget, read them in sequential column groups so
    // only one group's compressed chunks are resident at a time. Only for the
    // non-split path (K-split handles the lone-huge-*tall* group); narrow reads
    // partition to a single group and fall through to the streaming path unchanged.
    let leaves =
        projected_leaf_indices(meta.metadata().file_metadata().schema_descr(), &columns);
    let colwindow_budget = column_fetch_mb.saturating_mul(1024 * 1024);
    let colwindow = !split
        && colwindow_budget > 0
        && selected.iter().any(|&rg| {
            partition_columns_by_budget(
                &projected_col_sizes(meta.metadata().row_group(rg), &leaves),
                colwindow_budget,
            )
            .len()
                > 1
        });
    if colwindow {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ArrowError>>(S3_CHANNEL_DEPTH);
        rt.spawn(drive_colwindowed(
            store.clone(),
            obj_path.clone(),
            meta.clone(),
            schema.clone(),
            leaves,
            selected,
            colwindow_budget,
            batch_size,
            decode_budget_bytes,
            column_prefetch_budget_mb,
            tx,
        ));
        return Ok(into_py_stream(Box::new(S3ChannelReader {
            schema,
            receivers: vec![rx],
            cur: 0,
        })));
    }

    // Build the per-unit sub-range lists (each becomes one task + one channel,
    // drained in order).
    let units: Vec<Vec<(usize, usize, usize)>> = if split {
        let rg = selected[0];
        let total_rows = meta.metadata().row_group(rg).num_rows().max(0) as usize;
        let chunk = total_rows.div_ceil(k.max(1)).max(1);
        let mut units = Vec::new();
        let mut start = 0usize;
        while start < total_rows {
            let len = chunk.min(total_rows - start);
            units.push(vec![(rg, start, len)]);
            start += len;
        }
        units
    } else {
        // One unit: every selected group, whole, in order.
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

    // Spawn one task per unit on the shared runtime; collect receivers in order.
    let mut receivers = Vec::with_capacity(units.len());
    for subranges in units {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ArrowError>>(S3_CHANNEL_DEPTH);
        receivers.push(rx);
        let (store, path, meta, mask) =
            (store.clone(), obj_path.clone(), meta.clone(), mask.clone());
        rt.spawn(drive_unit(
            store,
            path,
            meta,
            mask,
            subranges,
            decode_budget_bytes,
            batch_size,
            fetch_window_mb,
            prefetch_windows,
            tx,
        ));
    }

    Ok(into_py_stream(Box::new(S3ChannelReader {
        schema,
        receivers,
        cur: 0,
    })))
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
    m.add_class::<ParquetFileMetadata>()?;
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
            slice_prefetched(&ranges, &data, &(110..115)).unwrap().as_ref(),
            &[10, 11, 12, 13, 14]
        );
        // Sub-range of the second chunk.
        assert_eq!(
            slice_prefetched(&ranges, &data, &(340..350)).unwrap().as_ref(),
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
