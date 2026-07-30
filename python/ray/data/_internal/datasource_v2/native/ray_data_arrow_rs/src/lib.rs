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

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::PageIndexPolicy;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;

use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;

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

/// Pull the fields Python needs out of an already-loaded `ArrowReaderMetadata`.
/// Local and S3 both funnel through here so the shape is identical.
fn build_file_metadata(meta: &ArrowReaderMetadata) -> ParquetFileMetadata {
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
    let opts = reader_options(policy);
    let meta = ArrowReaderMetadata::load(&File::open(&path)?, opts)?;
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
        let step = if window_rows == 0 {
            len.max(1)
        } else {
            window_rows.max(1)
        };
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
                    prefetch_windows=2, predicate_json=None))]
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
                let opts = reader_options(PageIndexPolicy::Optional);
                let mut probe = ParquetObjectReader::new(store.clone(), obj_path.clone());
                let meta = ArrowReaderMetadata::load_async(&mut probe, opts).await?;
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
    let meta = py
        .allow_threads(|| {
            let opts = reader_options(PageIndexPolicy::Skip);
            ArrowReaderMetadata::load(&File::open(&path)?, opts)
        })
        .map_err(to_py)?;
    Ok(build_file_metadata(&meta))
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
    let meta = py
        .allow_threads(|| {
            rt.block_on(async {
                let opts = reader_options(PageIndexPolicy::Skip);
                let mut probe = ParquetObjectReader::new(store.clone(), obj_path.clone());
                ArrowReaderMetadata::load_async(&mut probe, opts).await
            })
        })
        .map_err(to_py)?;
    Ok(build_file_metadata(&meta))
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
    let meta = py
        .allow_threads(|| {
            let opts = reader_options(PageIndexPolicy::Skip);
            ArrowReaderMetadata::load(&File::open(&path)?, opts)
        })
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
