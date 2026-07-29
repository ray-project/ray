"""Experimental arrow-rs Parquet reader (prototype).

Subclasses :class:`ParquetFileReader` and swaps *only* the per-fragment decode
step (:meth:`_iter_fragment_tables`) for the ``ray_data_arrow_rs`` PyO3
extension (a thin wrapper over the Rust ``parquet``/``arrow`` crates).
Everything above the seam — chunking / row-group fan-out, column projection
resolution, ``path`` / ``row_hash`` synthesis, ``limit`` slicing, block sizing,
per-fragment retry — is inherited unchanged from :class:`FileReader` /
:class:`ParquetFileReader`.

Selected via ``DataContext.use_arrow_rs_parquet_reader`` (only takes effect when
``use_datasource_v2`` is also set). Switched in
:meth:`ParquetScanner.create_reader`.

How it reads
------------
The native extension exposes two entry points, both returning an Arrow
C-stream (consumed zero-copy via ``pa.RecordBatchReader.from_stream``):

- ``read_row_groups(path, row_groups, columns, batch_size, ...)`` — local files.
- ``read_row_groups_s3(bucket, key, region, anonymous, ...creds..., row_groups,
  columns, batch_size, decode_budget_bytes, fetch_window_mb, k,
  split_threshold_bytes, prefetch_windows)`` — S3 via the Rust ``object_store``
  crate, using a **windowed** fetch (only ``fetch_window_mb`` of compressed bytes
  in flight per stream) so S3 peak RSS is a knob, not a property of the row-group
  size. ``prefetch_windows`` pipelines consecutive windows (issue window N+1's GET
  while window N decodes) to hide S3 latency without staging the whole row group.

All of the crate's performance knobs (decode budget, K-split, fetch window,
prefetch depth) are settable per read through ``dataset_kwargs`` under an
``arrow_rs_`` prefix — see the "Tuning knobs" section below the imports.

Byte-budgeted decode (no reader-side accumulation)
--------------------------------------------------
The native reader sizes each decode batch *by bytes, not rows*: it reads each
row group's uncompressed size / row count from the footer and picks a row count
so ``rows × bytes_per_row ≈ decode_budget_bytes`` (2 MiB default,
:data:`_ARROW_RS_DECODE_BUDGET_BYTES`). A wide-string group gets few rows/batch,
a numeric group many — both land near the budget, so the decoded working set is
flat across schemas (this is *why* arrow-rs memory doesn't scale with the data
the way PyArrow's whole-row-group materialization does). The ``batch_size`` we
pass is only the upper *clamp*.

We yield each budget-sized batch straight through — exactly like the base
PyArrow path yields one table per scanner batch (:meth:`FileReader.
_iter_fragment_tables`). Coalescing to ``target_max_block_size`` is done once,
downstream, by the read op's :class:`BlockOutputBuffer`. Accumulating a full
block *here* as well (an earlier prototype did) just stacks a second
block-sized buffer on top of the output buffer's, roughly doubling per-worker
peak RSS relative to PyArrow — so we don't. The decode transient stays bounded
by the byte budget; the single ~128 MiB coalesce buffer lives downstream, shared
with the PyArrow path.

Prototype limitations (documented, not hidden)
----------------------------------------------
- Predicate handling prunes at row-group granularity *natively*: the pushed
  Ray ``Expr`` is lowered to a small JSON IR (:func:`_predicate_to_ir`) and
  handed to the crate, which drops row groups whose footer statistics prove no
  row can match (``predicate.rs``) before fetching or decoding them. This
  replaces PyArrow's ``fragment.subset(filter=...)``. Pruning is conservative
  (a missing column / absent stats / uncomparable type keeps the group), so it
  can only avoid IO/decode, never change results. *Row-level* filtering is then
  applied post-decode in Python via PyArrow (the final authority) — the crate
  has no in-decode ``RowFilter`` yet, so rows inside a surviving row group are
  decoded before being dropped.
- The native path covers local **and S3** files whose columns the crate
  decodes byte-identically to PyArrow: flat types, ``dictionary``, ``map``,
  and ``extension`` types (registered like Ray's tensor types or not — the
  crate passes the embedded arrow-schema field metadata straight through FFI,
  so pyarrow reconstructs them exactly as it would on its own read path), plus
  struct / list / map nesting of all those to any depth. Where the crate's
  decode *differs* from what the pyarrow scanner would output but the
  difference is mechanical, the planned ``read()`` stays native and realigns
  post-decode via a per-file :class:`_ColumnAlignment`: schema-evolution
  columns are null-filled, per-file type drift is cast to the unified schema,
  INT96 hint units are upcast to pyarrow's default ns (but a file decoding
  INT96 under ``coerce_int96_timestamp_unit`` falls back — decode-time
  coercion floors where a cast truncates, splitting on pre-1970 values), and
  forced ``dictionary_columns`` reads are dictionary-cast. An empty projection
  with no predicate (count-style scan) decodes nothing at all — footer row
  counts answer it (:class:`_NativeCountFragment`).
- Still gated to PyArrow: non-local/S3 filesystems, Parquet-format kwargs
  outside the native allowlist (anything not I/O-only, not reproduced by the
  alignment, and not footer-verified — e.g. decryption, an explicit
  ``page_checksum_verification=False``, or ``binary_type`` / ``list_type``
  without a pinned dataset schema; see
  :meth:`ArrowRsParquetFileReader._blocking_format_kwargs`; the thrift footer
  limits stay native via a metadata-only pyarrow probe,
  :meth:`_verify_footer_limits`), extension-typed
  schema drift, and tz-carrying / nested INT96 oddities. There is no per-type
  gate: every type Parquet can encode decodes byte-identically through the
  crate, and Arrow's in-memory-only types (``union``, ``list_view``, …) cannot
  appear in a Parquet footer at all. (Nested-column
  *projection* via dotted names is NOT a gate: V2 discards dotted names in
  ``FileReader._split_columns`` before any reader sees them — both paths
  silently drop the column, and a flat column literally named ``"a.b"``
  decodes natively; see ``test_dotted_nested_projection_native_parity``.)
  Everything gated transparently falls back to the
  PyArrow reader, so correctness is never at risk — but benchmarks must
  confirm the arrow-rs path actually ran (see the
  ``RAY_DATA_USE_ARROW_RS_PARQUET_READER`` verification).
"""

import json
import logging
import os
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, NamedTuple, Optional, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.native_metadata import (
    read_native_metadata as _read_native_metadata_via_crate,
    s3_config as _s3_config,
)
from ray.data._internal.datasource_v2.readers.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    _ARROW_SCANNER_BATCH_READAHEAD,
)
from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
    ParquetFileReader,
    _estimate_batch_size_from_metadata,
)
from ray.data._internal.object_extensions.arrow import (
    raise_on_pickle_object_columns,
)
from ray.data._internal.util import MiB
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow.compute as pc  # noqa: F401

    from ray.data._internal.datasource_v2.listing.file_manifest import (  # noqa: F401
        FileManifest,
    )
    from ray.data.expressions import Expr  # noqa: F401

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------
# Every knob below is settable *per read* via ``dataset_kwargs`` — the same
# channel PyArrow's own I/O-tuning kwargs (``pre_buffer``, ``buffer_size``,
# ...) travel — under an ``arrow_rs_`` prefix:
#
#     ray.data.read_parquet(
#         path, dataset_kwargs={"arrow_rs_fetch_window_mb": 64, "arrow_rs_k": 4}
#     )
#
# Precedence: ``dataset_kwargs`` value > ``RAY_DATA_ARROW_RS_*`` env var >
# built-in default. The env vars remain the cluster-wide lever (benchmark
# sweeps set them per worker); the kwargs are the per-read override.
#
# The PyArrow reader ignores the ``arrow_rs_*`` keys — they're popped out of
# the format kwargs in ``ParquetFileReader.__init__`` before PyArrow can see
# them — exactly as this reader ignores PyArrow's I/O-only kwargs
# (:data:`_FORMAT_KWARGS_PERF_ONLY`). So a call carrying either family of
# perf knobs stays valid whichever reader ``use_arrow_rs_parquet_reader``
# selects. Resolution + validation: :meth:`ArrowRsParquetFileReader._tuning`.

# Knob ``arrow_rs_decode_budget_bytes`` — crate arg ``decode_budget_bytes``.
# Byte budget for a single arrow-rs decode batch. Sizing decode batches by
# bytes (not a fixed row count) keeps the transient working set flat across
# schemas: a wide file gets few rows/batch, a narrow file gets many. Kept far
# below ``target_block_size`` so the decode transient is bounded while output
# blocks are still coalesced to the normal Ray block size. Default 2 MiB: the
# standalone benchmark found budget is the *floor* knob (it moves peak only ~12 MB
# across its range; the S3 fetch window is the real lever), and 2 MiB is the lowest
# that holds throughput — so we take the smaller working set. Swept on the Linux/S3
# run to confirm it holds under real integration (Agents.md §7.1).
# Tuning: raise toward 8–32 MiB only if per-batch overhead shows up on very
# wide/stringy schemas; lowering below 2 MiB buys ~no memory (the fetch
# window / block buffer dominate peak) and costs throughput.
_ARROW_RS_DECODE_BUDGET_BYTES = env_integer(
    "RAY_DATA_ARROW_RS_DECODE_BUDGET_BYTES", 2 * MiB
)

# Floor on the estimated decode batch size (rows), so a very wide schema can't
# collapse the batch to a handful of rows and starve throughput.
_ARROW_RS_MIN_DECODE_BATCH_ROWS = 2048

# Knob ``arrow_rs_k`` — crate arg ``k``.
# Intra-fragment parallelism: when a fragment is a *single* row group larger than
# the block-size target (the lone-big-fragment case Ray's thread pool can't split),
# the native reader decodes it in ``K`` parallel row-range workers and merges them
# back in order. Every other layout (multiple / small row groups) uses K=1 because
# Ray's fragment thread pool already parallelizes those — so crate-K and Ray's pool
# never multiply.
#
# Default K=1: locally, K-split costs memory (each range holds its own decode
# transient) for ~no speed, since there is no network latency to hide (benchmarks:
# Agents.md §5.1, §6.3). K>1 is opt-in and is reserved for the S3 phase, where
# concurrent range GETs hide request latency.
# Tuning: try 2–8 for big single-row-group files on S3 when read throughput is
# latency-bound; expect per-task peak memory to scale ~linearly with K (each
# range worker holds its own fetch window + decode transient). Keep 1 for
# local reads and many-small-row-group layouts.
_ARROW_RS_K = env_integer("RAY_DATA_ARROW_RS_K", 1)

# Knob ``arrow_rs_split_threshold_bytes`` — crate arg ``split_threshold_bytes``.
# A lone row group is only K-split when its uncompressed size exceeds this
# threshold (smaller groups decode sequentially — splitting them buys nothing
# and costs merge overhead). When the knob is unset the reader uses its
# ``target_block_size`` (the Ray block-size target), falling back to the
# default below when it has neither.
# Tuning: rarely needed — lower it (e.g. to 0) only to force the K-split on
# for testing, or raise it to keep K-splitting away from mid-size groups.
_ARROW_RS_DEFAULT_SPLIT_THRESHOLD_BYTES = 128 * MiB

# Knob ``arrow_rs_fetch_window_mb`` — crate arg ``fetch_window_mb``.
# S3 fetch window (MiB of *compressed* bytes in flight per stream). This is the
# memory knob for the S3 path: the native reader slices each row group's rows into
# windows sized so only ~this many compressed bytes are fetched+buffered before
# decode, so peak RSS is `≈ fetch_window + decode_budget` — flat regardless of
# row-group size, instead of PyArrow's whole-row-group pre-buffer. 0 = no window
# cap (fetch the whole range at once). Swept on the Linux/S3 run (Agents.md §7.1).
# Tuning: this is the primary memory<->throughput trade on S3. Raise (64+) to
# amortize request latency over fewer, larger GETs when memory is plentiful;
# lower toward 4 to cap per-task RSS on memory-tight clusters. No effect on
# local reads.
_ARROW_RS_FETCH_WINDOW_MB = env_integer("RAY_DATA_ARROW_RS_FETCH_WINDOW_MB", 16)

# Knob ``arrow_rs_prefetch_windows`` — crate arg ``prefetch_windows``.
# S3 window prefetch depth: how many consecutive fetch windows the native reader
# keeps in flight per stream. With depth 2 the GET for window N+1 is issued while
# window N decodes, so S3 first-byte latency is hidden behind decode instead of
# paid serially between windows — the memory-bounded analog of pyarrow's
# whole-fragment pre_buffer (peak stays `≈ prefetch_windows * fetch_window_mb`
# compressed in flight, not the whole row group). 1 = strictly serial windows
# (no prefetch). Only affects the S3 path (local reads have no fetch latency to
# hide); reserved for and swept on the Linux/S3 run (Agents.md §7.1).
# Tuning: 2 already overlaps fetch with decode; depth >2 only helps when a
# single window decodes faster than it fetches (very high-latency stores) and
# multiplies the compressed bytes held in flight. Drop to 1 to minimize
# memory at the cost of exposing S3 latency between windows.
_ARROW_RS_PREFETCH_WINDOWS = env_integer("RAY_DATA_ARROW_RS_PREFETCH_WINDOWS", 2)

# Parquet-format kwargs (``pds.ParquetFileFormat``) that tune PyArrow's I/O
# strategy only — they cannot change decoded bytes, so the native path (which
# has its own I/O strategy: byte-budgeted streaming + bounded fetch window) may
# safely ignore them. Every other format kwarg either has a native equivalent
# planned per file (see ``_FORMAT_KWARGS_ALIGNED``) or forces a PyArrow
# fallback (see ``ArrowRsParquetFileReader._blocking_format_kwargs``).
_FORMAT_KWARGS_PERF_ONLY = frozenset(
    {"pre_buffer", "buffer_size", "use_buffered_stream", "cache_options"}
)
# Parquet-format kwargs whose *semantic* effect the planned native read
# reproduces post-decode via ``_ColumnAlignment`` casts. Membership here means
# the kwarg never blocks the reader; the per-file plan decides. For
# ``coerce_int96_timestamp_unit`` the plan is a *fallback* whenever the file
# actually decodes an INT96 column: pyarrow's decode-time coercion floors
# (parquet types.h divides the unsigned nanos-of-day) while a post-decode cast
# truncates toward zero — off by one unit on every pre-1970 value, so the cast
# cannot reproduce it. Files without INT96 stay native (the kwarg is inert).
_FORMAT_KWARGS_ALIGNED = frozenset(
    {"coerce_int96_timestamp_unit", "dictionary_columns"}
)
# Parquet-format kwargs the planned native read enforces via a metadata-only
# pyarrow footer probe (:meth:`ArrowRsParquetFileReader._verify_footer_limits`):
# the thrift limits only decide whether a file's *footer* is accepted or
# rejected — they can never change decoded bytes — so running pyarrow's own
# footer parse with the limits applied reproduces the accept/reject behavior
# (and the raised ``OSError``) exactly, after which the decode stays native.
_FORMAT_KWARGS_FOOTER_VERIFIED = frozenset(
    {"thrift_string_size_limit", "thrift_container_size_limit"}
)
# Schema-shaping kwargs (pyarrow 21+): on a file *without* an embedded arrow
# schema they change decoded types (``binary_type=large_binary`` flips
# binary→large_binary AND string→large_string; ``list_type=LargeListType``
# flips list→large_list); on embedded-schema files (all Ray-written files)
# they are inert. On the V2 pipeline the pinned unified schema — computed by
# the listing via ``pq.read_schema``, which is blind to these kwargs — is the
# final authority: the base reader's pinned-schema cast silently *undoes*
# them (verified empirically, pyarrow 24). So with a pinned schema, parity is
# simply "output the pinned schema", which the native path's per-file
# :class:`_ColumnAlignment` drift casts already guarantee — admit natively.
# WITHOUT a pinned schema the kwargs do change the output and the crate
# doesn't reproduce them — fall back. See
# :meth:`ArrowRsParquetFileReader._blocking_format_kwargs`.
_FORMAT_KWARGS_SCHEMA_SHAPED = frozenset({"binary_type", "list_type"})


class _ArrowRsTuning(NamedTuple):
    """Resolved values of the tuning knobs above for one reader instance
    (kwarg > env var > default; see :meth:`ArrowRsParquetFileReader._tuning`).
    ``split_threshold_bytes=None`` means "derive from ``target_block_size``"
    at the call site."""

    decode_budget_bytes: int
    k: int
    split_threshold_bytes: Optional[int]
    fetch_window_mb: int
    prefetch_windows: int


# There is deliberately NO per-type support gate: every type Parquet can store
# (flat types, ``dictionary``, ``map``, ``extension`` — registered like Ray's
# tensor types or not — plus struct / list / map nesting of all those to any
# depth) decodes byte-identically to PyArrow through the crate's C-data
# interface (verified in ``test_extension_types_native_parity`` and the type
# probes behind it). The Arrow types the crate has NOT been verified against
# (``union``, ``list_view`` / ``large_list_view``, ``run_end_encoded``, …) are
# in-memory-only types with no Parquet encoding — PyArrow itself refuses to
# write them ("Unhandled type for Arrow to Parquet schema conversion") — so a
# schema read from a Parquet footer can never contain one, and a gate on them
# was unreachable dead code (removed 2026-07-28).


def _pyarrow_fragment_int96_roots(fragment: "pds.ParquetFileFragment") -> set:
    """Root (top-level) column names backing an INT96 leaf in a PyArrow Parquet
    fragment, read from the *parquet* schema (``fragment.metadata.schema``).

    The fragment's ``physical_schema`` is PyArrow's post-coercion Arrow schema, in
    which INT96 already shows up as ``timestamp[ns]`` — so it can't reveal which
    columns were INT96 on disk. The parquet schema descriptor can, via each leaf
    column's ``physical_type``. Used by the conservative re-gate so an INT96 file
    is never handed to the crate through the per-fragment path."""
    roots: set = set()
    try:
        schema = fragment.metadata.schema
        for i in range(len(schema)):
            col = schema.column(i)
            if col.physical_type == "INT96":
                roots.add(col.path.split(".", 1)[0])
    except Exception:  # noqa: BLE001 - missing/odd metadata => treat as none
        pass
    return roots


def _raise_if_strict_no_fallback(reason: str) -> None:
    """Correctness-harness guard: when ``RAY_DATA_ARROW_RS_STRICT`` is set (to
    anything but ``0``/``false``), any decision to serve part of a read through
    the PyArrow fallback raises instead of proceeding. A large-scale validation
    run flips this on to *guarantee* every byte it checked came off the native
    arrow-rs path — a silent fallback would make the run validate PyArrow.
    Read per call (not at import) so a harness can toggle it within a session.
    Inert by default: production reads never set the variable.
    """
    if os.environ.get("RAY_DATA_ARROW_RS_STRICT", "").lower() in ("", "0", "false"):
        return
    raise RuntimeError(
        "RAY_DATA_ARROW_RS_STRICT is set, but this read requires the PyArrow "
        f"fallback: {reason}. Strict mode exists for validation harnesses that "
        "must prove the native arrow-rs path ran; unset the env var to allow "
        "the fallback."
    )


def _trace_reader_path(supported: bool) -> None:
    """Benchmark instrumentation (inert unless ``RAY_DATA_ARROW_RS_PATH_TRACE``
    names a directory): append ``native``/``fallback`` for each fragment to a
    per-pid file so a harness can assert which path the support gate chose. Never
    raises into the read path.
    """
    trace_dir = os.environ.get("RAY_DATA_ARROW_RS_PATH_TRACE")
    if not trace_dir:
        return
    try:
        import socket

        # Namespace by hostname so nodes writing to a shared trace dir (multi-node
        # verification) don't collide on pid; the harness's ``path_*.log`` glob
        # still matches. Single-node is unaffected.
        line = "native\n" if supported else "fallback\n"
        fname = f"path_{socket.gethostname()}_{os.getpid()}.log"
        with open(os.path.join(trace_dir, fname), "a") as fh:
            fh.write(line)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Predicate lowering: Ray Expr -> native pruning IR (predicate pushdown, part 1)
# ---------------------------------------------------------------------------
# The native crate does statistics-based row-group pruning from a small JSON IR
# (parsed by ``predicate.rs``). We lower the *pushed* Ray ``Expr`` predicate into
# that IR here rather than translating the PyArrow expression, because the Ray
# AST (ColumnExpr / LiteralExpr / BinaryExpr / UnaryExpr) is directly
# introspectable.
#
# The lowering is **total**: any node it can't represent becomes ``{"t":
# "unknown"}``, which the crate treats as "keep this row group". So a partially
# understood predicate like ``a > 5 AND some_udf(b)`` still lowers to
# ``And[cmp(a>5), unknown]`` and prunes soundly on the ``a > 5`` conjunct instead
# of giving up. Pruning is conservative on the Rust side (a group is dropped only
# when provably empty), and the reader re-applies the full predicate post-decode,
# so this only ever avoids IO/decode — it can never change which rows are
# returned.

# Ray comparison Operation -> IR op string.
_CMP_OP_TO_IR = {
    "gt": "gt",
    "lt": "lt",
    "ge": "ge",
    "le": "le",
    "eq": "eq",
    "ne": "ne",
}
# When the column is on the *right* of a comparison (``5 < col``), flip the op so
# the IR always reads ``col OP literal``.
_CMP_OP_FLIP = {"gt": "lt", "lt": "gt", "ge": "le", "le": "ge", "eq": "eq", "ne": "ne"}

_IR_UNKNOWN: Dict[str, Any] = {"t": "unknown"}


def _literal_to_ir_value(value: Any) -> Optional[Dict[str, Any]]:
    """Lower a Python literal to a tagged IR value, or None if the crate can't
    order it for pruning (bytes, datetimes, decimals, ...), which makes the
    enclosing atom ``unknown``. ``bool`` is checked before ``int`` because
    ``bool`` is an ``int`` subclass."""
    if isinstance(value, bool):
        return {"vt": "bool", "v": value}
    if isinstance(value, int):
        return {"vt": "int", "v": value}
    if isinstance(value, float):
        return {"vt": "float", "v": value}
    if isinstance(value, str):
        return {"vt": "str", "v": value}
    if value is None:
        return {"vt": "null"}
    return None


def _predicate_to_ir(expr: "Expr") -> Dict[str, Any]:
    """Lower a Ray Data predicate ``Expr`` to the native pruning IR (see above).

    Total by construction: unrepresentable subtrees become ``_IR_UNKNOWN``.
    """
    from ray.data.expressions import (
        AliasExpr,
        BinaryExpr,
        ColumnExpr,
        LiteralExpr,
        Operation,
        UnaryExpr,
    )

    def unwrap(e: "Expr") -> "Expr":
        # Aliasing doesn't change the value being compared.
        while isinstance(e, AliasExpr):
            e = e.expr
        return e

    def lower(e: "Expr") -> Dict[str, Any]:
        e = unwrap(e)

        if isinstance(e, UnaryExpr):
            if e.op == Operation.NOT:
                return {"t": "not", "pred": lower(e.operand)}
            if e.op in (Operation.IS_NULL, Operation.IS_NOT_NULL):
                operand = unwrap(e.operand)
                if isinstance(operand, ColumnExpr):
                    tag = "is_null" if e.op == Operation.IS_NULL else "is_not_null"
                    return {"t": tag, "col": operand.name}
            return _IR_UNKNOWN

        if isinstance(e, BinaryExpr):
            if e.op in (Operation.AND, Operation.OR):
                tag = "and" if e.op == Operation.AND else "or"
                return {"t": tag, "preds": [lower(e.left), lower(e.right)]}

            if e.op in (Operation.IN, Operation.NOT_IN):
                col = unwrap(e.left)
                rhs = unwrap(e.right)
                if isinstance(col, ColumnExpr) and isinstance(rhs, LiteralExpr):
                    raw = rhs.value
                    raw = raw if isinstance(raw, list) else [raw]
                    values = [_literal_to_ir_value(v) for v in raw]
                    if all(v is not None for v in values):
                        return {
                            "t": "in",
                            "col": col.name,
                            "values": values,
                            "negated": e.op == Operation.NOT_IN,
                        }
                return _IR_UNKNOWN

            ir_op = _CMP_OP_TO_IR.get(e.op.value)
            if ir_op is not None:
                left = unwrap(e.left)
                right = unwrap(e.right)
                if isinstance(left, ColumnExpr) and isinstance(right, LiteralExpr):
                    val = _literal_to_ir_value(right.value)
                    if val is not None:
                        return {"t": "cmp", "col": left.name, "op": ir_op, "value": val}
                elif isinstance(left, LiteralExpr) and isinstance(right, ColumnExpr):
                    val = _literal_to_ir_value(left.value)
                    if val is not None:
                        return {
                            "t": "cmp",
                            "col": right.name,
                            "op": _CMP_OP_FLIP[ir_op],
                            "value": val,
                        }
            return _IR_UNKNOWN

        return _IR_UNKNOWN

    return lower(expr)


def _predicate_json(predicate: "Optional[Expr]") -> Optional[str]:
    """Serialize the pushed predicate's pruning IR for the crate, or None when
    there's nothing prunable (no predicate, or it lowered entirely to
    ``unknown``) so we skip the pushdown argument altogether."""
    if predicate is None:
        return None
    ir = _predicate_to_ir(predicate)
    if ir == _IR_UNKNOWN:
        return None
    return json.dumps(ir)


def _is_extension_type(t: pa.DataType) -> bool:
    """Two-way extension detection: ``isinstance`` for registered extensions,
    ``extension_name`` for canonical pyarrow extensions (e.g.
    ``fixed_shape_tensor``) that aren't ``pa.ExtensionType`` instances on every
    pyarrow version."""
    return isinstance(t, pa.ExtensionType) or (
        getattr(t, "extension_name", None) is not None
    )


class _ColumnAlignment(NamedTuple):
    """Per-file post-decode fixups that make a native decode byte-match what the
    pyarrow scanner would have produced for the same file against the unified
    dataset schema. Built once per file at plan time
    (:meth:`ArrowRsParquetFileReader._plan_column_alignment`), applied to every
    decoded batch (:func:`_apply_column_alignment`) *before* the post-decode
    filter, so the predicate evaluates against the same types pyarrow's scanner
    filters on.

    - ``null_fill``: columns absent from this file (schema evolution) appended
      as typed all-null columns — exactly pyarrow's null-fill under a pinned
      dataset schema.
    - ``casts``: columns whose crate-decoded type differs from the expected
      output type (per-file type drift vs the unified schema, INT96 unit
      realignment, forced ``dictionary_columns`` decode). The bool is
      ``allow_time_truncate``, set only for INT96 unit coercion where pyarrow
      itself truncates (``coerce_int96_timestamp_unit``); every other cast is
      safe, so lossy data errors loudly — the same outcome pyarrow's own
      scanner cast produces.
    - ``order``: final column order, or ``None`` to keep the crate's order.
      Set only when ``null_fill`` is non-empty (appended columns must land in
      read order, matching the scanner's projected-column order).
    """

    null_fill: Tuple[Tuple[str, pa.DataType], ...]
    casts: Tuple[Tuple[str, pa.DataType, bool], ...]
    order: Optional[Tuple[str, ...]]

    @property
    def is_noop(self) -> bool:
        return not self.null_fill and not self.casts


_NOOP_ALIGNMENT = _ColumnAlignment(null_fill=(), casts=(), order=None)


def _apply_column_alignment(
    table: pa.Table, alignment: Optional[_ColumnAlignment]
) -> pa.Table:
    """Apply a plan-time :class:`_ColumnAlignment` to one decoded batch."""
    if alignment is None or alignment.is_noop:
        return table
    import pyarrow.compute as pc

    for name, target, allow_time_truncate in alignment.casts:
        idx = table.schema.get_field_index(name)
        if idx == -1:
            continue
        column = table.column(idx)
        if allow_time_truncate:
            column = column.cast(
                options=pc.CastOptions(target, allow_time_truncate=True)
            )
        else:
            column = column.cast(target)  # safe: lossy values raise, like pyarrow
        table = table.set_column(idx, pa.field(name, target), column)
    for name, fill_type in alignment.null_fill:
        table = table.append_column(
            pa.field(name, fill_type), pa.nulls(table.num_rows, type=fill_type)
        )
    if alignment.order is not None:
        table = table.select([c for c in alignment.order if c in table.column_names])
    return table


class _NativeParquetFragment(NamedTuple):
    """A native (pyarrow-free) unit of work for one file's row-group slice.

    The arrow-rs ``read()`` builds these instead of pyarrow ``ParquetFileFragment``
    objects for files the native reader handles, so pyarrow never opens a
    supported file. ``row_groups is None`` means "all row groups in the file"
    (whole-file read). Exposes ``.path`` so it flows through the same
    :meth:`FileReader._dispatch_fragment_reads` threading/retry machinery as a
    pyarrow fragment; :meth:`ArrowRsParquetFileReader._iter_fragment_tables`
    dispatches on the type. ``alignment`` carries this file's post-decode
    fixups (:class:`_ColumnAlignment`), ``None`` when the decode already
    matches the expected output.
    """

    path: str
    row_groups: Optional[List[int]]
    alignment: Optional[_ColumnAlignment] = None


class _NativeCountFragment(NamedTuple):
    """A zero-decode work unit for an empty projection (count-style scan) with
    no predicate: the footer row counts are exact, so the read yields a
    zero-column table with the right ``num_rows`` and never touches a data
    page. The base pyarrow path instead scans a stub column;
    :meth:`FileReader._postprocess`'s stub guard re-adds the row-preserving
    stub downstream, identically for both paths."""

    path: str
    num_rows: int


@DeveloperAPI
class ArrowRsParquetFileReader(ParquetFileReader):
    """Parquet reader that decodes each fragment via the arrow-rs extension.

    See the module docstring for the design. Only :meth:`_iter_fragment_tables`,
    :meth:`_resolve_batch_size`, and :meth:`_on_batch_read` are overridden; the
    rest of the read pipeline is inherited from :class:`ParquetFileReader`.
    """

    @override
    def _resolve_batch_size(self, dataset: pds.Dataset) -> int:
        """Size the decode batch to the arrow-rs byte budget, not the block size.

        Priority: explicit ``batch_size`` > byte-budget estimate from row-group
        metadata > default. Unlike the base reader this targets
        :data:`_ARROW_RS_DECODE_BUDGET_BYTES` (~8 MiB) rather than
        ``target_block_size`` (~128 MiB), because each decode batch is yielded
        straight through in :meth:`_iter_fragment_tables` (the downstream
        ``BlockOutputBuffer`` does the coalescing to the block size).
        """
        if self._explicit_batch_size is not None:
            return self._explicit_batch_size

        if self._target_block_size is None:
            return _ARROW_DEFAULT_BATCH_SIZE

        first_fragment = next(dataset.get_fragments(), None)
        if first_fragment is None:
            return _ARROW_DEFAULT_BATCH_SIZE

        estimated = _estimate_batch_size_from_metadata(
            first_fragment, self._columns, self._tuning.decode_budget_bytes
        )
        if estimated is None:
            return _ARROW_DEFAULT_BATCH_SIZE
        return max(estimated, _ARROW_RS_MIN_DECODE_BATCH_ROWS)

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        """No-op: the decode batch size is fixed by the byte budget, so there is
        nothing to refine from actual data (unlike the base reader)."""
        return None

    @override
    def read(self, input_split: "FileManifest") -> "Iterator[pa.Table]":
        """Pyarrow-free Parquet read for supported files.

        For files the native reader handles (local/S3, flat + struct/list types,
        no schema evolution / dictionary / extension), the footer, row-group
        layout, and statistics come from the crate's ``read_metadata`` and decode
        from ``read_row_groups`` — pyarrow never opens the file. Files (or whole
        splits) the native path can't handle transparently fall back to the base
        pyarrow ``read()`` / scanner, so correctness is never at risk. The
        format-agnostic finishing (limit, partition/``path``/``row_hash``
        synthesis, projection) is shared with the base reader via
        :meth:`_postprocess`.
        """
        if len(input_split) == 0:
            return

        # Reader-wide ineligibility: unsupported filesystem, or a Parquet-format
        # kwarg outside the native allowlist (anything not perf-only, not
        # reproduced by the per-file :class:`_ColumnAlignment`, and not
        # footer-verified — e.g. decryption, ``binary_type``) — use the base
        # pyarrow read() unchanged, which honors every format kwarg.
        blocked = self._blocking_format_kwargs(aligned_ok=True)
        if not self._filesystem_supported() or blocked:
            if blocked:
                _raise_if_strict_no_fallback(
                    f"unsupported parquet format kwargs {sorted(blocked)}"
                )
                logger.debug(
                    "arrow-rs read falling back to pyarrow: unsupported "
                    "parquet format kwargs %s",
                    sorted(blocked),
                )
            else:
                _raise_if_strict_no_fallback(
                    f"unsupported filesystem {type(self._filesystem).__name__}"
                )
            yield from super().read(input_split)
            return

        plan = self._plan_native_read(input_split)
        if plan is None:
            # A file's footer couldn't be read natively (corrupt / unsupported
            # footer); fall the whole split back to pyarrow rather than reason
            # about a partially-known layout.
            _raise_if_strict_no_fallback(
                "a file's footer could not be read via the native crate"
            )
            yield from super().read(input_split)
            return

        fragments_with_offsets, columns_to_synthesize, scanner_kwargs = plan
        triples = self._dispatch_fragment_reads(fragments_with_offsets, scanner_kwargs)
        yield from self._postprocess(triples, columns_to_synthesize)

    def _verify_footer_limits(self, paths: List[str]) -> None:
        """Enforce ``thrift_string_size_limit`` / ``thrift_container_size_limit``
        on the planned native read with a metadata-only pyarrow footer parse.

        The limits guard *footer deserialization* (accept vs reject a file's
        metadata) and can never change decoded bytes, so running pyarrow's own
        thrift parser with the limits applied reproduces the base path's
        accept/reject behavior — and the exact ``OSError`` it raises — while
        the data decode stays native. This is the one deliberate exception to
        "pyarrow never opens a supported file": a footer-only read (a few KB),
        and only when the user actually set a limit. A raised error is the
        *correct* outcome, not a fallback trigger — the base path would raise
        the same error, so we let it propagate."""
        limits = {
            key: self._parquet_format_kwargs[key]
            for key in _FORMAT_KWARGS_FOOTER_VERIFIED
            if self._parquet_format_kwargs.get(key) is not None
        }
        if not limits:
            return
        import pyarrow.parquet as pq
        from pyarrow.fs import LocalFileSystem

        fs = self._filesystem or LocalFileSystem()
        for path in paths:
            with fs.open_input_file(path) as source:
                # Constructing ParquetFile parses the footer under the limits;
                # a violation raises pyarrow's usual thrift OSError.
                pq.ParquetFile(source, **limits)

    def _read_native_metadata(
        self, path: str
    ) -> Optional[Tuple[pa.Schema, List[int], List[str]]]:
        """Read one file's footer via the crate: ``(arrow schema, per-row-group
        row counts, int96 root columns)``, or ``None`` if the native footer read
        fails (caller then falls the whole split back to pyarrow). Does *not*
        swallow a missing extension — :meth:`_import_extension` raises that
        loudly. The int96 list lets :meth:`_plan_column_alignment` realign the
        crate's decoded unit for those columns to what PyArrow produces."""
        # Surfaces a missing extension loudly (import inside the crate call);
        # any *footer-read* failure below becomes a whole-split pyarrow fallback.
        self._import_extension()

        try:
            md = _read_native_metadata_via_crate(path, self._filesystem)
        except Exception as e:  # noqa: BLE001 - any footer failure => fallback
            logger.debug("arrow-rs native metadata read failed for %s: %s", path, e)
            return None
        return pa.schema(md), list(md.row_group_num_rows), list(md.int96_columns)

    def _plan_native_read(
        self, manifest: "FileManifest"
    ) -> Optional[Tuple[List[Tuple[Any, int]], Optional[set], dict]]:
        """Plan a native read: footer-read every file, decide native vs pyarrow
        per file, and build the ordered ``[(fragment, file_row_offset)]`` list
        plus the shared column split and scanner kwargs. Returns ``None`` to
        signal a whole-split pyarrow fallback (some file's footer read failed)."""
        import pyarrow.dataset as pds
        from pyarrow.fs import LocalFileSystem

        from ray.data._internal.datasource_v2.chunkers.parquet_file_chunking_utils import (  # noqa: E501
            _fragments_from_chunk_metadata,
        )

        unique_paths = list(dict.fromkeys(list(manifest.paths)))

        # Thrift footer limits, when set, decide whether each file is accepted
        # or REJECTED — enforce them first with pyarrow's own parser so a
        # too-large footer raises the identical OSError the base path would
        # raise (parity-of-error), before any native work happens.
        self._verify_footer_limits(unique_paths)

        native_md: Dict[str, Tuple[pa.Schema, List[int], List[str]]] = {}
        for path in unique_paths:
            md = self._read_native_metadata(path)
            if md is None:
                return None  # whole-split pyarrow fallback
            native_md[path] = md

        # Column split, mirroring the base reader's ``dataset.schema.names``:
        # with a pinned unified schema, ``pds.dataset(schema=...)`` reports
        # exactly that schema — so a unified column absent from every file in
        # this split still counts as on-disk (and gets null-filled per file by
        # the alignment), instead of being silently dropped. Without a unified
        # schema, fall back to the union of the files' footer schemas.
        # Partition / path / row_hash columns aren't on disk anywhere and so
        # land in the synthesize set either way.
        if self._file_dataset_schema is not None:
            on_disk_names = set(self._file_dataset_schema.names)
        else:
            on_disk_names = set()
            for schema, _, _ in native_md.values():
                on_disk_names.update(schema.names)
        columns_to_read_from_file, columns_to_synthesize = self._split_columns(
            on_disk_names
        )

        scanner_kwargs = {
            "columns": columns_to_read_from_file,
            "filter": (
                self._predicate.to_pyarrow() if self._predicate is not None else None
            ),
            # The native decode re-derives its per-batch size by byte budget from
            # the footer, so this is only an upper clamp; pyarrow-fallback
            # fragments (nested/dictionary/extension) further clamp it themselves.
            "batch_size": self._explicit_batch_size or _ARROW_DEFAULT_BATCH_SIZE,
            "batch_readahead": _ARROW_SCANNER_BATCH_READAHEAD,
        }
        scanner_kwargs.update(self._arrow_scanner_kwargs())

        read_columns = self._resolve_read_columns_for(scanner_kwargs)

        # Empty projection with no predicate (count-style scan): zero decode —
        # the footer row counts already read above are exact, so emit
        # count fragments for every file and never touch a data page. (With a
        # predicate the count depends on the data; that case falls through to
        # the per-file verdict below, which rejects empty projections.)
        if (
            read_columns is not None
            and len(read_columns) == 0
            and scanner_kwargs["filter"] is None
        ):
            count_fragments: List[Tuple[Any, int]] = []
            for path, chunk_metadata in zip(
                manifest.paths, manifest.file_chunk_metadatas
            ):
                count_fragments.extend(
                    self._native_count_fragments(
                        path, chunk_metadata, native_md[path][1]
                    )
                )
            return count_fragments, columns_to_synthesize, scanner_kwargs

        # Per-file verdict: native decode (with an optional post-decode
        # alignment plan) vs pyarrow fallback.
        alignment_by_path: Dict[str, Optional[_ColumnAlignment]] = {}
        for path, (schema, _, int96_cols) in native_md.items():
            alignment = self._plan_column_alignment(schema, read_columns, int96_cols)
            if alignment is not None:
                alignment_by_path[path] = None if alignment.is_noop else alignment
        native_paths = set(alignment_by_path)
        fallback_paths = [p for p in unique_paths if p not in native_paths]
        if fallback_paths:
            _raise_if_strict_no_fallback(
                "no native column-alignment plan for file(s) "
                f"{fallback_paths} (unplannable schema drift or unsupported "
                "read-time coercion)"
            )

        # Build pyarrow fragments for the fallback files only (pyarrow never opens
        # native files). One dataset over the fallback paths; the per-file fan-out
        # reuses the base chunker helper so offsets / row-group slicing match the
        # base path exactly.
        fallback_fragment_by_path: dict = {}
        if fallback_paths:
            fb_dataset = pds.dataset(
                source=fallback_paths,
                format=self._make_format(),
                filesystem=self._filesystem or LocalFileSystem(),
                schema=self._file_dataset_schema,
                ignore_prefixes=self._ignore_prefixes,
            )
            fallback_fragment_by_path = {
                frag.path: frag for frag in fb_dataset.get_fragments()
            }

        fragments_with_offsets: List[Tuple[Any, int]] = []
        for path, chunk_metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
            if path in native_paths:
                fragments_with_offsets.extend(
                    self._native_fragments_for_file(
                        path,
                        chunk_metadata,
                        native_md[path][1],
                        alignment_by_path[path],
                    )
                )
            else:
                fragment = fallback_fragment_by_path[path]
                if chunk_metadata is None:
                    fragments_with_offsets.append((fragment, 0))
                else:
                    fragments_with_offsets.extend(
                        _fragments_from_chunk_metadata(fragment, chunk_metadata)
                    )

        return fragments_with_offsets, columns_to_synthesize, scanner_kwargs

    @staticmethod
    def _native_fragments_for_file(
        path: str,
        chunk_metadata: Optional[dict],
        row_group_num_rows: List[int],
        alignment: Optional[_ColumnAlignment] = None,
    ) -> List[Tuple[_NativeParquetFragment, int]]:
        """Build native fragments for one file, matching the base reader's
        granularity so ``row_hash`` offsets are identical:

        - whole file (``chunk_metadata is None``) → one fragment over *all* row
          groups at offset 0 (the base emits one whole-file fragment);
        - a chunk → one fragment *per row group* in ``[start, end)``, each seeded
          with the pre-filter file row offset of that group's first row (mirrors
          :func:`_fragments_from_chunk_metadata`), so post-filter accumulation
          within a group starts from the right position.

        ``alignment`` is the file's post-decode fixup plan, embedded in every
        fragment so it survives the threaded fragment dispatch.
        """
        if chunk_metadata is None:
            return [(_NativeParquetFragment(path, None, alignment), 0)]

        total = len(row_group_num_rows)
        start = min(chunk_metadata["row_group_start"], total)
        end = min(chunk_metadata["row_group_end"], total)
        fragments: List[Tuple[_NativeParquetFragment, int]] = []
        file_row_offset = sum(row_group_num_rows[:start])
        for rg in range(start, end):
            fragments.append(
                (_NativeParquetFragment(path, [rg], alignment), file_row_offset)
            )
            file_row_offset += row_group_num_rows[rg]
        return fragments

    @staticmethod
    def _native_count_fragments(
        path: str,
        chunk_metadata: Optional[dict],
        row_group_num_rows: List[int],
    ) -> List[Tuple[_NativeCountFragment, int]]:
        """Build zero-decode count fragments for one file (empty projection, no
        predicate), at the same granularity/offsets as
        :meth:`_native_fragments_for_file` so ``limit`` slicing and any
        synthesized columns (``path``, partitions) behave identically."""
        if chunk_metadata is None:
            return [(_NativeCountFragment(path, sum(row_group_num_rows)), 0)]

        total = len(row_group_num_rows)
        start = min(chunk_metadata["row_group_start"], total)
        end = min(chunk_metadata["row_group_end"], total)
        fragments: List[Tuple[_NativeCountFragment, int]] = []
        file_row_offset = sum(row_group_num_rows[:start])
        for rg in range(start, end):
            fragments.append(
                (_NativeCountFragment(path, row_group_num_rows[rg]), file_row_offset)
            )
            file_row_offset += row_group_num_rows[rg]
        return fragments

    @cached_property
    def _tuning(self) -> _ArrowRsTuning:
        """Resolve the arrow-rs tuning knobs for this reader.

        Each knob comes from the ``arrow_rs_*`` key in ``dataset_kwargs`` when
        present (popped into ``self._arrow_rs_tuning`` by
        ``ParquetFileReader.__init__``), else from its ``RAY_DATA_ARROW_RS_*``
        env var, else the built-in default — see the "Tuning knobs" section at
        the top of this module for what each knob does and how to tune it. A
        ``None`` value means "use the default", consistent with the
        format-kwarg convention. Invalid values raise loudly (a mis-set perf
        knob must not silently degrade a benchmark or production read).
        """

        def resolve(key: str, default: Optional[int], minimum: int) -> Optional[int]:
            value = self._arrow_rs_tuning.get(key)
            if value is None:
                return default
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(
                    f"'{key}' in 'dataset_kwargs' must be an int, got {value!r}"
                )
            if value < minimum:
                raise ValueError(
                    f"'{key}' in 'dataset_kwargs' must be >= {minimum}, got {value}"
                )
            return value

        return _ArrowRsTuning(
            decode_budget_bytes=resolve(
                "arrow_rs_decode_budget_bytes", _ARROW_RS_DECODE_BUDGET_BYTES, 1
            ),
            k=resolve("arrow_rs_k", _ARROW_RS_K, 1),
            split_threshold_bytes=resolve("arrow_rs_split_threshold_bytes", None, 0),
            fetch_window_mb=resolve(
                "arrow_rs_fetch_window_mb", _ARROW_RS_FETCH_WINDOW_MB, 0
            ),
            prefetch_windows=resolve(
                "arrow_rs_prefetch_windows", _ARROW_RS_PREFETCH_WINDOWS, 1
            ),
        )

    @cached_property
    def _pushdown_predicate_json(self) -> Optional[str]:
        """The pushed predicate lowered to the native pruning IR (JSON), or
        ``None`` when there's nothing prunable. Depends only on
        ``self._predicate``, so it's computed once and reused for every
        fragment. See :func:`_predicate_to_ir` for the (total, conservative)
        lowering and the soundness argument."""
        return _predicate_json(self._predicate)

    def _filesystem_supported(self) -> bool:
        """Whether the native crate can read from this filesystem at all.
        Local and S3 are wired in `_iter_fragment_tables` / the native `read()`
        (S3 uses the windowed, byte-budgeted native path). Any other
        filesystem (GCS, ABFS, HTTP, …) falls back to PyArrow."""
        from pyarrow.fs import LocalFileSystem, S3FileSystem

        # ``None`` means the default local filesystem (matching
        # ``native_metadata_supported_filesystem`` and the non-S3 native read
        # path); treat it as supported so eligible local reads don't silently
        # fall back to PyArrow.
        return self._filesystem is None or isinstance(
            self._filesystem, (LocalFileSystem, S3FileSystem)
        )

    def _blocking_format_kwargs(self, aligned_ok: bool) -> Dict[str, Any]:
        """Parquet-format kwargs (the ``dataset_kwargs`` payload spread into
        ``pds.ParquetFileFormat``) that the native path cannot honor — a
        non-empty result forces a PyArrow fallback, which honors them all.

        The audit rule is an explicit ALLOWLIST, so a format kwarg added by a
        future pyarrow version is *unsupported until proven supported* — never
        silently ignored (e.g. pyarrow 21+'s ``binary_type`` / ``list_type`` /
        ``arrow_extensions_enabled`` change the decoded schema, and
        ``thrift_string_size_limit`` changes which files are *rejected*, so
        ignoring any of them would diverge from the PyArrow paths):

        - :data:`_FORMAT_KWARGS_PERF_ONLY` (``pre_buffer``, ``buffer_size``,
          ``use_buffered_stream``, ``cache_options``) tune PyArrow's I/O
          strategy only and cannot change decoded bytes; the crate has its own
          I/O strategy (byte-budgeted streaming + fetch window), so they are
          safely ignorable natively.
        - :data:`_FORMAT_KWARGS_ALIGNED` (``coerce_int96_timestamp_unit``,
          ``dictionary_columns``) are reproduced by the *planned* path via
          :class:`_ColumnAlignment`, and
          :data:`_FORMAT_KWARGS_FOOTER_VERIFIED` (the thrift limits) are
          enforced by the planned path's pyarrow footer probe
          (:meth:`_verify_footer_limits`) — both admitted only with
          ``aligned_ok=True``. The per-fragment re-gate can plan neither an
          alignment nor a probe (see :meth:`_reader_level_supported`), so
          there they block (``aligned_ok=False``).
        - :data:`_FORMAT_KWARGS_SCHEMA_SHAPED` (``binary_type``,
          ``list_type``) are admitted on the planned path only when a unified
          dataset schema is pinned: the pin is the output-type authority (it
          silently *undoes* these kwargs on the base path too), and the
          alignment's drift casts already produce the pinned types. Without a
          pinned schema the kwargs genuinely change output types — fall back.
        - ``page_checksum_verification=True`` is admitted everywhere: the
          crate is built with parquet's ``crc`` feature and always verifies
          stored page CRCs, so ``True`` *is* the native behavior. An explicit
          ``False`` — the opt-out for reading a file despite corrupt
          checksums — is something the crate build cannot honor, so it falls
          back to PyArrow (the only reader that can skip the check).
        - A ``None`` value means "pyarrow default" for every format kwarg, so
          ``None``-valued keys never block.
        """
        allowed = _FORMAT_KWARGS_PERF_ONLY | (
            (_FORMAT_KWARGS_ALIGNED | _FORMAT_KWARGS_FOOTER_VERIFIED)
            if aligned_ok
            else frozenset()
        )
        if aligned_ok and self._file_dataset_schema is not None:
            allowed |= _FORMAT_KWARGS_SCHEMA_SHAPED
        blocked: Dict[str, Any] = {}
        for key, value in self._parquet_format_kwargs.items():
            if value is None or key in allowed:
                continue
            if key == "page_checksum_verification" and value is True:
                continue
            blocked[key] = value
        return blocked

    def _reader_level_supported(self) -> bool:
        """Reader-wide half of the *per-fragment* re-gate (the pyarrow-fragment
        path in :meth:`_iter_fragment_tables`): filesystem + Parquet-format
        kwargs. The aligned-kwarg checks stay here — not in the planned native
        ``read()`` — because the per-fragment path has no crate footer metadata
        to plan a :class:`_ColumnAlignment` from (a pyarrow
        ``physical_schema`` already reflects ``coerce_int96_timestamp_unit`` /
        ``dictionary_columns``, so an alignment computed from it would be a
        false no-op). The planned path handles both kwargs natively via
        :meth:`_plan_column_alignment`."""
        if not self._filesystem_supported():
            return False
        if self._blocking_format_kwargs(aligned_ok=False):
            return False
        return True

    def _columns_supported(
        self,
        physical_schema: pa.Schema,
        read_columns: Optional[List[str]],
        int96_columns: Optional[List[str]] = None,
    ) -> bool:
        """Per-fragment re-gate verdict: native only when the decode needs *no*
        post-decode fixups. Used by the pyarrow-fragment path, where the
        alignment can't be trusted (see :meth:`_reader_level_supported`); the
        planned ``read()`` instead admits any file with a plannable
        :class:`_ColumnAlignment`."""
        alignment = self._plan_column_alignment(
            physical_schema, read_columns, int96_columns
        )
        return alignment is not None and alignment.is_noop

    def _plan_column_alignment(
        self,
        physical_schema: pa.Schema,
        read_columns: Optional[List[str]],
        int96_columns: Optional[List[str]] = None,
    ) -> Optional[_ColumnAlignment]:
        """Per-file half of the support gate, upgraded from a yes/no verdict to
        a *plan*: how to make this file's native decode match what the pyarrow
        scanner would produce. Returns ``None`` for a pyarrow fallback, a no-op
        alignment for a byte-identical native decode, or a fixup plan
        (null-fill / cast / reorder) the decode path applies per batch.

        Takes a ``pa.Schema`` (the crate's ``read_metadata`` schema — i.e. what
        the crate will actually decode) plus, optionally, the root column names
        the crate reports as INT96-physical. Still conservative — anything not
        covered falls back to PyArrow, so correctness is never at risk:

        - empty projection (count scan) → handled upstream by the zero-decode
          count path (:class:`_NativeCountFragment`) when there's no predicate;
          ``None`` here so the per-fragment re-gate keeps PyArrow's stub dance;
        - a column absent from this file (schema evolution) → **null-fill**
          with the unified type (``None`` when there's no unified schema to
          take the type from, or the fill type is an extension);
        - an INT96 column → **cast** to timestamp[ns] (PyArrow's default; an
          exact upcast from any embedded hint unit). When
          ``coerce_int96_timestamp_unit`` is set the file **falls back**
          instead: decode-time coercion floors, a post-decode cast truncates
          toward zero — irreconcilable on pre-1970 values. Non-timestamp /
          tz-carrying INT96 oddities stay on PyArrow;
        - a forced ``dictionary_columns`` read → **cast** to
          ``dictionary<int32, type>`` (what PyArrow's forced-dict decode
          yields); non-string/binary targets stay on PyArrow;
        - a per-file type that differs from the unified schema → **cast** to
          the unified type (the scanner's implicit cast under a pinned
          schema); extension-typed drift stays on PyArrow.
        """
        unified_schema = self._file_dataset_schema
        int96 = set(int96_columns or ())
        # The expected output columns: the explicit read set when projected;
        # otherwise the *unified* schema's columns (the scanner outputs the
        # pinned dataset schema, null-filling what a file lacks — using the
        # file's own names here would silently drop evolved columns); the
        # file's names only when there is no unified schema to pin.
        if read_columns is not None:
            names = read_columns
        elif unified_schema is not None:
            names = list(unified_schema.names)
        else:
            names = list(physical_schema.names)

        if read_columns is not None and len(read_columns) == 0:
            return None

        coerce_unit = self._parquet_format_kwargs.get("coerce_int96_timestamp_unit")
        dictionary_columns = set(
            self._parquet_format_kwargs.get("dictionary_columns") or ()
        )

        null_fill: List[Tuple[str, pa.DataType]] = []
        casts: List[Tuple[str, pa.DataType, bool]] = []
        for name in names:
            idx = physical_schema.get_field_index(name)
            if idx == -1:
                # Column absent from this file (schema evolution): null-fill
                # with the unified type — exactly pyarrow's behavior under a
                # pinned dataset schema. Without a unified schema the fill type
                # is unknowable — defer to PyArrow.
                if unified_schema is None:
                    return None
                unified_idx = unified_schema.get_field_index(name)
                if unified_idx == -1:
                    return None
                fill_type = unified_schema.field(unified_idx).type
                if _is_extension_type(fill_type):
                    return None
                null_fill.append((name, fill_type))
                continue

            field_type = physical_schema.field(idx).type
            target = field_type
            allow_time_truncate = False
            if name in int96:
                if coerce_unit is not None:
                    # A cast cannot reproduce pyarrow's decode-time coercion:
                    # pyarrow FLOORS (parquet types.h Int96GetXxx divide the
                    # unsigned nanos-of-day before adding the signed day
                    # offset) while a post-decode cast truncates the signed
                    # total toward zero — one unit apart on every pre-1970
                    # value with a sub-unit remainder (measured: all 1715
                    # negative values in the 1964 corpus fixture). The kwarg
                    # is honored by decoding this file via pyarrow instead.
                    return None
                # No kwarg: pyarrow decodes INT96 to timestamp[ns, no tz]; the
                # crate instead honors an embedded non-ns arrow-schema hint.
                # Realign by casting to ns — an exact upcast (multiplication),
                # never a truncation.
                if not (pa.types.is_timestamp(target) and target.tz is None):
                    return None  # nested/tz-carrying INT96 oddity — stay safe
                target = pa.timestamp("ns")
            if name in dictionary_columns:
                # PyArrow's forced dictionary decode yields
                # dictionary<values=type, indices=int32>. Only string/binary
                # columns are dictionary-read by pyarrow's parquet layer.
                if not (pa.types.is_string(target) or pa.types.is_binary(target)):
                    return None
                target = pa.dictionary(pa.int32(), target)
            if unified_schema is not None:
                unified_idx = unified_schema.get_field_index(name)
                if unified_idx != -1:
                    unified_type = unified_schema.field(unified_idx).type
                    if unified_type != target:
                        # Per-file drift vs the pinned unified schema: the
                        # scanner casts implicitly; mirror it. Extension-typed
                        # drift (e.g. per-file tensor shapes) must not be cast.
                        if _is_extension_type(unified_type) or _is_extension_type(
                            target
                        ):
                            return None
                        target = unified_type

            if target != field_type:
                casts.append((name, target, allow_time_truncate))

        if not null_fill and not casts:
            return _NOOP_ALIGNMENT
        # Appended null-fill columns must land in read order (the scanner's
        # projected-column order); reordering is only needed when a column was
        # appended.
        order = tuple(names) if null_fill else None
        return _ColumnAlignment(
            null_fill=tuple(null_fill), casts=tuple(casts), order=order
        )

    def _arrow_rs_supported(
        self,
        fragment: pds.ParquetFileFragment,
        read_columns: Optional[List[str]],
    ) -> bool:
        """Whole-gate verdict for a pyarrow fragment: reader-level checks plus
        the per-file column/type checks against the fragment's physical schema.
        Used by the per-fragment ``_iter_fragment_tables`` path.

        A fragment's ``physical_schema`` is PyArrow's *post-coercion* Arrow schema,
        so an INT96 column already reads as ``timestamp[ns]`` and can't reveal
        whether the crate would decode it differently (it honors an embedded
        non-ns hint). This re-gate can't see the crate's output, so it is
        conservative: any INT96-physical read column falls the fragment back to
        PyArrow. The authoritative plan-time gate (:meth:`_columns_supported` with
        the crate's ``int96_columns``) is what admits INT96→ns files to the native
        path; this path only ever *withholds*, never wrongly admits.
        """
        if not self._reader_level_supported():
            return False
        int96_roots = _pyarrow_fragment_int96_roots(fragment)
        if int96_roots:
            names = (
                read_columns
                if read_columns is not None
                else list(fragment.physical_schema.names)
            )
            if any(name in int96_roots for name in names):
                return False
        return self._columns_supported(fragment.physical_schema, read_columns)

    @staticmethod
    def _import_extension():
        """Import the native extension, raising a clear, actionable error if it
        isn't built. Called on every native entry point so a missing module
        surfaces loudly (never a silent fall back to PyArrow, which would
        corrupt benchmark attribution)."""
        try:
            import ray_data_arrow_rs

            return ray_data_arrow_rs
        except ImportError as e:
            raise ImportError(
                "use_arrow_rs_parquet_reader=True requires the "
                "'ray_data_arrow_rs' extension. Build it with "
                "`maturin develop --release` from "
                "python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs/."
            ) from e

    def _resolve_read_columns_for(self, scanner_kwargs: dict) -> Optional[List[str]]:
        """The set of columns the native decode must read from the file: the
        projected columns plus any columns referenced only by the pushed filter
        (which we still filter on post-decode). ``None`` means all columns."""
        from ray.data._internal.datasource.parquet_datasource import (
            _resolve_read_columns,
        )
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        columns = scanner_kwargs.get("columns")
        filter_expr = scanner_kwargs.get("filter")
        filter_columns = (
            get_column_references(self._predicate)
            if self._predicate is not None
            else None
        )
        return _resolve_read_columns(columns, filter_expr, filter_columns)

    @override
    def _iter_fragment_tables(
        self,
        fragment: pds.Fragment,
        scanner_kwargs: dict,
    ) -> "Iterator[pa.Table]":
        # Native front-end (arrow-rs ``read()``) hands us pyarrow-free work units.
        if isinstance(fragment, _NativeCountFragment):
            # Empty projection, no predicate: the footer count is exact — yield
            # a zero-column table with the right num_rows and decode nothing.
            # (``Table.select([])`` preserves ``num_rows``; the base path's
            # zero-column tables flow through ``_postprocess`` identically.)
            _trace_reader_path(True)
            if fragment.num_rows > 0:
                yield pa.table({"__num_rows": pa.nulls(fragment.num_rows)}).select([])
            return
        if isinstance(fragment, _NativeParquetFragment):
            _trace_reader_path(True)
            yield from self._iter_native_tables(
                fragment.path,
                fragment.row_groups,
                scanner_kwargs,
                alignment=fragment.alignment,
            )
            return

        # Pyarrow fragment (used when ``read()`` is not overridden, e.g. the
        # reader-level-unsupported delegate, and by unit tests that drive this
        # method directly). Re-check the per-fragment gate and either decode
        # natively or fall back to the PyArrow scanner.
        read_columns = self._resolve_read_columns_for(scanner_kwargs)
        supported = self._arrow_rs_supported(fragment, read_columns)
        _trace_reader_path(supported)
        if not supported:
            _raise_if_strict_no_fallback(
                f"fragment {fragment.path!r} rejected by the per-fragment "
                "support gate"
            )
            yield from super()._iter_fragment_tables(fragment, scanner_kwargs)
            return

        row_groups = (
            [rg.id for rg in fragment.row_groups]
            if fragment.row_groups is not None
            else None
        )
        yield from self._iter_native_tables(fragment.path, row_groups, scanner_kwargs)

    def _iter_native_tables(
        self,
        path: str,
        row_groups: Optional[List[int]],
        scanner_kwargs: dict,
        alignment: Optional[_ColumnAlignment] = None,
    ) -> "Iterator[pa.Table]":
        """Decode ``row_groups`` of ``path`` via the native crate and yield
        ``pa.Table`` batches, applying the file's :class:`_ColumnAlignment`
        (null-fill / cast / reorder) to each batch *before* the post-decode
        filter so the predicate sees the same types pyarrow's scanner filters
        on.

        Row-group pruning is native (``predicate.rs``), replacing PyArrow's
        ``fragment.subset(filter=...)``: we hand the crate the row-group ids plus
        the pushed predicate lowered to a JSON IR, and it drops the groups whose
        footer statistics prove no row can match before fetching or decoding
        them. Pruning is conservative by construction — a missing column, absent
        stats, or an uncomparable type all *keep* the group — so it can only ever
        avoid IO/decode, never change which rows surface. Row-level filtering
        then runs post-decode here (the final authority), and a fully-pruned file
        simply yields nothing.
        """
        ray_data_arrow_rs = self._import_extension()

        from pyarrow.fs import S3FileSystem

        columns = scanner_kwargs.get("columns")
        filter_expr = scanner_kwargs.get("filter")
        batch_size = scanner_kwargs.get("batch_size") or _ARROW_DEFAULT_BATCH_SIZE
        read_columns = self._resolve_read_columns_for(scanner_kwargs)
        predicate_json = self._pushdown_predicate_json

        tuning = self._tuning
        split_threshold = tuning.split_threshold_bytes
        if split_threshold is None:
            split_threshold = (
                self._target_block_size
                if self._target_block_size is not None
                else _ARROW_RS_DEFAULT_SPLIT_THRESHOLD_BYTES
            )

        fs = self._filesystem
        if isinstance(fs, S3FileSystem):
            # pyarrow filesystem paths are normally scheme-less ("bucket/key"),
            # but strip a leading "s3://" defensively so we never split it into
            # a bogus "s3:" bucket.
            if path.startswith("s3://"):
                path = path[len("s3://") :]
            bucket, _, key = path.partition("/")
            cfg = _s3_config(fs)
            reader = ray_data_arrow_rs.read_row_groups_s3(
                bucket,
                key,
                cfg["region"],
                cfg["anonymous"],
                endpoint=cfg["endpoint"],
                access_key_id=cfg["access_key_id"],
                secret_access_key=cfg["secret_access_key"],
                session_token=cfg["session_token"],
                allow_http=cfg["allow_http"],
                virtual_hosted_style=cfg["virtual_hosted_style"],
                row_groups=row_groups,
                columns=read_columns,
                batch_size=batch_size,
                decode_budget_bytes=tuning.decode_budget_bytes,
                fetch_window_mb=tuning.fetch_window_mb,
                k=tuning.k,
                split_threshold_bytes=split_threshold,
                prefetch_windows=tuning.prefetch_windows,
                predicate_json=predicate_json,
            )
        else:
            reader = ray_data_arrow_rs.read_row_groups(
                path,
                row_groups,
                read_columns,
                batch_size,
                tuning.decode_budget_bytes,
                tuning.k,
                split_threshold,
                predicate_json,
            )

        record_batch_reader = pa.RecordBatchReader.from_stream(reader)

        # Yield each budget-sized batch straight through. The read op's
        # BlockOutputBuffer coalesces to target_max_block_size downstream (same
        # as the PyArrow path) — accumulating a full block here too would just
        # stack a second block-sized buffer on top of it. See module docstring.
        for batch in record_batch_reader:
            table = pa.Table.from_batches([batch], schema=record_batch_reader.schema)
            table = _apply_column_alignment(table, alignment)
            # Same opt-in gate as the pyarrow path: unpickling an
            # ArrowPythonObjectType column executes arbitrary code, so serving
            # one requires the explicit env opt-in. Before the row filter — the
            # check is schema-based, and pyarrow's scanner raises even for
            # batches the filter would empty out.
            if not self._allow_pickle_object_columns:
                raise_on_pickle_object_columns(table)
            if filter_expr is not None:
                table = table.filter(filter_expr)
                if table.num_rows == 0:
                    continue
            if columns is not None:
                table = table.select([c for c in columns if c in table.column_names])
            yield table
