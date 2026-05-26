"""Zarr v2 datasource for Ray Data.

Two output schemas, selected at the call site:

* Long-form (default). Each output row corresponds to one chunk of one
  array. Arrays in the same call need not share any dimension; they coexist
  as separate rows distinguished by an ``array`` column.
* Wide-form (``align_axis_0=True``). Each output row is one axis-0 chunk
  shared across all selected arrays; the row carries one column per array
  plus ``t_start`` / ``t_stop`` for the global range.

See :class:`ZarrV2Datasource` for the row schemas and
:func:`ray.data.read_zarr` for the public API.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from itertools import product
from typing import TYPE_CHECKING, Any, List, Optional

import numpy as np
import pandas as pd
from fsspec.spec import AbstractFileSystem

from ray._common.retry import call_with_retry
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow
    from zarr import Array as ZarrArray
    from zarr.hierarchy import Group as ZarrGroup

    from ray.data.context import DataContext

    ZarrRoot = ZarrGroup | ZarrArray


REQUIRED_ZARRAY_KEYS = ("shape", "chunks", "dtype")

# Zarr-specific transient-error patterns appended to the user's
# ``DataContext.retried_io_errors`` when reading chunks. The defaults in
# ``DataContext`` cover AWS-flavored object-store errors; these cover the
# kind of network-layer messages that bubble up through fsspec/numcodecs
# when reading chunked array data over HTTPS/S3/GCS.
_ZARR_TRANSIENT_ERROR_PATTERNS = (
    "Connection reset",
    "Read timeout",
    "Connection refused",
    "network",
    "socket",
    "HTTP error",
)


@dataclass(frozen=True)
class ZarrArrayMeta:
    """Validated ``.zarray`` metadata for a single Zarr v2 array."""

    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str

    @classmethod
    def from_json(cls, raw_meta: dict[str, Any], array_path: str) -> ZarrArrayMeta:
        """Validate and parse a ``.zarray`` JSON object into a ZarrArrayMeta.

        Raises ``ValueError`` if any of ``shape``/``chunks``/``dtype`` is
        missing. ``array_path`` is included in the error message so callers
        don't have to thread context themselves.
        """
        missing = [k for k in REQUIRED_ZARRAY_KEYS if k not in raw_meta]
        if missing:
            raise ValueError(
                f"Invalid .zarray metadata for array path {array_path!r}: "
                f"missing required key(s) {missing}."
            )
        return cls(
            shape=tuple(int(x) for x in raw_meta["shape"]),
            chunks=tuple(int(x) for x in raw_meta["chunks"]),
            dtype=str(raw_meta["dtype"]),
        )

    @property
    def rank(self) -> int:
        return len(self.shape)

    @property
    def itemsize(self) -> int:
        """Bytes per element."""
        return np.dtype(self.dtype).itemsize

    def effective_chunks(
        self, user_chunk_shape: tuple[int, ...] | None
    ) -> tuple[int, ...]:
        """Resolve the user's ``chunk_shape`` against this array's native chunks.

        ``user_chunk_shape`` is treated as a prefix that overrides the
        leading axes; trailing axes keep the array's native chunk values.
        This lets a single ``chunk_shape=[16]`` apply meaningfully across
        arrays of different ranks (e.g., 4-D images alongside 2-D poses).

        - ``None`` → use native chunks unchanged.
        - shorter than rank → override leading axes, keep native for the rest.
        - same length as rank → use as-is.
        - longer than rank → ``ValueError``.

        Example with array shape ``(200, 28, 28)``, native chunks ``(50, 28, 28)``:

            user=None              → (50, 28, 28)
            user=(16,)             → (16, 28, 28)
            user=(16, 14)          → (16, 14, 28)
            user=(16, 14, 14)      → (16, 14, 14)
            user=(16, 14, 14, 1)   → ValueError
        """
        if user_chunk_shape is None:
            return self.chunks
        if len(user_chunk_shape) > self.rank:
            raise ValueError(
                f"chunk_shape has {len(user_chunk_shape)} axes but array of "
                f"shape {self.shape!r} has rank {self.rank}. chunk_shape may "
                f"not be longer than any selected array's rank."
            )
        return user_chunk_shape + self.chunks[len(user_chunk_shape) :]

    def grid_shape(self, chunks: tuple[int, ...]) -> tuple[int, ...]:
        """Number of chunks along each axis under the given chunk shape."""
        return tuple(math.ceil(s / c) for s, c in zip(self.shape, chunks))

    def chunk_slices(
        self, chunk_index: tuple[int, ...], chunks: tuple[int, ...]
    ) -> tuple[tuple[int, int], ...]:
        """Per-axis ``(start, stop)`` for ``array[chunk_index]`` under ``chunks``.

        Trailing-edge chunks are clamped to ``shape[i]``, so they may be
        shorter than ``chunks[i]``. No padding is applied.
        """
        return tuple(
            (i * c, min((i + 1) * c, s))
            for i, c, s in zip(chunk_index, chunks, self.shape)
        )


# ---------------------------------------------------------------------------
# Metadata discovery
# ---------------------------------------------------------------------------


def _load_metadata_from_zmetadata_file(
    fs, z_meta_path: str
) -> dict[str, ZarrArrayMeta]:
    """Load all arrays listed in a consolidated ``.zmetadata`` file."""
    with fs.open(z_meta_path, "rb") as f:
        consolidated = json.load(f)
    if "metadata" not in consolidated:
        raise ValueError(
            f"Missing 'metadata' key in consolidated metadata at {z_meta_path}."
        )
    out: dict[str, ZarrArrayMeta] = {}
    for key, value in consolidated["metadata"].items():
        if not key.endswith(".zarray"):
            continue
        array_path = "" if key == ".zarray" else key[: -len("/.zarray")]
        out[array_path] = ZarrArrayMeta.from_json(value, array_path)
    return out


def _load_metadata_from_array_paths(
    fs, store_path: str, array_paths: Iterable[str]
) -> dict[str, ZarrArrayMeta]:
    """Load ``.zarray`` files for the user's explicit array paths.

    Each path is normalized via :func:`zarr.util.normalize_storage_path`,
    which strips surrounding slashes, collapses doubles, and rejects
    ``.``/``..`` segments. Raises ``ValueError`` if a requested path has
    no ``.zarray`` file at the expected location.
    """
    from zarr.util import normalize_storage_path

    store_root = store_path.rstrip("/")
    out: dict[str, ZarrArrayMeta] = {}
    for raw in array_paths:
        normalized = normalize_storage_path(raw)
        zarray_path = (
            f"{store_root}/{normalized}/.zarray"
            if normalized
            else f"{store_root}/.zarray"
        )
        try:
            with fs.open(zarray_path, "r") as f:
                raw_meta = json.load(f)
        except FileNotFoundError as e:
            raise ValueError(
                f"Array path {raw!r} not found: no .zarray file at {zarray_path}"
            ) from e
        out[normalized] = ZarrArrayMeta.from_json(raw_meta, normalized)
    return out


def _load_metadata_full_scan(fs, store_path: str) -> dict[str, ZarrArrayMeta]:
    """Recursively walk ``store_path`` for ``.zarray`` files.

    Each discovered relative path is canonicalized via
    :func:`zarr.util.normalize_storage_path` so the output keys match the
    format used by the other metadata-loading paths regardless of whether
    the underlying ``fs.walk`` yields trailing slashes.
    """
    from zarr.util import normalize_storage_path

    store_root = store_path.rstrip("/")
    store_prefix = store_root + "/"
    out: dict[str, ZarrArrayMeta] = {}
    for dirpath, _, filenames in fs.walk(store_path):
        if ".zarray" not in filenames:
            continue
        dirpath = dirpath.rstrip("/")
        if dirpath == store_root:
            array_path = ""
        else:
            array_path = normalize_storage_path(dirpath.removeprefix(store_prefix))
        zarray_path = f"{dirpath}/.zarray"
        try:
            with fs.open(zarray_path, "r") as f:
                raw = json.load(f)
        except FileNotFoundError:
            continue
        out[array_path] = ZarrArrayMeta.from_json(raw, array_path)
    return out


# ---------------------------------------------------------------------------
# Chunk reading
# ---------------------------------------------------------------------------


def _read_chunk(
    root: ZarrRoot,
    array_name: str,
    chunk_slices: tuple[tuple[int, int], ...],
    *,
    match: Optional[Sequence[str]] = None,
    max_attempts: int = 10,
    max_backoff_s: int = 32,
) -> np.ndarray:
    """Read ``array[chunk_slices]`` from a Zarr root with transient-error retry.

    ``chunk_slices`` is an N-tuple of ``(start, stop)`` pairs, one per axis.
    For a 0-D (scalar) array it is the empty tuple ``()``, which reads the
    single element.

    Retries are delegated to :func:`ray._common.retry.call_with_retry`,
    matching the pattern used by other Ray Data datasources (lance,
    iceberg). ``match`` defaults to ``DataContext.retried_io_errors``
    (covers the AWS-flavored object-store transient errors) plus a small
    set of zarr-specific network patterns. Pass an explicit ``match``
    sequence to override.
    """
    indexer = tuple(slice(s, e) for s, e in chunk_slices)

    def _read() -> np.ndarray:
        arr = root if array_name == "" else root[array_name]
        return arr[indexer]

    if match is None:
        match = list(DataContext.get_current().retried_io_errors) + list(
            _ZARR_TRANSIENT_ERROR_PATTERNS
        )
    return call_with_retry(
        _read,
        description=f"read zarr chunk array={array_name!r} slices={chunk_slices}",
        match=match,
        max_attempts=max_attempts,
        max_backoff_s=max_backoff_s,
    )


@dataclass(frozen=True)
class _ChunkDescriptor:
    """One long-form row's worth of read work: which chunk of which array."""

    array_name: str
    chunk_index: tuple[int, ...]
    chunk_slices: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class _AlignedChunkDescriptor:
    """One wide-row's worth of read work: a global axis-0 range across N aligned arrays.

    The row "owns" the range ``[t_start, t_stop)`` and reports those as
    columns. When ``overlap > 0``, the row's actual data extends to
    ``t_stop_data`` (which is ``min(t_stop + overlap, shape[0])``); the
    trailing slice is the lookahead from the next row's owned range so
    sliding windows that start in this row's owned range can reach their
    full tail without crossing a Ray Data row boundary.
    """

    chunk_index: int
    t_start: int
    t_stop: int
    t_stop_data: int


def _create_read_fn(
    batch: list[_ChunkDescriptor],
    root: ZarrRoot,
) -> Callable[[], Iterable[pd.DataFrame]]:
    """Build a read-task callable that materializes one DataFrame for one batch.

    Each output row carries ``(array, chunk_index, chunk)``. ``chunk`` is
    the data at its natural shape — possibly shorter than the nominal chunk
    shape at trailing boundaries.

    The caller is expected to pass batches whose chunks all come from one
    array. Arrow's tensor extension requires all tensor elements in a
    column to share rank, so mixing 4-D image chunks with 1-D label chunks
    in one block would fail at conversion time.
    :meth:`ZarrV2Datasource.get_read_tasks` enforces this by allocating one
    batch per array.
    """

    def read_fn() -> Iterable[pd.DataFrame]:
        yield pd.DataFrame(
            {
                "array": [d.array_name for d in batch],
                "chunk_index": [d.chunk_index for d in batch],
                "chunk_slices": [d.chunk_slices for d in batch],
                "chunk": [
                    _read_chunk(root, d.array_name, d.chunk_slices) for d in batch
                ],
            }
        )

    return read_fn


def _create_aligned_read_fn(
    batch: list[_AlignedChunkDescriptor],
    aligned_array_names: list[str],
    root: ZarrRoot,
) -> Callable[[], Iterable[pd.DataFrame]]:
    """Build a read-task callable for aligned (wide-row) reads.

    Each output row carries ``t_start``, ``t_stop``, and one column per
    aligned array holding that array's ``[t_start:t_stop, ...]`` slice at
    its natural shape (edge rows may be shorter). All arrays in one row
    share the same axis-0 range.
    """

    def read_fn() -> Iterable[pd.DataFrame]:
        cols: dict[str, list] = {
            "t_start": [d.t_start for d in batch],
            "t_stop": [d.t_stop for d in batch],
        }
        for name in aligned_array_names:
            cols[name] = [
                _read_chunk(root, name, ((d.t_start, d.t_stop_data),)) for d in batch
            ]
        yield pd.DataFrame(cols)

    return read_fn


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


class ZarrV2Datasource(Datasource):
    """Reads one or more Zarr v2 arrays into a Ray Data ``Dataset``.

    Two output schemas, selected at the call site via ``align_axis_0``:

    Long-form (default, ``align_axis_0=None``) — one row per chunk per
    array. Columns:

    * ``array``: the source array's path within the store
      (e.g., ``"data/camera0_rgb"``, or ``""`` for a root-level array).
    * ``chunk_index``: the N-D position of this chunk in the array's chunk
      grid, as a tuple of ints.
    * ``chunk_slices``: per-axis ``(start, stop)`` of this chunk in the
      source array's coordinate space.
    * ``chunk``: the chunk's data as an ``ndarray`` at its natural shape
      (possibly shorter at trailing boundaries — no padding).

    Arrays in the same call need not share any dimension; they coexist as
    separate rows distinguished by ``array``.

    Wide-form (opt-in, ``align_axis_0=True``) — one row per axis-0
    chunk, with one column per selected array. Columns:

    * ``t_start`` / ``t_stop``: global axis-0 range of this row.
    * ``<array_name>``: that array's ``[t_start:t_stop, ...]`` slice
      (one column per selected array).

    All selected arrays must share ``shape[0]`` and must end up with the
    same axis-0 chunk size after :paramref:`chunk_shape` resolution; if
    they don't, ``__init__`` raises ``ValueError`` with a hint pointing at
    the largest aligned subset. Use :paramref:`array_paths` to pick which
    arrays to read — ``align_axis_0`` itself does not filter.

    See :func:`ray.data.read_zarr` for the public API.
    """

    def __init__(
        self,
        path: str,
        filesystem: pyarrow.fs.FileSystem | AbstractFileSystem | None = None,
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False,
        align_axis_0: bool | None = None,
        overlap: int | None = None,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")

        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.paths = [str(path)]

        # Resolve filesystem + store path. The order of precedence:
        #   1. Explicit ``filesystem=`` always wins.
        #   2. ``.zip`` URL/path → auto-wrap with fsspec's ZipFileSystem.
        #   3. Otherwise delegate to Ray Data's standard URL→filesystem
        #      helper (the same one every other ``read_*`` API uses).
        if filesystem is None and self.paths[0].endswith(".zip"):
            import fsspec

            self._fs = fsspec.filesystem("zip", fo=self.paths[0])
            self._store_path = ""
        elif filesystem is None:
            from fsspec.implementations.arrow import ArrowFSWrapper

            from ray.data.datasource.path_util import (
                _resolve_paths_and_filesystem,
            )

            resolved_paths, pa_fs = _resolve_paths_and_filesystem([self.paths[0]])
            self._fs = ArrowFSWrapper(pa_fs)
            self._store_path = resolved_paths[0].rstrip("/")
        else:
            from pyarrow.fs import FileSystem

            if isinstance(filesystem, AbstractFileSystem):
                self._fs = filesystem
            elif isinstance(filesystem, FileSystem):
                from fsspec.implementations.arrow import ArrowFSWrapper

                self._fs = ArrowFSWrapper(filesystem)
            else:
                raise TypeError(
                    f"filesystem must be pyarrow.fs.FileSystem or "
                    f"fsspec.spec.AbstractFileSystem, got "
                    f"{type(filesystem).__name__}"
                )
            self._store_path = self.paths[0].rstrip("/")

        # Validate chunk_shape and canonicalize to a tuple. The tuple form
        # is what ``ZarrArrayMeta.effective_chunks`` consumes.
        if chunk_shape is not None:
            if (
                not isinstance(chunk_shape, list)
                or not chunk_shape
                or any(not isinstance(c, int) or c <= 0 for c in chunk_shape)
            ):
                raise ValueError(
                    f"chunk_shape must be a non-empty list of positive "
                    f"integers, got {chunk_shape!r}"
                )
        self.chunk_shape: tuple[int, ...] | None = (
            tuple(chunk_shape) if chunk_shape is not None else None
        )

        self._selected_arrays = self._load_metadata(array_paths)
        if not self._selected_arrays:
            raise ValueError(
                f"No arrays discovered in Zarr store at {self.paths[0]!r}."
            )

        # Aligned-mode setup: validate the alignment request and filter
        # ``self._selected_arrays`` down to the aligned subset. The
        # per-array effective-chunks loop below then validates that all
        # aligned arrays have the same axis-0 chunk.
        self._aligned_array_names: list[str] | None = self._resolve_align_axis_0(
            align_axis_0
        )

        # Validate overlap. Only meaningful when arrays are co-iterated as
        # wide rows, since the trailing lookahead is exposed via the
        # per-array column being longer than ``t_stop - t_start``.
        if overlap is not None and (not isinstance(overlap, int) or overlap < 0):
            raise ValueError(
                f"overlap must be a non-negative integer or None, got " f"{overlap!r}"
            )
        if overlap and self._aligned_array_names is None:
            raise ValueError(
                "overlap requires align_axis_0 to be set. In the default "
                "long-form (chunk-per-row) mode, there's no wide row to "
                "extend forward — exposed via the ``chunk_slices`` column "
                "on each chunk row instead."
            )
        self.overlap: int = overlap or 0

        # Resolve per-array chunk geometry. ``effective_chunks`` raises a
        # ``ValueError`` if the user's ``chunk_shape`` is longer than any
        # selected array's rank — so this loop is also where rank validation
        # happens.
        self._array_chunks: dict[str, tuple[int, ...]] = {}
        self._array_grids: dict[str, tuple[int, ...]] = {}
        for name, meta in self._selected_arrays.items():
            chunks = meta.effective_chunks(self.chunk_shape)
            self._array_chunks[name] = chunks
            self._array_grids[name] = meta.grid_shape(chunks)

        # If aligned, all listed arrays must share the same axis-0 chunk size
        # so each wide row corresponds to one axis-0 step across every array.
        if self._aligned_array_names is not None:
            axis_0_chunks = {
                name: self._array_chunks[name][0] for name in self._aligned_array_names
            }
            unique = set(axis_0_chunks.values())
            if len(unique) > 1:
                raise ValueError(
                    f"Aligned arrays must share the same axis-0 chunk size. "
                    f"Got: {axis_0_chunks}. Pass chunk_shape=[N] (or a "
                    f"per-array chunk_shape dict that resolves all aligned "
                    f"arrays to the same axis-0 prefix) to re-tile them."
                )

        # Lazy zarr import: ``zarr`` is a hard dep of this datasource (gated
        # by ``_check_import`` above) but ``import ray.data`` shouldn't drag
        # it in for users who never call ``read_zarr``.
        import zarr

        self.root = zarr.open(self._fs.get_mapper(self._store_path), mode="r")

    def _resolve_align_axis_0(self, align_axis_0: bool | None) -> list[str] | None:
        """Validate ``align_axis_0`` and return the aligned array names in order.

        Returns ``None`` when alignment is off (long-form mode). Otherwise
        returns the ordered list of selected array names after asserting
        they all share ``shape[0]``.

        ``align_axis_0`` does not filter ``_selected_arrays`` — the user
        chooses which arrays to read via ``array_paths``, and this method
        only validates that the resulting set is mutually aligned.
        """
        if not align_axis_0:
            return None

        if align_axis_0 is not True:
            raise TypeError(
                f"align_axis_0 must be a bool or None, got "
                f"{type(align_axis_0).__name__}"
            )

        shape0_by_array = {
            name: meta.shape[0] if meta.shape else 0
            for name, meta in self._selected_arrays.items()
        }
        if len(set(shape0_by_array.values())) > 1:
            from collections import Counter

            most_common_size, _ = Counter(shape0_by_array.values()).most_common(1)[0]
            aligned_subset = sorted(
                n for n, s in shape0_by_array.items() if s == most_common_size
            )
            raise ValueError(
                f"All selected arrays must share shape[0] when "
                f"align_axis_0=True. Got: {shape0_by_array}. Largest aligned "
                f"subset has shape[0]={most_common_size}: {aligned_subset}. "
                f"Pass that subset via array_paths=[...] to read only those "
                f"arrays."
            )

        return list(self._selected_arrays.keys())

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Total bytes = sum over selected arrays of ``prod(shape) * itemsize``."""
        return sum(
            math.prod(meta.shape) * meta.itemsize
            for meta in self._selected_arrays.values()
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Enumerate every chunk and wrap it (or batches of chunks) in ReadTasks.

        Long-form mode (default): one task per per-array chunk batch.
        Per-array batching keeps each block's ``chunk`` column rank-uniform
        (Arrow's tensor extension requires this). ``parallelism`` is
        treated as a per-array budget — each array's chunks are split into
        ``min(parallelism, n_chunks_for_array)`` tasks.

        Aligned mode (``align_axis_0=True``): one task per batch of
        aligned axis-0 chunks. Each yielded row carries ``t_start``,
        ``t_stop``, and one column per selected array containing that
        array's slice for the row's axis-0 range.
        """
        # ``data_context`` is part of the Datasource ABC; this datasource
        # doesn't read anything off it today (no context-aware behavior).
        # Threaded through to the helpers so they keep the same signature
        # in case a future change needs it.
        if self._aligned_array_names is not None:
            return self._get_aligned_read_tasks(
                parallelism, per_task_row_limit, data_context
            )
        return self._get_long_form_read_tasks(
            parallelism, per_task_row_limit, data_context
        )

    def _get_long_form_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Long-form read tasks. See :meth:`get_read_tasks` for semantics."""
        read_tasks: List[ReadTask] = []
        for name, meta in self._selected_arrays.items():
            chunks = self._array_chunks[name]
            grid = self._array_grids[name]
            descriptors = [
                _ChunkDescriptor(
                    array_name=name,
                    chunk_index=chunk_index,
                    chunk_slices=meta.chunk_slices(chunk_index, chunks),
                )
                for chunk_index in product(*(range(n) for n in grid))
            ]
            if not descriptors:
                continue
            n_tasks = min(parallelism, len(descriptors))
            batch_size = math.ceil(len(descriptors) / n_tasks)
            for start in range(0, len(descriptors), batch_size):
                batch = descriptors[start : start + batch_size]
                read_tasks.append(
                    ReadTask(
                        _create_read_fn(batch, self.root),
                        BlockMetadata(
                            num_rows=len(batch),
                            size_bytes=self._estimate_long_form_batch_mem_size(batch),
                            input_files=(self.paths[0],),
                            exec_stats=None,
                        ),
                        per_task_row_limit=per_task_row_limit,
                    )
                )
        return read_tasks

    def _estimate_long_form_batch_mem_size(self, batch: list[_ChunkDescriptor]) -> int:
        """Sum in-memory bytes across all chunks in one long-form batch."""
        return sum(
            math.prod(stop - start for start, stop in desc.chunk_slices)
            * self._selected_arrays[desc.array_name].itemsize
            for desc in batch
        )

    def _get_aligned_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Aligned read tasks. See :meth:`get_read_tasks` for semantics."""
        assert self._aligned_array_names is not None
        # All aligned arrays share the same axis-0 chunk size (validated in
        # ``__init__``) and the same shape[0]. Read the geometry off the first.
        first_name = self._aligned_array_names[0]
        axis_0_chunk = self._array_chunks[first_name][0]
        shape0 = self._selected_arrays[first_name].shape[0]

        descriptors = [
            _AlignedChunkDescriptor(
                chunk_index=i,
                t_start=i * axis_0_chunk,
                t_stop=min((i + 1) * axis_0_chunk, shape0),
                t_stop_data=min((i + 1) * axis_0_chunk + self.overlap, shape0),
            )
            for i in range(math.ceil(shape0 / axis_0_chunk))
        ]
        if not descriptors:
            return []

        n_tasks = min(parallelism, len(descriptors))
        batch_size = math.ceil(len(descriptors) / n_tasks)

        read_tasks: List[ReadTask] = []
        for start in range(0, len(descriptors), batch_size):
            batch = descriptors[start : start + batch_size]
            read_tasks.append(
                ReadTask(
                    _create_aligned_read_fn(
                        batch, self._aligned_array_names, self.root
                    ),
                    BlockMetadata(
                        num_rows=len(batch),
                        size_bytes=self._estimate_aligned_batch_mem_size(batch),
                        input_files=(self.paths[0],),
                        exec_stats=None,
                    ),
                    per_task_row_limit=per_task_row_limit,
                )
            )
        return read_tasks

    def _estimate_aligned_batch_mem_size(
        self, batch: list[_AlignedChunkDescriptor]
    ) -> int:
        """Sum bytes across all (row × aligned-array) pairs in a wide-row batch.

        Accounts for the trailing overlap data each row carries: the row's
        per-array slice covers ``[t_start, t_stop_data)``, not just
        ``[t_start, t_stop)``.
        """
        assert self._aligned_array_names is not None
        return sum(
            (desc.t_stop_data - desc.t_start)
            * (math.prod(meta.shape[1:]) if len(meta.shape) > 1 else 1)
            * meta.itemsize
            for desc in batch
            for meta in (
                self._selected_arrays[name] for name in self._aligned_array_names
            )
        )

    def _load_metadata(self, array_paths) -> dict[str, ZarrArrayMeta]:
        """Discover and load ``.zarray`` metadata for the selected arrays.

        Discovery prefers consolidated ``.zmetadata`` when it exists. If the
        store has no ``.zmetadata``, the datasource falls back to reading each
        requested array's ``.zarray`` directly (when ``array_paths`` is given)
        or to a recursive scan (when ``allow_full_metadata_scan`` is set).
        If ``array_paths`` is given, the discovered set is filtered down to it;
        any requested paths that aren't present in the store raise a
        ``ValueError`` listing what is available.
        """
        fs, store_path = self._fs, self._store_path

        z_meta_path = f"{store_path.rstrip('/')}/.zmetadata"
        if fs.exists(z_meta_path):
            logger.debug("Loading .zmetadata file")
            all_arrays = _load_metadata_from_zmetadata_file(fs, z_meta_path)
        elif array_paths:
            logger.debug("No .zmetadata; reading requested .zarray files directly")
            all_arrays = _load_metadata_from_array_paths(fs, store_path, array_paths)
        elif self.allow_full_metadata_scan:
            logger.info(
                "No array_paths provided and no .zmetadata found; "
                "executing full scan of Zarr store metadata"
            )
            all_arrays = _load_metadata_full_scan(fs, store_path)
            if not all_arrays:
                # ``fs.walk`` silently returns nothing on filesystems without
                # directory-listing support (most commonly plain HTTP/HTTPS).
                # That's distinct from "store exists but has no arrays", so
                # surface the likely cause.
                raise ValueError(
                    f"Full-store scan of {self.paths[0]!r} found no .zarray "
                    "files. This can occur if the filesystem does not "
                    "support recursive directory listing (e.g., plain "
                    "HTTP/HTTPS without an object-store listing API). Pass "
                    "array_paths=[...] with explicit array names to read "
                    "from this kind of store."
                )
        else:
            raise ValueError(
                "No array_paths were provided and this Zarr store does not "
                "contain .zmetadata. Pass array_paths=[...] or set "
                "allow_full_metadata_scan=True."
            )

        if array_paths:
            from zarr.util import normalize_storage_path

            requested = {normalize_storage_path(p) for p in array_paths}

            missing = sorted(requested - all_arrays.keys())
            if missing:
                raise ValueError(
                    f"Array(s) not found: {', '.join(repr(m) for m in missing)}. "
                    f"Available: {', '.join(repr(a) for a in sorted(all_arrays))}"
                )
            all_arrays = {k: v for k, v in all_arrays.items() if k in requested}

        return all_arrays
