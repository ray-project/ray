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

import logging
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import product
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import pandas as pd
from fsspec.core import split_protocol
from fsspec.spec import AbstractFileSystem

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow import fs as pyarrow_fs
    from zarr import Array as ZarrArray
    from zarr.hierarchy import Group as ZarrGroup

    from ray.data.context import DataContext

    ZarrRoot = ZarrGroup | ZarrArray


@dataclass(frozen=True)
class ZarrArrayMeta:
    """``shape``/``chunks``/``dtype`` for a single Zarr v2 array."""

    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str

    @classmethod
    def from_zarr_array(cls, arr: "ZarrArray") -> ZarrArrayMeta:
        """Adapt an opened ``zarr.Array`` (already validated by zarr on open)."""
        return cls(
            shape=tuple(int(s) for s in arr.shape),
            chunks=tuple(int(c) for c in arr.chunks),
            dtype=str(arr.dtype),
        )

    @property
    def rank(self) -> int:
        return len(self.shape)

    @property
    def itemsize(self) -> int:
        """Bytes per element."""
        return np.dtype(self.dtype).itemsize

    def effective_chunks(
        self,
        array_name: str,
        user_chunk_shape: tuple[int, ...] | dict[str, tuple[int, ...]] | None,
    ) -> tuple[int, ...]:
        """Resolve the user's ``chunk_shapes`` override(s) against this array's chunks.

        When ``user_chunk_shape`` is a single sequence, it is treated as a
        prefix that overrides the leading axes; trailing axes keep the
        array's native chunk values. This lets a single
        ``chunk_shapes=[16]`` apply meaningfully across arrays of different
        ranks (e.g., 4-D images alongside 2-D poses).

        When ``user_chunk_shape`` is a dict, it is interpreted as a
        per-array mapping from array path to that array's override prefix.
        Arrays omitted from the mapping keep their native chunks.

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

        if isinstance(user_chunk_shape, dict):
            user_chunk_shape = user_chunk_shape.get(array_name)
            if user_chunk_shape is None:
                return self.chunks
        if len(user_chunk_shape) > self.rank:
            raise ValueError(
                f"chunk_shapes override for array {array_name!r} has "
                f"{len(user_chunk_shape)} axes but array of shape "
                f"{self.shape!r} has rank {self.rank}. Each chunk_shapes "
                f"override may not be longer than its target array's rank."
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
# Chunk reading
# ---------------------------------------------------------------------------


def _read_chunk(
    root: ZarrRoot,
    array_name: str,
    chunk_slices: tuple[tuple[int, int], ...],
) -> np.ndarray:
    """Read ``array[chunk_slices]`` from a Zarr root.

    ``chunk_slices`` is an N-tuple of ``(start, stop)`` pairs, one per axis.
    For a 0-D (scalar) array it is the empty tuple ``()``, which reads the
    single element.

    Transient I/O errors (throttling, 5xx, connection resets, timeouts) are
    retried by the underlying filesystem/storage backend, which owns the retry
    policy: ``s3fs``/botocore and ``pyarrow.fs.S3FileSystem`` retry by default
    and are tunable on the ``filesystem`` passed to ``read_zarr`` (e.g. botocore
    ``retries`` config or pyarrow ``retry_strategy``).
    """
    indexer = tuple(slice(s, e) for s, e in chunk_slices)
    arr = root if array_name == "" else root[array_name]
    # ``arr`` is a zarr Array here (the caller resolves a concrete array path),
    # but zarr's types widen it to Array | Group; asarray pins the ndarray return.
    return np.asarray(arr[indexer])


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


def _validate_chunk_shapes_dict(chunk_shapes: dict) -> dict[str, tuple[int, ...]]:
    from zarr.util import normalize_storage_path

    normalized_chunk_shapes: dict[str, tuple[int, ...]] = {}
    original_keys_by_normalized: dict[str, str] = {}

    for k, v in chunk_shapes.items():
        if not isinstance(k, str):
            raise ValueError(
                "chunk_shapes dict keys must be array-path strings, "
                f"got key {k!r} of type {type(k).__name__}"
            )

        if not isinstance(v, (tuple, list)) or not v:
            raise ValueError(
                f"chunk_shapes[{k!r}] must be non-empty sequence of "
                f"positive integers (list or tuple), got {v!r}"
            )
        if any(isinstance(x, bool) or not isinstance(x, int) or x <= 0 for x in v):
            raise ValueError(
                f"chunk_shapes[{k!r}] must be a non-empty sequence of "
                f"positive integers (list or tuple), got {v!r}"
            )

        normalized_key = normalize_storage_path(k)

        if normalized_key in original_keys_by_normalized:
            prev_key = original_keys_by_normalized[normalized_key]
            raise ValueError(
                "chunk_shapes contains duplicate array paths after normalization: "
                f"{prev_key!r} and {k!r} both normalize to {normalized_key!r}"
            )

        original_keys_by_normalized[normalized_key] = k
        normalized_chunk_shapes[normalized_key] = tuple(v)
    return normalized_chunk_shapes


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


class ZarrV2Datasource(Datasource):
    """Reads one or more Zarr v2 arrays into a Ray Data ``Dataset``.

    Two output schemas, selected at the call site via ``align_axis_0``:

    Long-form (default, ``align_axis_0=False``) — one row per chunk per
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
    same axis-0 chunk size after :paramref:`chunk_shapes` resolution; if
    they don't, ``__init__`` raises ``ValueError`` with a hint pointing at
    the largest aligned subset. Use :paramref:`array_paths` to pick which
    arrays to read — ``align_axis_0`` itself does not filter.

    See :func:`ray.data.read_zarr` for the public API.
    """

    def __init__(
        self,
        path: str,
        filesystem: pyarrow_fs.FileSystem | AbstractFileSystem | None = None,
        chunk_shapes: dict[str, list] | list | None = None,
        array_paths: list[str] | None = None,
        allow_full_metadata_scan: bool = False,
        align_axis_0: bool = False,
        overlap: int = 0,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")

        # This datasource targets Zarr v2 stores via zarr-python 2.x APIs
        # (``zarr.util.normalize_storage_path``, ``.zarray`` metadata,
        # ``zarr.open(fs.get_mapper(...))``) that were removed/reworked in
        # zarr-python 3.x. Fail fast with an actionable message rather than a
        # cryptic ImportError mid-read if an incompatible version is installed.
        import zarr

        if int(zarr.__version__.split(".")[0]) >= 3:
            raise ImportError(
                f"read_zarr supports zarr-python 2.x (Zarr v2 stores), but found "
                f"zarr=={zarr.__version__}. Install a compatible version with "
                f"`pip install 'zarr<3'`."
            )

        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.paths = [str(path)]

        # Resolve filesystem + store path. The order of precedence:
        #   1. Explicit ``filesystem=`` always wins.
        #   2. ``.zip`` URL/path: auto-wrap with fsspec's ZipFileSystem.
        #   3. Otherwise delegate to Ray Data's standard URL to filesystem
        #      helper (the same one every other ``read_*`` API uses).
        # "store path" is the path to the Zarr store, relative to the filesystem root.
        # It is used to construct the Zarr root object.
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
            from fsspec.implementations.zip import ZipFileSystem

            if isinstance(self._fs, ZipFileSystem) and self.paths[0].endswith(".zip"):
                # An explicit archive filesystem: the store is the archive root,
                # not a ``.zip``-named entry inside it. (A real sub-path within
                # the archive is preserved by the scheme-strip below.)
                self._store_path = ""
            else:
                # Strip any URI scheme (e.g. ``gs://`` / ``s3://``) so the path
                # is backend-relative; pyarrow filesystems (wrapped in
                # ``ArrowFSWrapper``) require this. Mirrors the
                # ``filesystem is None`` branch, which strips the scheme via
                # ``_resolve_paths_and_filesystem``.
                _, store_path = split_protocol(self.paths[0])
                self._store_path = store_path.rstrip("/")

        if chunk_shapes is not None and not isinstance(
            chunk_shapes, (tuple, list, dict)
        ):
            raise ValueError(
                f"chunk_shapes must be a non-empty sequence of positive "
                f"integers (list or tuple), or a dict, got {chunk_shapes!r}"
            )

        self.chunk_shapes: tuple[int, ...] | dict[str, tuple[int, ...]] | None = None
        if chunk_shapes is not None:
            if isinstance(chunk_shapes, dict):
                self.chunk_shapes = _validate_chunk_shapes_dict(chunk_shapes)
            else:
                if not chunk_shapes or any(
                    isinstance(x, bool) or not isinstance(x, int) or x <= 0
                    for x in chunk_shapes
                ):
                    raise ValueError(
                        "chunk_shapes must be a non-empty sequence of positive integers "
                        f"(list or tuple), got {chunk_shapes!r}"
                    )

                self.chunk_shapes = tuple(chunk_shapes)

        # Open the store with zarr (consolidated metadata when available). zarr
        # reads and validates `.zarray`/`.zmetadata` here, so the datasource does
        # not re-check that metadata itself.
        store = self._fs.get_mapper(self._store_path)
        z_meta_path = f"{self._store_path.rstrip('/')}/.zmetadata"
        self._consolidated = self._fs.exists(z_meta_path)
        if self._consolidated:
            self.root = zarr.open_consolidated(store, mode="r")
        else:
            self.root = zarr.open(store, mode="r")

        self._metadata_by_path = self._load_metadata(array_paths)
        if not self._metadata_by_path:
            raise ValueError(
                f"No arrays discovered in Zarr store at {self.paths[0]!r}."
            )

        # Reject per-array overrides that do not correspond to any selected
        # array in this read.
        if isinstance(self.chunk_shapes, dict):
            unknown_chunk_shape_keys = sorted(
                set(self.chunk_shapes) - set(self._metadata_by_path)
            )
            if unknown_chunk_shape_keys:
                raise ValueError(
                    f"Unknown array path(s) in chunk_shapes: {unknown_chunk_shape_keys}"
                )

        if not isinstance(align_axis_0, bool):
            raise TypeError(
                f"align_axis_0 must be a bool, got {type(align_axis_0).__name__}"
            )

        if not align_axis_0:
            self._aligned_array_names = None
        else:
            scalar_arrays = sorted(
                name for name, meta in self._metadata_by_path.items() if not meta.shape
            )
            if scalar_arrays:
                raise ValueError(
                    f"align_axis_0=True requires every selected array to have "
                    f"at least one axis, but these are 0-D (scalar): "
                    f"{scalar_arrays}. Drop them with array_paths=[...]."
                )
            shape0_by_array = {
                name: meta.shape[0] for name, meta in self._metadata_by_path.items()
            }
            if len(set(shape0_by_array.values())) > 1:
                raise ValueError(
                    f"All selected arrays must share shape[0] when "
                    f"align_axis_0=True. Got: {shape0_by_array}. Pass a "
                    f"shape-compatible subset via array_paths=[...]."
                )
            self._aligned_array_names = list(self._metadata_by_path.keys())

        # Validate overlap. Only meaningful when arrays are co-iterated as
        # wide rows, since the trailing lookahead is exposed via the
        # per-array column being longer than ``t_stop - t_start``.
        if isinstance(overlap, bool) or not isinstance(overlap, int) or overlap < 0:
            raise ValueError(f"overlap must be a non-negative integer, got {overlap!r}")
        if overlap and self._aligned_array_names is None:
            raise ValueError(
                "overlap requires align_axis_0=True. In the default long-form "
                "(chunk-per-row) mode, there's no wide row to extend forward — "
                "the ``chunk_slices`` column on each chunk row already exposes "
                "the global axis-0 range."
            )
        self.overlap = overlap

        # Resolve per-array chunk geometry. ``effective_chunks`` raises a
        # ``ValueError`` if a shared ``chunk_shapes`` prefix or any per-array
        # ``chunk_shapes`` override is longer than the target array's rank —
        # so this loop is also where rank validation happens.
        self._array_chunks: dict[str, tuple[int, ...]] = {}
        self._array_grids: dict[str, tuple[int, ...]] = {}
        for name, meta in self._metadata_by_path.items():
            chunks = meta.effective_chunks(name, self.chunk_shapes)
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
                    f"Got: {axis_0_chunks}. Pass chunk_shapes=[N] (or a "
                    f"per-array chunk_shapes dict that resolves all aligned "
                    f"arrays to the same axis-0 prefix) to re-tile them."
                )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Total bytes = sum over selected arrays of ``prod(shape) * itemsize``."""
        return sum(
            math.prod(meta.shape) * meta.itemsize
            for meta in self._metadata_by_path.values()
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
        for name, meta in self._metadata_by_path.items():
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
            n_tasks = max(1, min(parallelism, len(descriptors)))
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
            * self._metadata_by_path[desc.array_name].itemsize
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
        shape0 = self._metadata_by_path[first_name].shape[0]

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

        n_tasks = max(1, min(parallelism, len(descriptors)))
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
        """Sum bytes across all (row, aligned-array) pairs in a wide-row batch.

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
                self._metadata_by_path[name] for name in self._aligned_array_names
            )
        )

    def _load_metadata(self, array_paths) -> dict[str, ZarrArrayMeta]:
        """Read ``shape``/``chunks``/``dtype`` for the selected arrays off ``self.root``.

        zarr validated the store's metadata when it was opened, so this only
        adapts the resulting ``zarr.Array`` objects. Discovery uses consolidated
        metadata when present, then explicit ``array_paths``, then an optional
        full scan (``allow_full_metadata_scan``). If ``array_paths`` is given,
        the discovered set is filtered down to it.
        """
        import zarr
        from zarr.util import normalize_storage_path

        root = self.root

        if isinstance(root, zarr.Array):
            return {"": ZarrArrayMeta.from_zarr_array(root)}

        requested = (
            {normalize_storage_path(p) for p in array_paths} if array_paths else None
        )

        if not self._consolidated and not self.allow_full_metadata_scan:
            if requested is None:
                raise ValueError(
                    "No array_paths were provided and this Zarr store does not "
                    "contain .zmetadata. Pass array_paths=[...] or set "
                    "allow_full_metadata_scan=True."
                )
            out: dict[str, ZarrArrayMeta] = {}
            for raw in array_paths:
                name = normalize_storage_path(raw)
                try:
                    arr = root[name]
                except KeyError as e:
                    raise ValueError(
                        f"Array path {raw!r} not found in Zarr store."
                    ) from e
                out[name] = ZarrArrayMeta.from_zarr_array(arr)
            return out

        all_arrays: dict[str, ZarrArrayMeta] = {}

        def _collect(name: str, obj) -> None:
            if isinstance(obj, zarr.Array):
                all_arrays[name] = ZarrArrayMeta.from_zarr_array(obj)

        root.visititems(_collect)

        if requested is not None:
            missing = sorted(requested - all_arrays.keys())
            if missing:
                raise ValueError(
                    f"Array(s) not found: {', '.join(repr(m) for m in missing)}. "
                    f"Available: {', '.join(repr(a) for a in sorted(all_arrays))}"
                )
            all_arrays = {k: v for k, v in all_arrays.items() if k in requested}

        return all_arrays
