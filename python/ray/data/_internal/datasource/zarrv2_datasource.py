from __future__ import annotations

import logging
import math
import numbers
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import (
    _check_import,
    _is_local_scheme,
    iterate_with_retry,
)
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem
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
        return np.dtype(self.dtype).itemsize

    def effective_chunks(
        self,
        array_name: str,
        user_chunk_shape: tuple[int, ...] | dict[str, tuple[int, ...]] | None,
    ) -> tuple[int, ...]:
        """Resolve the user's ``chunk_shapes`` override(s) against this array's chunks.

        A single sequence overrides the leading axes (trailing axes keep the
        native chunks), so one ``chunk_shapes=[16]`` applies across arrays of
        different ranks. A dict maps array path → that array's override prefix;
        arrays absent from it keep native chunks. ``None`` keeps native chunks;
        an override longer than the array's rank raises ``ValueError``.
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
    retry_match: Optional[List[str]] = None,
) -> np.ndarray:
    """Read ``array[chunk_slices]`` as an ndarray.

    The underlying filesystem's own retry policy still applies underneath.
    """

    def _read() -> np.ndarray:
        indexer = tuple(slice(s, e) for s, e in chunk_slices)
        arr = root if array_name == "" else root[array_name]
        return np.asarray(arr[indexer])

    if not retry_match:
        return _read()
    # TODO(Artur): This would be more elegant with a general retry helper for non-iterables.
    return next(
        iterate_with_retry(
            lambda: [_read()], description="read a Zarr chunk", match=retry_match
        )
    )


@dataclass(frozen=True)
class _ChunkRange:
    """A contiguous slice ``[flat_start, flat_stop)`` of an array's chunk grid.

    The flat indices address the row-major flattening of the chunk grid; the
    read fn unravels each to an N-D ``chunk_index`` lazily on the worker. Keeping
    a range (not a materialized per-chunk list) makes read-task planning
    O(parallelism) rather than O(total chunks) -- important for stores with very
    many chunks.
    """

    array_name: str
    meta: ZarrArrayMeta
    chunks: tuple[int, ...]
    grid: tuple[int, ...]
    flat_start: int
    flat_stop: int


@dataclass(frozen=True)
class _AlignedChunkDescriptor:
    """One wide row: a global axis-0 range ``[t_start, t_stop)`` across the
    aligned arrays. With ``overlap > 0`` the row's data extends to
    ``t_stop_data = min(t_stop + overlap, shape[0])`` (lookahead so windows
    starting in this row reach their tail without crossing a row boundary).
    """

    chunk_index: int
    t_start: int
    t_stop: int
    t_stop_data: int


def _create_read_fn(
    chunk_range: _ChunkRange,
    root: ZarrRoot,
    per_task_row_limit: Optional[int],
    retry_match: Optional[List[str]],
) -> Callable[[], Iterable[Block]]:
    """Build a callable that materializes one block for a chunk-grid range.

    This is the case where arrays are not aligned. Chunks are enumerated lazily
    (on the worker) from ``chunk_range``. ``per_task_row_limit`` caps how many
    chunks this task reads so a downstream ``limit`` reads only what it needs
    (``None`` reads the whole range).
    """
    cr = chunk_range
    stop = cr.flat_stop
    if per_task_row_limit is not None:
        stop = min(stop, cr.flat_start + per_task_row_limit)

    def read_fn() -> Iterable[Block]:
        builder = DelegatingBlockBuilder()
        for flat_index in range(cr.flat_start, stop):
            chunk_index = tuple(int(i) for i in np.unravel_index(flat_index, cr.grid))
            chunk_slices = cr.meta.chunk_slices(chunk_index, cr.chunks)
            builder.add(
                {
                    "array": cr.array_name,
                    "chunk_index": chunk_index,
                    "chunk_slices": chunk_slices,
                    "chunk": _read_chunk(
                        root, cr.array_name, chunk_slices, retry_match
                    ),
                }
            )
        yield builder.build()

    return read_fn


def _create_aligned_read_fn(
    batch: list[_AlignedChunkDescriptor],
    aligned_array_names: list[str],
    root: ZarrRoot,
    per_task_row_limit: Optional[int],
    retry_match: Optional[List[str]],
) -> Callable[[], Iterable[Block]]:
    """Build a callable for aligned (wide-row) reads.

    Each output row carries ``t_start``, ``t_stop``, and one column per
    aligned array holding that array's ``[t_start:t_stop, ...]`` slice at
    its natural shape (edge rows may be shorter). All arrays in one row
    share the same axis-0 range.

    This is the case where arrays are aligned on axis 0. ``per_task_row_limit``
    caps how many rows this task reads (``None`` reads the whole batch).
    """
    batch = batch[:per_task_row_limit]

    def read_fn() -> Iterable[Block]:
        builder = DelegatingBlockBuilder()
        for d in batch:
            row: dict[str, Any] = {"t_start": d.t_start, "t_stop": d.t_stop}
            for name in aligned_array_names:
                row[name] = _read_chunk(
                    root, name, ((d.t_start, d.t_stop_data),), retry_match
                )
            builder.add(row)
        yield builder.build()

    return read_fn


def _is_positive_int(x) -> bool:
    """True for a positive integer, including NumPy integers; False for bool."""
    return not isinstance(x, bool) and isinstance(x, numbers.Integral) and int(x) > 0


def _validate_chunk_shapes_dict(chunk_shapes: dict) -> dict[str, tuple[int, ...]]:
    """Normalize chunk_shapes keys to store paths and validate their values."""
    from zarr.util import normalize_storage_path

    normalized: dict[str, tuple[int, ...]] = {}
    for k, v in chunk_shapes.items():
        if (
            not isinstance(v, (tuple, list))
            or not v
            or not all(_is_positive_int(x) for x in v)
        ):
            raise ValueError(
                f"chunk_shapes[{k!r}] must be a non-empty sequence of positive "
                f"integers (list or tuple), got {v!r}"
            )
        normalized[normalize_storage_path(k)] = tuple(int(x) for x in v)
    return normalized


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


class ZarrV2Datasource(Datasource):
    """Reads one or more Zarr v2 arrays into a Ray Data ``Dataset``.

    Emits long-form rows (one per chunk per array) or, with
    ``align_axis_0=True``, wide rows (one per axis-0 chunk, one column per
    array). See :func:`ray.data.read_zarr` for the row schemas and full API.
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
        import zarr

        _check_import(self, module="fsspec", package="fsspec")
        from fsspec.spec import AbstractFileSystem

        if int(zarr.__version__.split(".")[0]) >= 3:
            raise ImportError(
                f"read_zarr supports zarr-python 2.x (Zarr v2 stores), but found "
                f"zarr=={zarr.__version__}. Install a compatible version with "
                f"`pip install 'zarr<3'`."
            )

        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.paths = [str(path)]
        # ``local://`` stores live on the driver's local disk, so pin reads to
        # the driver node (workers on other nodes can't see those files).
        self._supports_distributed_reads = not _is_local_scheme(self.paths)

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
                # not a ``.zip``-named entry inside it.
                self._store_path = ""
            else:
                from fsspec.core import split_protocol

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
                if not chunk_shapes or not all(
                    _is_positive_int(x) for x in chunk_shapes
                ):
                    raise ValueError(
                        "chunk_shapes must be a non-empty sequence of positive integers "
                        f"(list or tuple), got {chunk_shapes!r}"
                    )

                self.chunk_shapes = tuple(int(x) for x in chunk_shapes)

        # Open the store with zarr (consolidated metadata when available).
        # Detect consolidation by *trying* ``open_consolidated``.
        store = self._fs.get_mapper(self._store_path)
        try:
            self.root = zarr.open_consolidated(store, mode="r")
            self._consolidated = True
        except KeyError:
            self.root = zarr.open(store, mode="r")
            self._consolidated = False

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
        if not isinstance(overlap, int) or overlap < 0:
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

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads

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
        """Enumerate every chunk and wrap it (or batches of chunks) in ReadTasks."""
        from ray.data.context import DataContext

        retry_match = (data_context or DataContext.get_current()).retried_io_errors
        if self._aligned_array_names is not None:
            return self._get_aligned_read_tasks(
                parallelism, per_task_row_limit, retry_match
            )
        return self._get_long_form_read_tasks(
            parallelism, per_task_row_limit, retry_match
        )

    def _get_long_form_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int],
        retry_match: Optional[List[str]],
    ) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        for name, meta in self._metadata_by_path.items():
            chunks = self._array_chunks[name]
            grid = self._array_grids[name]
            n_chunks = math.prod(grid)
            if n_chunks == 0:
                continue
            # Split the chunk grid into contiguous flat-index ranges. This is
            # O(n_tasks), not O(n_chunks): we never materialize a per-chunk list
            # on the driver -- the read fn unravels chunks lazily on the worker.
            n_tasks = max(1, min(parallelism, n_chunks))
            batch_size = math.ceil(n_chunks / n_tasks)
            for flat_start in range(0, n_chunks, batch_size):
                flat_stop = min(flat_start + batch_size, n_chunks)
                chunk_range = _ChunkRange(
                    name, meta, chunks, grid, flat_start, flat_stop
                )
                read_tasks.append(
                    ReadTask(
                        _create_read_fn(
                            chunk_range, self.root, per_task_row_limit, retry_match
                        ),
                        BlockMetadata(
                            num_rows=flat_stop - flat_start,
                            size_bytes=self._estimate_range_mem_size(chunk_range),
                            input_files=(self.paths[0],),
                            exec_stats=None,
                        ),
                        per_task_row_limit=per_task_row_limit,
                    )
                )
        return read_tasks

    def _estimate_range_mem_size(self, chunk_range: _ChunkRange) -> int:
        """Upper-bound in-memory bytes for a chunk-grid range.

        Assumes a full-size chunk per index; trailing-edge chunks are smaller,
        so this slightly over-estimates. O(1) -- it does not enumerate the range.
        """
        n = chunk_range.flat_stop - chunk_range.flat_start
        return n * math.prod(chunk_range.chunks) * chunk_range.meta.itemsize

    def _get_aligned_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int],
        retry_match: Optional[List[str]],
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
                        batch,
                        self._aligned_array_names,
                        self.root,
                        per_task_row_limit,
                        retry_match,
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
        requested = (
            {normalize_storage_path(p) for p in array_paths} if array_paths else None
        )

        if isinstance(root, zarr.Array):
            # A store that is itself an array exposes exactly one path: "" (root).
            # Reject any requested path that isn't the root so a bad ``array_paths``
            # fails loudly here instead of silently returning the root array.
            if requested is not None and requested != {""}:
                raise ValueError(
                    f"This Zarr store is a single root-level array (path ''), "
                    f"but array_paths={array_paths!r} requested other path(s). "
                    f"Pass array_paths=[''] or omit it."
                )
            return {"": ZarrArrayMeta.from_zarr_array(root)}

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
                if not isinstance(arr, zarr.Array):
                    raise ValueError(f"Array path {raw!r} is a group, not an array.")
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
