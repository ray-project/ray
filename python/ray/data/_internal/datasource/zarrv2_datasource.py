"""Zarr v2 datasource for Ray Data.

Each output row corresponds to **one chunk of one array** in the store.
Arrays read in the same call need not share any dimension; they coexist as
separate rows in the output, distinguished by an ``array`` column. See
:class:`ZarrV2Datasource` for the row schema and :func:`ray.data.read_zarr`
for the public API.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import product
from typing import TYPE_CHECKING, Any, List, Optional

import numpy as np
import pandas as pd
from fsspec.spec import AbstractFileSystem

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow
    from zarr import Array as ZarrArray
    from zarr.hierarchy import Group as ZarrGroup

    from ray.data.context import DataContext

    ZarrRoot = ZarrGroup | ZarrArray


REQUIRED_ZARRAY_KEYS = ("shape", "chunks", "dtype")


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

        ``user_chunk_shape`` is treated as a **prefix** that overrides the
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
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> np.ndarray:
    """Read ``array[chunk_slices]`` from a Zarr root with network-retry.

    ``chunk_slices`` is an N-tuple of ``(start, stop)`` pairs, one per axis.
    For a 0-D (scalar) array it is the empty tuple ``()``, which reads the
    single element.
    """
    last_error: Optional[BaseException] = None
    retry_keywords = (
        "connection reset",
        "timeout",
        "connection refused",
        "network",
        "socket",
        "http error",
    )
    indexer = tuple(slice(s, e) for s, e in chunk_slices)
    for attempt in range(max_retries):
        try:
            # Resolve the array inside the retry loop: on remote stores
            # ``root[array_name]`` reads the ``.zarray`` metadata file and
            # can fail transiently the same way the data read can.
            arr = root if array_name == "" else root[array_name]
            return arr[indexer]
        except Exception as e:
            last_error = e
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in retry_keywords):
                delay = base_delay * (2**attempt)
                logger.warning(
                    "Network error reading array=%s slices=%s, "
                    "attempt %s/%s, retrying in %.1fs: %s",
                    array_name,
                    chunk_slices,
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
            else:
                raise
    raise RuntimeError(
        f"Failed to read array={array_name!r} slices={chunk_slices} "
        f"after {max_retries} attempts"
    ) from last_error


@dataclass(frozen=True)
class _ChunkDescriptor:
    """One row's worth of read work: which chunk of which array to read."""

    array_name: str
    chunk_index: tuple[int, ...]
    chunk_slices: tuple[tuple[int, int], ...]


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


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


class ZarrV2Datasource(Datasource):
    """Reads chunks of one or more Zarr v2 arrays as a long-form Dataset.

    Each output row corresponds to **one chunk of one array**:

    * ``array``: the source array's path within the store
      (e.g., ``"data/camera0_rgb"``, or ``""`` for a root-level array).
    * ``chunk_index``: the N-D position of this chunk in the array's chunk
      grid, as a tuple of ints.
    * ``chunk_slices``: per-axis ``(start, stop)`` of this chunk in the
      source array's coordinate space — the global slice the chunk
      occupies. Lets downstream code map chunks back to original
      positions (e.g., for episode-aware processing) without recomputing
      from ``chunk_index`` and the chunk shape.
    * ``chunk``: the chunk's data as an ``ndarray``, at its natural shape
      (possibly shorter than the nominal chunk shape at trailing boundaries —
      no padding is applied).

    Arrays read in the same call need **not** share any dimension. Different
    ranks, shapes, dtypes, and chunk shapes coexist as separate rows in the
    output. Users who want paired/aligned views across arrays should compose
    with downstream ``zip``/``map_groups`` operations.

    See :func:`ray.data.read_zarr` for the public API.
    """

    def __init__(
        self,
        path: str,
        filesystem: pyarrow.fs.FileSystem | AbstractFileSystem | None = None,
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")

        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.paths = [str(path)]

        # Resolve filesystem (zarr requires fsspec) and store path. When no
        # filesystem is provided, delegate URL resolution to Ray Data's
        # standard helper so we get the same handling as every other
        # ``read_*`` API (e.g., ``s3://anonymous@<bucket>/...`` URLs).
        if filesystem is None:
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

        # Validate chunk_shape eagerly so the error message comes from this
        # entry point rather than the per-array resolution loop below.
        if chunk_shape is not None:
            if not chunk_shape or any(
                not isinstance(c, int) or c <= 0 for c in chunk_shape
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

        # Lazy zarr import: only needed once we read.
        import zarr

        self.root = zarr.open(self._fs.get_mapper(self._store_path), mode="r")

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
        """Enumerate every chunk and split into batches **per array**.

        Each :class:`ReadTask` processes chunks from a single array and
        yields one ``DataFrame`` whose rows are ``(array, chunk_index, chunk)``.
        Per-array batching keeps each block's ``chunk`` column rank-uniform
        (Arrow's tensor extension requires this).

        ``parallelism`` is treated as a *per-array* budget — each array's
        chunks are split into ``min(parallelism, n_chunks_for_array)`` tasks.
        """
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
                            size_bytes=self._estimate_batch_mem_size(batch),
                            input_files=(self.paths[0],),
                            exec_stats=None,
                        ),
                        per_task_row_limit=per_task_row_limit,
                    )
                )
        return read_tasks

    def _estimate_batch_mem_size(self, batch: list[_ChunkDescriptor]) -> int:
        """Sum in-memory bytes across all chunks in one batch."""
        return sum(
            math.prod(stop - start for start, stop in desc.chunk_slices)
            * self._selected_arrays[desc.array_name].itemsize
            for desc in batch
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
