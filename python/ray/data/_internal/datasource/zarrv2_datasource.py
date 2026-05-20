"""Zarr datasource for Ray Data."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Iterable
from itertools import product
from math import prod
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional, TypedDict
from urllib.parse import urlsplit

import fsspec
import fsspec.core
import numpy as np
import pandas as pd
from fsspec.spec import AbstractFileSystem

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from zarr import Array as ZarrArray
    from zarr.hierarchy import Group as ZarrGroup

    from ray.data.context import DataContext

    ZarrRoot = ZarrGroup | ZarrArray

REQUIRED_ZARRAY_KEYS = ("shape", "chunks", "dtype")

# CPython 64-bit object-size approximations, used to estimate the in-memory
# size of a descriptor-mode output row without instantiating one. Values are
# from `sys.getsizeof` on CPython 3.10+ and are stable enough for Ray Data's
# block-sizing heuristics (they only need to be within an order of magnitude).
_PYINT_BYTES = 28
_PYSTR_BASE = 49  # plus one byte per character
_PYTUPLE_BASE = 56  # plus 8 bytes per element
_PYLIST_BASE = 56  # plus 8 bytes per element
_PYPTR_BYTES = 8

# Cost of one int held inside a tuple or list: the pointer slot in the
# container + the int object itself.
_INT_IN_SEQ = _PYPTR_BYTES + _PYINT_BYTES
# Cost of one (int, int) tuple held inside a list.
_PAIR_IN_LIST = _PYPTR_BYTES + _PYTUPLE_BASE + 2 * _INT_IN_SEQ


class ZarrArrayMeta(TypedDict):
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str


class ZarrChunkRow(TypedDict):
    array: str
    meta: ZarrArrayMeta
    chunk_index: tuple[int, ...]


class ZarrGridData(TypedDict):
    meta: ZarrArrayMeta
    grid_shape: tuple[int, ...]


def _zarr_array_meta_from_json(raw_meta: Any, array_path: str) -> ZarrArrayMeta:
    """Validate and parse a ``.zarray`` JSON object into a :class:`ZarrArrayMeta`.

    Raises ``ValueError`` if ``raw_meta`` is not a JSON object or is missing
    any of :data:`REQUIRED_ZARRAY_KEYS`. ``array_path`` is included in the
    error message so callers don't have to thread context themselves.
    """
    if not isinstance(raw_meta, dict):
        raise ValueError(
            f"Invalid .zarray metadata for array path {array_path!r}: "
            f"expected a JSON object, got {type(raw_meta).__name__}."
        )
    missing = [key for key in REQUIRED_ZARRAY_KEYS if key not in raw_meta]
    if missing:
        raise ValueError(
            f"Invalid .zarray metadata for array path {array_path!r}: "
            f"missing required key(s) {missing}. "
            f"Expected keys: {list(REQUIRED_ZARRAY_KEYS)}."
        )
    return {
        "shape": tuple(int(x) for x in raw_meta["shape"]),
        "chunks": tuple(int(x) for x in raw_meta["chunks"]),
        "dtype": str(raw_meta["dtype"]),
    }


def _descriptor_row_size_bytes(array_name: str, meta: ZarrArrayMeta) -> int:
    """Estimate the in-memory bytes of one descriptor-mode output row.

    A descriptor row has six cells: ``array`` (str), ``dtype`` (str),
    ``array_shape`` and ``chunk_shape`` (each a tuple of ``ndim`` ints),
    ``chunk_slices`` (a list of ``ndim`` ``(int, int)`` tuples), and
    ``padding`` (a list of ``ndim`` ints). The estimate is derived from the
    Python object sizes defined above; the dominant term scales linearly with
    ``ndim``.
    """
    ndim = len(meta["shape"])
    return (
        # "array" cell
        _PYSTR_BASE
        + len(array_name)
        # "dtype" cell
        + _PYSTR_BASE
        + len(meta["dtype"])
        # "array_shape" and "chunk_shape" — two tuples of ndim ints
        + 2 * (_PYTUPLE_BASE + ndim * _INT_IN_SEQ)
        # "chunk_slices" — list of ndim (int, int) tuples
        + _PYLIST_BASE
        + ndim * _PAIR_IN_LIST
        # "padding" — list of ndim ints
        + _PYLIST_BASE
        + ndim * _INT_IN_SEQ
    )


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
        out[array_path] = _zarr_array_meta_from_json(value, array_path)
    return out


def _load_metadata_from_array_paths(
    fs, store_path: str, array_paths: Iterable[str]
) -> dict[str, ZarrArrayMeta]:
    """Load ``.zarray`` files for the user's explicit array paths.

    Normalizes each path: trims slashes and treats ``"."`` as the root array
    (the canonical key ``""``). Raises ``ValueError`` if a requested path has
    no ``.zarray`` file at the expected location.
    """
    store_root = store_path.rstrip("/")
    out: dict[str, ZarrArrayMeta] = {}
    for raw in array_paths:
        normalized = raw.strip("/")
        if normalized == ".":
            normalized = ""
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
        out[normalized] = _zarr_array_meta_from_json(raw_meta, normalized)
    return out


def _load_metadata_full_scan(fs, store_path: str) -> dict[str, ZarrArrayMeta]:
    """Recursively walk ``store_path`` for ``.zarray`` files."""
    store_root = store_path.rstrip("/")
    store_prefix = store_root + "/"
    out: dict[str, ZarrArrayMeta] = {}
    for dirpath, _, filenames in fs.walk(store_path):
        if ".zarray" not in filenames:
            continue
        array_path = (
            ""
            if dirpath.rstrip("/") == store_root
            else dirpath.removeprefix(store_prefix)
        )
        zarray_path = f"{dirpath.rstrip('/')}/.zarray"
        try:
            with fs.open(zarray_path, "r") as f:
                raw = json.load(f)
        except FileNotFoundError:
            continue
        out[array_path] = _zarr_array_meta_from_json(raw, array_path)
    return out


def _chunk_geometry(
    chunk_index: tuple[int, ...],
    shape: tuple[int, ...],
    chunks: tuple[int, ...],
) -> tuple[list[tuple[int, int]], list[int], tuple[int, ...]]:
    """Compute per-dimension slice bounds, trailing padding, and actual shape
    for one chunk of an array.

    ``chunks`` is the array's chunk shape; the chunk at the trailing edge of an
    axis may be smaller than ``chunks[dim]`` if the array's ``shape[dim]`` is
    not divisible. The returned ``chunk_shape`` reflects that truncation, and
    ``padding`` records how many trailing zeros would be needed to pad each
    truncated chunk back to ``chunks``.
    """
    chunk_slices: list[tuple[int, int]] = []
    padding: list[int] = []
    chunk_shape = list(chunks)
    for dim, (i, size, c) in enumerate(zip(chunk_index, shape, chunks)):
        start = i * c
        stop = min((i + 1) * c, size)
        chunk_slices.append((start, stop))
        if start + c > size:
            padding.append(start + c - size)
            chunk_shape[dim] = stop - start
        else:
            padding.append(0)
    return chunk_slices, padding, tuple(chunk_shape)


def _resolve_store(
    path: str, filesystem: AbstractFileSystem | None = None
) -> tuple[AbstractFileSystem, str]:
    """Resolve a user-supplied store path to an ``(fsspec_filesystem, path)`` pair.

    If ``filesystem`` is provided, it's used as-is and ``path`` is trimmed of
    trailing slashes. Otherwise, local paths are resolved via the local
    fsspec filesystem and remote URLs are dispatched through
    :func:`fsspec.core.url_to_fs`.
    """
    parsed = urlsplit(path)

    # if user passes filesystem, use it
    if filesystem is not None:
        return filesystem, path.rstrip("/")

    # local default
    if parsed.scheme in ("", "file"):
        local = path if not parsed.scheme else parsed.path
        root = str(Path(local).resolve())
        return fsspec.filesystem("file"), root

    # Generic fallback
    fs, root = fsspec.core.url_to_fs(path)
    return fs, root.rstrip("/")


def _strip_protocol(path: str, filesystem) -> str:
    """Return ``path`` without its URL scheme, as ``fsspec.get_mapper`` expects.

    Prefers the filesystem's own ``_strip_protocol`` if it exposes one (the
    standard fsspec contract); otherwise removes any ``<scheme>://`` prefix
    found via :func:`urlsplit`.
    """
    if hasattr(filesystem, "_strip_protocol"):
        return filesystem._strip_protocol(path)

    parsed = urlsplit(path)
    if parsed.scheme:
        return path.removeprefix(f"{parsed.scheme}://")
    return path


def _create_read_fn(
    batch: list[ZarrChunkRow], root: ZarrRoot | None = None
) -> Callable[[], Iterable[pd.DataFrame]]:
    """Build a read-task callable for a batch of Zarr chunk descriptors.

    If ``root`` is provided, the returned callable reads and materializes each
    chunk's data into a ``"chunk"`` column. Otherwise, it returns metadata-only
    rows that describe the chunk bounds, shape, dtype, and edge padding needed
    to reconstruct truncated boundary chunks.
    """
    if root is not None:

        def read_fn() -> Iterable[pd.DataFrame]:
            def _read_with_retry(
                array: str,
                chunk_slices: list[tuple[int, int]] | tuple[tuple[int, int], ...],
                max_retries: int = 5,
                base_delay: float = 1.0,
            ) -> np.ndarray:
                import time

                slice_tuple = tuple(slice(start, stop) for start, stop in chunk_slices)
                last_error = None

                for attempt in range(max_retries):
                    try:
                        if array == "":
                            return root[slice_tuple]

                        return root[array][slice_tuple]

                    except Exception as e:
                        last_error = e
                        error_msg = str(e).lower()

                        if any(
                            keyword in error_msg
                            for keyword in [
                                "connection reset",
                                "timeout",
                                "connection refused",
                                "network",
                                "socket",
                                "http error",
                            ]
                        ):
                            delay = base_delay * (2**attempt)
                            logger.warning(
                                "Network error reading array=%s slices=%s, "
                                "attempt %s/%s, retrying in %.1fs: %s",
                                array,
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
                    f"Failed to read array={array!r} slices={chunk_slices} "
                    f"after {max_retries} attempts"
                ) from last_error

            arrays = []
            array_shapes = []
            chunk_shapes = []
            dtypes = []
            full_chunk_slices = []
            full_paddings = []
            chunks = []

            for row in batch:
                chunk_slices, padding, chunk_shape = _chunk_geometry(
                    row["chunk_index"],
                    row["meta"]["shape"],
                    row["meta"]["chunks"],
                )

                chunk = _read_with_retry(row["array"], chunk_slices)

                dtype = np.dtype(row["meta"]["dtype"])
                chunk = chunk.astype(dtype, copy=False)

                if np.issubdtype(chunk.dtype, np.floating):
                    chunk = np.nan_to_num(chunk, nan=0, posinf=0, neginf=0, copy=False)

                if any(p > 0 for p in padding):
                    chunk = np.pad(
                        chunk,
                        pad_width=[(0, int(p)) for p in padding],
                        mode="constant",
                        constant_values=0,
                    )

                arrays.append(row["array"])
                array_shapes.append(row["meta"]["shape"])
                chunk_shapes.append(chunk_shape)
                dtypes.append(row["meta"]["dtype"])
                full_chunk_slices.append(chunk_slices)
                full_paddings.append(padding)
                chunks.append(chunk)

            yield pd.DataFrame(
                {
                    "array": arrays,
                    "array_shape": array_shapes,
                    "chunk_shape": chunk_shapes,
                    "dtype": dtypes,
                    "chunk_slices": full_chunk_slices,
                    "padding": full_paddings,
                    "chunk": chunks,
                }
            )

        return read_fn
    else:

        def read_fn() -> Iterable[pd.DataFrame]:
            arrays = []
            array_shapes = []
            chunk_shapes = []
            dtypes = []
            full_chunk_slices = []
            full_paddings = []

            for row in batch:
                chunk_slices, padding, chunk_shape = _chunk_geometry(
                    row["chunk_index"],
                    row["meta"]["shape"],
                    row["meta"]["chunks"],
                )
                arrays.append(row["array"])
                array_shapes.append(row["meta"]["shape"])
                chunk_shapes.append(chunk_shape)
                dtypes.append(row["meta"]["dtype"])
                full_chunk_slices.append(chunk_slices)
                full_paddings.append(padding)

            yield pd.DataFrame(
                {
                    "array": arrays,
                    "array_shape": array_shapes,
                    "chunk_shape": chunk_shapes,
                    "dtype": dtypes,
                    "chunk_slices": full_chunk_slices,
                    "padding": full_paddings,
                }
            )

        return read_fn


class ZarrV2Datasource(Datasource):
    """Reads chunks of one or more arrays from a Zarr v2 store.

    Each output row corresponds to one chunk of one selected array. With
    ``materialize=True`` (the default) rows include the chunk's data; with
    ``materialize=False`` rows hold only the chunk's descriptor (array name,
    slice bounds, padding, dtype). See :func:`ray.data.read_zarr` for the
    public API and full argument documentation.
    """

    def __init__(
        self,
        path: str,
        filesystem: AbstractFileSystem | None = None,
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False,
        materialize: bool = True,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")
        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.materialize = materialize
        self.root = None

        if chunk_shape:
            for val in chunk_shape:
                if val <= 0 or not isinstance(val, int):
                    raise ValueError("chunk shape must only contain positive integers")

        self.paths = [str(path)]
        self.chunk_shape: tuple[int, ...] | None = (
            tuple(chunk_shape) if chunk_shape is not None else None
        )
        self._selected_arrays = self._load_metadata(array_paths, filesystem)
        self._grid_shape_dict = self._gen_grid_shape()

        if self.materialize:
            self.root = self._zarr_root_init(filesystem)

    def _zarr_root_init(self, filesystem: AbstractFileSystem | None = None) -> ZarrRoot:
        """Open the Zarr store and return its root handle for materialization.

        Only called when ``materialize=True``. ``zarr`` is imported lazily here
        so descriptor-only usage doesn't need it. ``zarr.open`` returns an
        ``Array`` when the store's root is itself an array (``.zarray`` at
        root) and a ``Group`` when the root is a group (``.zgroup`` at root);
        the read-task dispatch handles both via the empty-string convention
        in ``self._selected_arrays``.
        """
        import zarr

        if filesystem is None:
            filesystem, _ = _resolve_store(self.paths[0], filesystem)

        mapper_path = _strip_protocol(self.paths[0], filesystem)
        mapper = filesystem.get_mapper(mapper_path)
        return zarr.open(mapper, mode="r")

    def _load_metadata(self, array_paths, filesystem) -> dict[str, ZarrArrayMeta]:
        """Discover and load ``.zarray`` metadata for the selected arrays.

        Discovery prefers consolidated ``.zmetadata`` when it exists. If the
        store has no ``.zmetadata``, the datasource falls back to reading each
        requested array's ``.zarray`` directly (when ``array_paths`` is given)
        or to a recursive scan (when ``allow_full_metadata_scan`` is set).
        If ``array_paths`` is given, the discovered set is filtered down to it;
        any requested paths that aren't present in the store raise a
        ``ValueError`` listing what is available.
        """
        fs, store_path = _resolve_store(self.paths[0], filesystem)

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
        else:
            raise ValueError(
                "No array_paths were provided and this Zarr store does not "
                "contain .zmetadata. Pass array_paths=[...] or set "
                "allow_full_metadata_scan=True."
            )

        if array_paths:
            # Normalize user-supplied paths for the filter step: strip slashes
            # and treat "." as the root ("").
            requested: set[str] = set()
            for p in array_paths:
                normalized = p.strip("/")
                if normalized == ".":
                    normalized = ""
                requested.add(normalized)

            missing = sorted(requested - all_arrays.keys())
            if missing:
                raise ValueError(
                    f"Array(s) not found: {', '.join(repr(m) for m in missing)}. "
                    f"Available: {', '.join(repr(a) for a in sorted(all_arrays))}"
                )
            all_arrays = {k: v for k, v in all_arrays.items() if k in requested}

        return all_arrays

    def _gen_grid_shape(self) -> dict[str, ZarrGridData]:
        """Compute per-array chunk grid shapes from array metadata.

        This applies any user-provided chunk-shape override, validates that the
        chunk rank matches the array rank, and records the number of chunks
        needed along each dimension for downstream read task generation.
        """
        grid_shape_dict: dict[str, ZarrGridData] = {}
        for array, meta in self._selected_arrays.items():
            shape = meta["shape"]
            chunk_shape = meta["chunks"]
            if self.chunk_shape:
                chunk_shape = self.chunk_shape
                meta["chunks"] = chunk_shape

            if len(shape) != len(chunk_shape):
                raise ValueError(
                    f"chunk shape must have same dimension length as the array: {array}"
                )

            grid_shape = tuple(
                math.ceil(size / chunk) for size, chunk in zip(shape, chunk_shape)
            )

            grid_shape_dict[array] = {"meta": meta, "grid_shape": grid_shape}
        return grid_shape_dict

    def _row_size_bytes(self, array_name: str, meta: ZarrArrayMeta) -> int:
        """Approximate in-memory size of a single output row from this datasource."""
        if self.materialize:
            return prod(meta["chunks"]) * np.dtype(meta["dtype"]).itemsize
        return _descriptor_row_size_bytes(array_name, meta)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate the dataset's total in-memory size as ``sum(num_chunks * row_size)``."""
        total = 0
        for array_name, data in self._grid_shape_dict.items():
            num_chunks = prod(data["grid_shape"])
            total += num_chunks * self._row_size_bytes(array_name, data["meta"])
        return total

    def _estimate_batch_mem_size(self, batch: List[ZarrChunkRow]) -> int:
        """Sum the per-row size estimate across one read task's batch."""
        return sum(self._row_size_bytes(row["array"], row["meta"]) for row in batch)

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Flatten the chunk grid into ``parallelism`` batches and wrap each in a ReadTask.

        Chunks across all selected arrays are concatenated and split into batches
        of size ``ceil(num_chunks / parallelism)``. Each batch becomes one
        ``ReadTask`` whose callable yields a single ``pd.DataFrame``.
        """
        read_tasks: List[ReadTask] = []
        batch: list[ZarrChunkRow] = []

        num_chunks = sum(
            prod(value["grid_shape"]) for _, value in self._grid_shape_dict.items()
        )
        parallelism = min(parallelism, num_chunks) if num_chunks > 0 else 1
        batch_size = math.ceil(num_chunks / parallelism)

        for array, data in self._grid_shape_dict.items():
            for chunk_index in product(*(range(n) for n in data["grid_shape"])):

                batch.append(
                    {"array": array, "meta": data["meta"], "chunk_index": chunk_index}
                )

                if len(batch) >= batch_size:
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
                    batch = []
        if batch:
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
