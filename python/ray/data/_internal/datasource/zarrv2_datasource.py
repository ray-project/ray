from __future__ import annotations

import json
import logging
import math
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, List, Optional, TypedDict

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


class ZarrArrayMeta(TypedDict):
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: str


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


def _axis0_chunk_slices(shape0: int, chunk_size: int) -> list[tuple[int, int]]:
    """Tile axis 0 into ``(start, stop)`` slice pairs of length up to ``chunk_size``.

    The final slice may be shorter than ``chunk_size`` if ``shape0`` is not a
    multiple of it. No padding is applied — callers receive the actual shapes.
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    return [
        (start, min(start + chunk_size, shape0))
        for start in range(0, shape0, chunk_size)
    ]


def _resolve_store(
    path: str, filesystem: AbstractFileSystem | None = None
) -> tuple[AbstractFileSystem, str]:
    """Resolve a user-supplied store path to an ``(fsspec_filesystem, path)`` pair.

    If the caller already supplied an fsspec filesystem, use it as-is.
    Otherwise delegate URL resolution to Ray Data's standard
    :func:`_resolve_paths_and_filesystem` helper (the same one every other
    ``read_*`` API uses) and wrap the resulting ``pyarrow.fs.FileSystem``
    into fsspec via :class:`fsspec.implementations.arrow.ArrowFSWrapper`
    since Zarr's storage layer speaks fsspec. This gives free support for
    Ray Data URL conventions like ``s3://anonymous@<bucket>/...``.
    """
    if filesystem is not None:
        return filesystem, path.rstrip("/")

    from ray.data.datasource.path_util import _resolve_paths_and_filesystem

    paths, pa_fs = _resolve_paths_and_filesystem([path])
    return _to_fsspec(pa_fs), paths[0].rstrip("/")


def _to_fsspec(
    filesystem: pyarrow.fs.FileSystem | AbstractFileSystem | None,
) -> AbstractFileSystem | None:
    """Normalize a user-supplied filesystem to an fsspec ``AbstractFileSystem``.

    Ray Data normally uses pyarrow.fs.FileSystem, but the zarr python library
    requires fsspec.spec.AbstractFileSystem. This function wraps the pyarrow
    filesystem in an fsspec filesystem if needed.
    """
    if filesystem is None:
        return None
    if isinstance(filesystem, AbstractFileSystem):
        return filesystem

    from pyarrow.fs import FileSystem

    if isinstance(filesystem, FileSystem):
        from fsspec.implementations.arrow import ArrowFSWrapper

        return ArrowFSWrapper(filesystem)

    raise TypeError(
        f"filesystem must be pyarrow.fs.FileSystem or "
        f"fsspec.spec.AbstractFileSystem, got {type(filesystem).__name__}"
    )


def _read_array_slice(
    root: ZarrRoot,
    array_name: str,
    start: int,
    stop: int,
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> np.ndarray:
    """Read ``array[start:stop, ...]`` from a Zarr root with network-retry."""
    import time

    last_error: Optional[BaseException] = None
    retry_keywords = (
        "connection reset",
        "timeout",
        "connection refused",
        "network",
        "socket",
        "http error",
    )
    for attempt in range(max_retries):
        try:
            # Resolve the array inside the retry loop: on remote stores
            # ``root[array_name]`` reads the ``.zarray`` metadata file and
            # can fail transiently the same way the data read can.
            arr = root if array_name == "" else root[array_name]
            return arr[start:stop, ...]
        except Exception as e:
            last_error = e
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in retry_keywords):
                delay = base_delay * (2**attempt)
                logger.warning(
                    "Network error reading array=%s [%s:%s], "
                    "attempt %s/%s, retrying in %.1fs: %s",
                    array_name,
                    start,
                    stop,
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
            else:
                raise
    raise RuntimeError(
        f"Failed to read array={array_name!r} [{start}:{stop}] "
        f"after {max_retries} attempts"
    ) from last_error


def _create_read_fn(
    slices: list[tuple[int, int]],
    array_names: list[str],
    root: ZarrRoot,
) -> Callable[[], Iterable[pd.DataFrame]]:
    """Build a read-task callable that materializes one row per axis-0 slice.

    Each output row has one column per array in ``array_names``. The column
    value is the array's ``[start:stop, ...]`` slice (an ndarray). Edge
    slices at the trailing boundary may be shorter than the others.
    """

    def read_fn() -> Iterable[pd.DataFrame]:
        columns: dict[str, list] = {name: [] for name in array_names}
        for start, stop in slices:
            for name in array_names:
                columns[name].append(_read_array_slice(root, name, start, stop))
        yield pd.DataFrame(columns)

    return read_fn


class ZarrV2Datasource(Datasource):
    """Reads chunks of axis-0-aligned arrays from a Zarr v2 store.

    Each output row corresponds to one slice along axis 0; the row carries one
    column per selected array, containing that array's ``[start:stop, ...]``
    slice. All selected arrays must share ``shape[0]``. See
    :func:`ray.data.read_zarr` for the public API.
    """

    def __init__(
        self,
        path: str,
        filesystem: pyarrow.fs.FileSystem | AbstractFileSystem | None = None,
        chunk_size: Optional[int] = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")

        self.allow_full_metadata_scan = allow_full_metadata_scan
        self.paths = [str(path)]
        self._filesystem = _to_fsspec(filesystem)

        self._selected_arrays = self._load_metadata(array_paths)
        if not self._selected_arrays:
            raise ValueError(
                f"No arrays discovered in Zarr store at {self.paths[0]!r}."
            )

        self._shape0 = self._validate_axis0_alignment()

        if chunk_size is None:
            chunk_size = min(
                meta["chunks"][0] for meta in self._selected_arrays.values()
            )
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError(
                f"chunk_size must be a positive integer, got {chunk_size!r}"
            )
        self.chunk_size = chunk_size
        self._slices = _axis0_chunk_slices(self._shape0, self.chunk_size)

        # Lazy zarr import: only needed once we read.
        import zarr

        fs, store_path = _resolve_store(self.paths[0], self._filesystem)
        self.root = zarr.open(fs.get_mapper(store_path), mode="r")

    def _validate_axis0_alignment(self) -> int:
        """Ensure all selected arrays agree on ``shape[0]``; return that length."""
        # Reject 0-D (scalar) arrays upfront — they appear in real stores
        # (e.g., per-experiment hyperparameters in ERA5/CMIP6) but have no
        # axis 0 to align on. Indexing ``meta["shape"][0]`` on them would
        # raise IndexError with no useful context.
        scalars = [
            name for name, meta in self._selected_arrays.items() if not meta["shape"]
        ]
        if scalars:
            raise ValueError(
                f"Cannot read 0-dimensional (scalar) arrays via read_zarr; "
                f"this datasource requires alignment along axis 0. Offending "
                f"array(s): {scalars!r}. Use array_paths=[...] to exclude them."
            )

        shape0_by_array = {
            name: meta["shape"][0] for name, meta in self._selected_arrays.items()
        }
        sizes = set(shape0_by_array.values())
        if len(sizes) > 1:
            raise ValueError(
                "Arrays in a single read_zarr call must share axis 0 (their "
                f"first dimension). Got: {shape0_by_array!r}. Use array_paths="
                "[...] to select a compatible subset."
            )
        return sizes.pop()

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
        fs, store_path = _resolve_store(self.paths[0], self._filesystem)

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

    def _row_bytes(self, slice_len: int) -> int:
        """In-memory bytes for one output row carrying ``slice_len`` along axis 0."""
        total = 0
        for meta in self._selected_arrays.values():
            non_axis0_numel = (
                math.prod(meta["shape"][1:]) if len(meta["shape"]) > 1 else 1
            )
            total += slice_len * non_axis0_numel * np.dtype(meta["dtype"]).itemsize
        return total

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Total bytes = sum of (full array sizes) across all selected arrays."""
        return sum(
            math.prod(meta["shape"]) * np.dtype(meta["dtype"]).itemsize
            for meta in self._selected_arrays.values()
        )

    def _estimate_batch_mem_size(self, batch: list[tuple[int, int]]) -> int:
        """Sum per-row sizes across one read task's batch."""
        return sum(self._row_bytes(stop - start) for start, stop in batch)

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """Split the axis-0 chunk list into ``parallelism`` batches and wrap each in a ReadTask.

        Each ``ReadTask``'s callable yields a single ``pd.DataFrame`` whose
        rows are axis-0 slices and whose columns are the selected arrays.
        """
        read_tasks: List[ReadTask] = []
        array_names = list(self._selected_arrays)

        n_rows = len(self._slices)
        if n_rows == 0:
            return read_tasks
        actual_parallelism = min(parallelism, n_rows)
        batch_size = math.ceil(n_rows / actual_parallelism)

        for start in range(0, n_rows, batch_size):
            batch = self._slices[start : start + batch_size]
            read_tasks.append(
                ReadTask(
                    _create_read_fn(batch, array_names, self.root),
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
