"""Zarr datasource for Ray Data."""

from __future__ import annotations

import json
import math
import sys
from collections.abc import Callable, Iterable
from itertools import product
from math import prod
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional, TypedDict, cast
from urllib.parse import urlsplit

import fsspec
import fsspec.core
import numpy as np
import pandas as pd
from fsspec.spec import AbstractFileSystem

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from zarr.hierarchy import Group as ZarrGroup

    from ray.data.context import DataContext

REQUIRED_ZARRAY_KEYS = ("shape", "chunks", "dtype")


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


def _zarr_array_meta_from_json(raw_meta: dict[str, Any]) -> ZarrArrayMeta:
    return {
        "shape": tuple(int(x) for x in raw_meta["shape"]),
        "chunks": tuple(int(x) for x in raw_meta["chunks"]),
        "dtype": str(raw_meta["dtype"]),
    }


def _resolve_store(
    path: str, filesystem: AbstractFileSystem | None = None
) -> tuple[AbstractFileSystem, str]:

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
    if hasattr(filesystem, "_strip_protocol"):
        return filesystem._strip_protocol(path)

    parsed = urlsplit(path)
    if parsed.scheme:
        return path.removeprefix(f"{parsed.scheme}://")
    return path


def _create_read_fn(
    batch: list[ZarrChunkRow], root: ZarrGroup | None = None
) -> Callable[[], Iterable[pd.DataFrame]]:
    """Build a read-task callable for a batch of Zarr chunk descriptors.

    If ``root`` is provided, the returned callable reads and materializes each
    chunk's data into a ``"chunk"`` column. Otherwise, it returns metadata-only
    rows that describe the chunk bounds, shape, dtype, and edge padding needed
    to reconstruct truncated boundary chunks.
    """
    if root:

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
                            print(
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
                chunk_slices = []
                padding = []
                chunk_shape = list(row["meta"]["chunks"])
                for dim, (i, size, chunk) in enumerate(
                    zip(row["chunk_index"], row["meta"]["shape"], row["meta"]["chunks"])
                ):
                    start = i * chunk
                    stop = min((i + 1) * chunk, size)
                    chunk_slices.append((start, stop))

                    if start + chunk > size:
                        padding_slice = start + chunk - size
                        chunk_shape[dim] = stop - start
                    else:
                        padding_slice = 0
                    padding.append(padding_slice)

                chunk = _read_with_retry(row["array"], list(chunk_slices))

                dtype = np.dtype(row["meta"]["dtype"])
                chunk = chunk.astype(dtype, copy=False)

                if np.issubdtype(chunk.dtype, np.floating):
                    chunk = np.nan_to_num(
                        chunk,
                        nan=0,
                        posinf=0,
                        neginf=0,
                        copy=False,
                    )

                pad_width = [(0, int(pad_amount)) for pad_amount in list(padding)]
                if any(pad_amount > 0 for pad_amount in list(padding)):
                    chunk = np.pad(
                        chunk,
                        pad_width=pad_width,
                        mode="constant",
                        constant_values=0,
                    )

                full_chunk_slices.append(chunk_slices)
                arrays.append(row["array"])
                array_shapes.append(row["meta"]["shape"])
                chunk_shapes.append(tuple(chunk_shape))
                dtypes.append(row["meta"]["dtype"])
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
                chunk_slices = []
                padding = []
                chunk_shape = list(row["meta"]["chunks"])
                for dim, (i, size, chunk) in enumerate(
                    zip(row["chunk_index"], row["meta"]["shape"], row["meta"]["chunks"])
                ):
                    start = i * chunk
                    stop = min((i + 1) * chunk, size)
                    chunk_slices.append((start, stop))

                    if start + chunk > size:
                        padding_slice = start + chunk - size
                        chunk_shape[dim] = stop - start
                    else:
                        padding_slice = 0
                    padding.append(padding_slice)
                full_chunk_slices.append(chunk_slices)
                arrays.append(row["array"])
                array_shapes.append(row["meta"]["shape"])
                chunk_shapes.append(tuple(chunk_shape))
                dtypes.append(row["meta"]["dtype"])
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
    """Datasource for reading Zarr arrays."""

    def __init__(
        self,
        path: str,
        filesystem: AbstractFileSystem | None = None,
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False,
        materialize: bool = False,
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
        self._selected_arrays = self._load_array_metadata(array_paths, filesystem)
        self._grid_shape_dict = self._gen_grid_shape()

        if self.materialize:
            self.root = self._zarr_root_init(filesystem)

    def _zarr_root_init(
        self, filesystem: AbstractFileSystem | None = None
    ) -> ZarrGroup:
        import zarr

        if filesystem is None:
            filesystem, _ = _resolve_store(self.paths[0], filesystem)

        mapper_path = _strip_protocol(self.paths[0], filesystem)
        mapper = filesystem.get_mapper(mapper_path)

        root = zarr.open_group(mapper, mode="r")
        return root

    def _load_array_metadata(self, array_paths, filesystem) -> dict[str, ZarrArrayMeta]:
        """Load validated ``.zarray`` metadata for the arrays selected by the user.

        Metadata discovery prefers explicit ``array_paths`` first, then
        consolidated ``.zmetadata``, and finally an optional full-store scan when
        ``allow_full_metadata_scan`` is enabled. Every discovered ``.zarray``
        entry must contain the required shape, chunk, and dtype fields.
        """
        fs, store_path = _resolve_store(self.paths[0], filesystem)
        array_metadata: dict[str, ZarrArrayMeta] = {}

        # 1) if the user provided array paths
        if array_paths:
            print("array_paths provided. Collecting .zarray file metadata")

            for array in array_paths:
                normalized_array = array.strip("/")

                if normalized_array != "":
                    full_path = f"{store_path.rstrip('/')}/{normalized_array}/.zarray"
                else:
                    full_path = f"{store_path.rstrip('/')}/.zarray"

                try:
                    with fs.open(full_path, "r") as f:
                        data = json.load(f)
                    raw_meta = cast(dict[str, Any], data)

                    missing_keys = [
                        key for key in REQUIRED_ZARRAY_KEYS if key not in raw_meta
                    ]

                    if missing_keys:
                        raise ValueError(
                            f"Invalid .zarray metadata for array path '{array}'. "
                            f"Missing required key(s): {missing_keys}. "
                            f"Expected keys: {list(REQUIRED_ZARRAY_KEYS)}. "
                            f".zarray path: {full_path}"
                        )

                    meta: ZarrArrayMeta = _zarr_array_meta_from_json(raw_meta)
                    array_metadata[normalized_array] = meta
                except FileNotFoundError as e:
                    raise ValueError(
                        f"{array} is not a valid array path in the Zarr store. "
                        f"Could not find .zarray file at: {full_path}"
                    ) from e

        else:
            z_meta_path = f"{store_path.rstrip('/')}/.zmetadata"

            # 2) if the user did not provide array paths, but .zmetadata exists
            if fs.exists(z_meta_path):
                print("No array_paths provided. Loading .zmetadata file")
                with fs.open(z_meta_path, "rb") as f:
                    consolidated = json.load(f)
                metadata = consolidated["metadata"]

                for key, value in metadata.items():
                    if not key.endswith(".zarray"):
                        continue

                    raw_meta = cast(dict[str, Any], value)

                    missing_keys = [
                        key for key in REQUIRED_ZARRAY_KEYS if key not in raw_meta
                    ]

                    if missing_keys:
                        raise ValueError(
                            f"Invalid .zarray metadata for array path '{array}'. "
                            f"Missing required key(s): {missing_keys}. "
                            f"Expected keys: {list(REQUIRED_ZARRAY_KEYS)}. "
                            f".zarray path: {full_path}"
                        )

                    meta: ZarrArrayMeta = _zarr_array_meta_from_json(raw_meta)

                    if key == ".zarray":
                        array_metadata[""] = meta
                    else:
                        array_metadata[key[: -len("/.zarray")]] = meta

            # 3) if the user did not provide array paths, and .zmetadata does not exist
            else:
                # since this scan can be potentially very time consuming, it will only run if the user explicitly allowed for it
                if self.allow_full_metadata_scan:
                    print(
                        "No array_paths provided & no .zmetadata found. Executing full scan of zarr store metadata"
                    )
                    for dirpath, _, filenames in fs.walk(store_path):
                        for filename in filenames:
                            if filename == ".zarray":

                                if dirpath.rstrip("/") == store_path.rstrip("/"):
                                    array = ""
                                else:
                                    array = dirpath.removeprefix(
                                        store_path.rstrip("/") + "/"
                                    )
                                array_path = f"{dirpath.rstrip('/')}/.zarray"

                                try:
                                    with fs.open(array_path, "r") as f:
                                        data = json.load(f)
                                        raw_meta = cast(dict[str, Any], data)

                                        missing_keys = [
                                            key
                                            for key in REQUIRED_ZARRAY_KEYS
                                            if key not in raw_meta
                                        ]

                                        if missing_keys:
                                            raise ValueError(
                                                f"Invalid .zarray metadata for array path '{array}'. "
                                                f"Missing required key(s): {missing_keys}. "
                                                f"Expected keys: {list(REQUIRED_ZARRAY_KEYS)}. "
                                                f".zarray path: {full_path}"
                                            )

                                        meta: ZarrArrayMeta = (
                                            _zarr_array_meta_from_json(raw_meta)
                                        )
                                        array_metadata[array.strip("/")] = meta
                                except FileNotFoundError:
                                    continue
                else:
                    raise ValueError(
                        "No array_paths were provided and this Zarr store does not contain .zmetadata. "
                        "Pass array_paths=[...] or set allow_full_metadata_scan=True."
                    )
        return array_metadata

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

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.materialize:
            arrays = []
            array_shapes = []
            chunk_shapes = []
            dtypes = []
            full_chunk_slices = []
            full_paddings = []

            for array, data in self._grid_shape_dict.items():
                meta = data["meta"]
                for chunk_index in product(*(range(n) for n in data["grid_shape"])):
                    chunk_slices = []
                    padding = []
                    chunk_shape = list(meta["chunks"])

                    for dim, (i, size, chunk) in enumerate(
                        zip(chunk_index, meta["shape"], meta["chunks"])
                    ):
                        start = i * chunk
                        stop = min((i + 1) * chunk, size)
                        chunk_slices.append((start, stop))

                        if start + chunk > size:
                            padding_slice = start + chunk - size
                            chunk_shape[dim] = stop - start
                        else:
                            padding_slice = 0
                        padding.append(padding_slice)

                    arrays.append(array)
                    array_shapes.append(meta["shape"])
                    chunk_shapes.append(tuple(chunk_shape))
                    dtypes.append(meta["dtype"])
                    full_chunk_slices.append(chunk_slices)
                    full_paddings.append(padding)

            return self._sizeof_batch(
                {
                    "array": arrays,
                    "array_shape": array_shapes,
                    "chunk_shape": chunk_shapes,
                    "dtype": dtypes,
                    "chunk_slices": full_chunk_slices,
                    "padding": full_paddings,
                }
            )
        else:
            total_size_bytes = 0

            for array, data in self._grid_shape_dict.items():
                meta = data["meta"]

                shape = meta["shape"]
                dtype = meta["dtype"]

                num_elements = prod(shape)
                dtype_size_bytes = np.dtype(dtype).itemsize

                total_size_bytes += num_elements * dtype_size_bytes

            return total_size_bytes

    def _estimate_batch_mem_size(self, batch: List[ZarrChunkRow]) -> int:
        batch_size_bytes = 0

        for zarr_chunk_row in batch:
            meta = zarr_chunk_row["meta"]

            chunks = meta["chunks"]
            dtype = meta["dtype"]

            num_elements = prod(chunks)
            dtype_size_bytes = np.dtype(dtype).itemsize

            batch_size_bytes += num_elements * dtype_size_bytes

        return batch_size_bytes

    def _sizeof_batch(self, obj, seen=None):
        if seen is None:
            seen = set()

        obj_id = id(obj)
        if obj_id in seen:
            return 0
        seen.add(obj_id)

        size = sys.getsizeof(obj)

        if isinstance(obj, dict):
            size += sum(
                self._sizeof_batch(k, seen) + self._sizeof_batch(v, seen)
                for k, v in obj.items()
            )
        elif isinstance(obj, (list, tuple, set, frozenset)):
            size += sum(self._sizeof_batch(x, seen) for x in obj)

        return size

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:

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
                    batch_mem_size = None
                    if self.materialize:
                        batch_mem_size = self._estimate_batch_mem_size(batch)
                    else:
                        batch_mem_size = self._sizeof_batch(batch)

                    read_tasks.append(
                        ReadTask(
                            _create_read_fn(batch, self.root),
                            BlockMetadata(
                                num_rows=len(batch),
                                size_bytes=batch_mem_size,
                                input_files=(self.paths[0],),
                                exec_stats=None,
                            ),
                        )
                    )
                    batch = []
        if batch:
            batch_mem_size = None
            if self.materialize:
                batch_mem_size = self._estimate_batch_mem_size(batch)
            else:
                batch_mem_size = self._sizeof_batch(batch)

            read_tasks.append(
                ReadTask(
                    _create_read_fn(batch, self.root),
                    BlockMetadata(
                        num_rows=len(batch),
                        size_bytes=batch_mem_size,
                        input_files=(self.paths[0],),
                        exec_stats=None,
                    ),
                )
            )
        return read_tasks
