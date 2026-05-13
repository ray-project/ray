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
import pandas as pd

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from ray.data.context import DataContext


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


def _make_azure_fs(url: str, obj: Any) -> tuple[Any, Any]:
    """Create an authenticated Azure FsspecStore and AzureStore from a URL.

    Args:
        url: The Azure URL to parse.
        obj: The original filesystem/storage options object used to construct
            the authenticated Azure store.

    Returns:
        A tuple containing the authenticated fsspec filesystem and Azure store.
    """
    _check_import(obj, module="obstore", package="obstore")

    from obstore.fsspec import FsspecStore
    from obstore.store import AzureStore

    store = AzureStore.from_url(
        url=url,
    )
    return (
        FsspecStore(
            config=store.config,
            protocol="abfs",
        ),
        store,
    )


def _resolve_store(path: str, obj: Any) -> tuple[Any, str]:
    """
    Return a filesystem-like object and a rooted path prefix for the Zarr store.
    Works for:
    - Azure abfs://...
    - local paths
    - s3://... (including public anonymous buckets)
    - generic fsspec-supported URLs
    """
    parsed = urlsplit(path)

    # Azure
    if parsed.scheme in ("abfs", "abfss", "az"):
        fs, _ = _make_azure_fs(path, obj)
        return fs, path.rstrip("/")

    # Local
    if parsed.scheme in ("", "file"):
        local = path if not parsed.scheme else parsed.path
        root = str(Path(local).resolve())
        return fsspec.filesystem("file"), root

    # Public S3
    if parsed.scheme == "s3":
        fs = fsspec.filesystem("s3")
        return fs, path.rstrip("/")

    # Generic fallback
    fs, root = fsspec.core.url_to_fs(path)
    return fs, root.rstrip("/")


def _create_read_fn(
    batch: list[ZarrChunkRow],
) -> Callable[[], Iterable[pd.DataFrame]]:
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
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
        allow_full_metadata_scan: bool = False
    ) -> None:
        super().__init__()
        self.allow_full_metadata_scan = allow_full_metadata_scan

        if chunk_shape:
            for val in chunk_shape:
                if val <= 0 or not isinstance(val, int):
                    raise ValueError("chunk shape must only contain positive integers")

        self.paths = [str(path)]
        self.chunk_shape: tuple[int, ...] | None = (
            tuple(chunk_shape) if chunk_shape is not None else None
        )
        self._selected_arrays = self._load_array_metadata(array_paths)
        self._grid_shape_dict = self._gen_grid_shape()
        
    def _load_array_metadata(self, array_paths) -> dict[str, ZarrArrayMeta]:
        fs, store_path = _resolve_store(self.paths[0], self)
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
                    meta: ZarrArrayMeta = {
                        "shape": tuple(int(x) for x in raw_meta["shape"]),
                        "chunks": tuple(int(x) for x in raw_meta["chunks"]),
                        "dtype": str(raw_meta["dtype"]),
                    }
                    array_metadata[normalized_array] = meta
                except FileNotFoundError as e:
                    raise ValueError(f"{array} is not a valid array path in the zarr store") from e
        
        else:
            z_meta_path = f"{store_path.rstrip('/')}/.zmetadata"
            
            # 2) if the user did not provide array paths, but .zmetadata exists
            if fs.exists(z_meta_path):
                print("No array_paths provided. Loading .zmetadata file")
                with fs.open(z_meta_path, 'rb') as f:
                    consolidated = json.load(f)
                metadata = consolidated['metadata']
                
                for key, value in metadata.items():
                    if not key.endswith(".zarray"):
                        continue
                    
                    raw_meta = cast(dict[str, Any], value)
                    meta: ZarrArrayMeta = {
                        "shape": tuple(int(x) for x in raw_meta["shape"]),
                        "chunks": tuple(int(x) for x in raw_meta["chunks"]),
                        "dtype": str(raw_meta["dtype"]),
                    }
                    
                    if key == ".zarray":
                        array_metadata[""] = meta
                    else:
                        array_metadata[key[: -len("/.zarray")]] = meta
            
            # 3) if the user did not provide array paths, and .zmetadata does not exist
            else:
                # since this scan can be potentially very time consuming, it will only run if the user explicitly allowed for it
                if self.allow_full_metadata_scan:
                    print("No array_paths provided & no .zmetadata found. Executing full scan of zarr store metadata")
                    for dirpath, _, filenames in fs.walk(store_path):
                        for filename in filenames:
                            if filename == ".zarray":
                                
                                if dirpath.rstrip("/") == store_path.rstrip("/"):
                                    array = ""
                                else:
                                    array = dirpath.removeprefix(store_path.rstrip("/") + "/")
                                array_path = f"{dirpath.rstrip('/')}/.zarray"
                                
                                try:
                                    with fs.open(array_path, 'r') as f:
                                        data = json.load(f)
                                        raw_meta = cast(dict[str, Any], data)
                                        meta: ZarrArrayMeta = {
                                            "shape": tuple(int(x) for x in raw_meta["shape"]),
                                            "chunks": tuple(int(x) for x in raw_meta["chunks"]),
                                            "dtype": str(raw_meta["dtype"]),
                                        }
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
                    read_tasks.append(
                        ReadTask(
                            _create_read_fn(batch),
                            BlockMetadata(
                                num_rows=len(batch),
                                size_bytes=self._sizeof_batch(batch),
                                input_files=(self.paths[0],),
                                exec_stats=None,
                            ),
                        )
                    )
                    batch = []
        if batch:
            read_tasks.append(
                ReadTask(
                    _create_read_fn(batch),
                    BlockMetadata(
                        num_rows=len(batch),
                        size_bytes=self._sizeof_batch(batch),
                        input_files=(self.paths[0],),
                        exec_stats=None,
                    ),
                )
            )
        return read_tasks
