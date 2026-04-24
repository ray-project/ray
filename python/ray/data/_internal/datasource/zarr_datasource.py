"""Zarr datasource for Ray Data."""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable
from itertools import product
from math import prod
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlsplit
import sys

import numpy as np
import pandas as pd
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

from obstore.fsspec import FsspecStore
from obstore.store import AzureStore
import fsspec
import fsspec.core


def _make_azure_fs(url: str) -> tuple[FsspecStore, AzureStore]:
    """Create an authenticated Azure FsspecStore and AzureStore from a URL.
    Args:
        url: The Azure Blob Storage URL (abfs://...).
    Returns:
        A tuple of (FsspecStore, AzureStore).
    """
    store = AzureStore.from_url(
        url=url,
    )
    return FsspecStore(
        config=store.config,
        protocol="abfs",
    ), store

def _resolve_store(path: str) -> tuple[Any, str]:
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
        fs, _ = _make_azure_fs(path)
        return fs, path.rstrip("/")

    # Local
    if parsed.scheme in ("", "file"):
        local = path if not parsed.scheme else parsed.path
        root = str(Path(local).resolve())
        return fsspec.filesystem("file"), root

    # Public S3
    if parsed.scheme == "s3":
        fs = fsspec.filesystem("s3", anon=True)
        return fs, path.rstrip("/")

    # Generic fallback
    fs, root = fsspec.core.url_to_fs(path)
    return fs, root.rstrip("/")

def _create_read_fn(
    batch: list[dict[str, object]],
) -> Callable[[], Iterable[pd.DataFrame]]:

    def read_fn() -> Iterable[pd.DataFrame]:
        arrays = []
        array_shapes = []
        chunk_shapes = []
        dtypes = []
        full_chunk_slices = []
        
        for row in batch:
            chunk_slices = []
            for i, size, chunk in zip(row['chunk_index'], row['meta']['shape'], row['meta']['chunks']):
                start = i * chunk
                stop = min((i + 1) * chunk, size)
                chunk_slices.append((start, stop))
            full_chunk_slices.append(chunk_slices)
            arrays.append(row['array'])
            array_shapes.append(row['meta']['shape'])
            chunk_shapes.append(row['meta']['chunks'])
            dtypes.append(row['meta']['dtype'])
        
        yield pd.DataFrame({
            "array": arrays,
            "array_shape": array_shapes,
            "chunk_shape": chunk_shapes,
            "dtype": dtypes,
            "chunk_slices": full_chunk_slices
        })
                

    return read_fn

class ZarrV2Datasource(Datasource):
    """Datasource for reading Zarr arrays."""

    def __init__(
        self,
        path: str,
        chunk_shape: List[int] | None = None,
        array_paths: List[str] | None = None,
    ) -> None:
        super().__init__()
        _check_import(self, module="zarr", package="zarr")
        
        if chunk_shape:
            for val in chunk_shape:
                if val <= 0 or not isinstance(val, int):
                    raise ValueError("chunk shape must only contain positive integerse")

        self.paths = [str(path)]
        self.chunk_shape = tuple(chunk_shape) if chunk_shape else chunk_shape
        self._metadata = self._load_consolidated_metadata()
        self._selected_arrays = self._select_array_metadata(array_paths)
        self._grid_shape_dict = self._gen_grid_shape()

    def _load_consolidated_metadata(self) -> dict:
        fs, store_path = _resolve_store(self.paths[0])
        if store_path:
            meta_path = f"{store_path.rstrip('/')}/.zmetadata"
        else:
            meta_path = ".zmetadata"

        with fs.open(meta_path, "rb") as f:
            consolidated = json.load(f)

        if "metadata" not in consolidated:
            raise ValueError("Missing 'metadata' in consolidated .zmetadata file")

        return consolidated["metadata"]

    def _select_array_metadata(
        self,
        array_paths: Iterable[str] | None
    ) -> dict[str, dict[str, object]]:
        """Pick a single array's metadata from the consolidated metadata."""
        arrays: dict[str, dict[str, object]] = {}
        for key, value in self._metadata.items():
            if not key.endswith(".zarray"):
                continue
            elif key == ".zarray":
                arrays[""] = value
            else:
                arrays[key[: -len("/.zarray")]] = value

        if not arrays:
            raise ValueError("No arrays found in consolidated metadata.")
        
        if array_paths is None:
            requested_paths = [p.strip("/") for p in arrays.keys()]
            normalized_paths = [p if p != "." else "" for p in requested_paths]
            selected_paths = normalized_paths
        else:
            requested_paths = [p.strip("/") for p in array_paths]
            normalized_paths = [p if p != "." else "" for p in requested_paths]
            missing = [p for p in normalized_paths if p not in arrays]
            if missing:
                available = ", ".join(sorted(p or "." for p in arrays.keys()))
                raise ValueError(
                    f"Array(s) not found: {', '.join(missing)}. Available: {available}"
                )
            selected_paths = normalized_paths
        
        return {path: arrays[path] for path in selected_paths}
    
    def _gen_grid_shape(self):
        grid_shape_dict = {}
        for array, meta in self._selected_arrays.items():
            shape = tuple(meta['shape'])
            chunk_shape = tuple(meta['chunks'])
            if self.chunk_shape:
                chunk_shape = self.chunk_shape
                meta['chunks'] = chunk_shape
            
            if len(shape) != len(chunk_shape):
                raise ValueError(f"chunk shape must have same dimension length as the array: {array}")
            
            grid_shape = tuple(
                math.ceil(size / chunk)
                for size, chunk in zip(shape, chunk_shape)
            )
            
            grid_shape_dict[array] = {"meta": meta, "grid_shape": grid_shape}
        return grid_shape_dict
        

    def estimate_inmemory_data_size(self) -> Optional[int]:
        full_bytes_estimate = 0
        for _, meta in self._selected_arrays.items():
            shape = tuple(meta['shape'])
            dtype = np.dtype(meta["dtype"])
            bytes_estimate = int(prod(shape) * dtype.itemsize)
            full_bytes_estimate += bytes_estimate
        
        return full_bytes_estimate
    
    def _sizeof_batch(self, obj, seen=None):
        if seen is None:
            seen = set()

        obj_id = id(obj)
        if obj_id in seen:
            return 0
        seen.add(obj_id)

        size = sys.getsizeof(obj)

        if isinstance(obj, dict):
            size += sum(self._sizeof_batch(k, seen) + self._sizeof_batch(v, seen) for k, v in obj.items())
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
        batch: list[dict[str, object]] = []
        
        num_chunks = sum(prod(value['grid_shape']) for _, value in self._grid_shape_dict.items())
        parallelism = min(parallelism, num_chunks) if num_chunks > 0 else 1
        batch_size = math.ceil(num_chunks / parallelism)
        
        for array, data in self._grid_shape_dict.items():
            for chunk_index in product(*(range(n) for n in data['grid_shape'])):
                
                batch.append({"array": array, "meta": data['meta'], "chunk_index": chunk_index})
                
                if len(batch) >= batch_size:
                    read_tasks.append(
                        ReadTask(
                            _create_read_fn(
                                batch
                            ),
                            BlockMetadata(
                                num_rows = len(batch),
                                size_bytes = self._sizeof_batch(batch),
                                input_files = [self.paths[0]],
                                exec_stats = None
                            )
                        )
                    )
                    batch = []
        if batch:
            read_tasks.append(
                ReadTask(
                    _create_read_fn(
                        batch
                    ),
                    BlockMetadata(
                        num_rows = len(batch),
                        size_bytes = self._sizeof_batch(batch),
                        input_files = [self.paths[0]],
                        exec_stats = None
                    )
                )
            )
        return read_tasks
        
                
        
    
    
