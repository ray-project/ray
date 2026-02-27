"""Zarr datasource for Ray Data."""

import itertools
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs

import ray
from ray.data._internal.util import _check_import, _is_local_scheme
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    import zarr

    from ray.data.context import DataContext

_METADATA_FILENAMES = {".zarray", ".zgroup", ".zattrs", "zarr.json"}


class ZarrDatasource(Datasource):
    """Datasource for reading Zarr arrays."""

    def __init__(
        self,
        paths: Union[str, List[str]],
        filesystem: Optional["pafs.FileSystem"] = None,
    ):
        super().__init__()
        _check_import(self, module="zarr", package="zarr")

        if isinstance(paths, str):
            paths = [paths]

        self._supports_distributed_reads = not _is_local_scheme(paths)
        if not self._supports_distributed_reads and ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the Ray "
                "cluster can't access your local files. To fix this issue, store "
                "files in cloud storage or a distributed filesystem like NFS."
            )

        self._paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)

        # Read and cache metadata for all stores (similar to ParquetDatasource listing files in __init__)
        self._store_metadata = {}  # {store_path: (metadata_bytes, path_lookup, is_v3)}
        for store_path in self._paths:
            file_infos = self._filesystem.get_file_info(
                pafs.FileSelector(store_path, recursive=True)
            )

            metadata_bytes: Dict[str, bytes] = {}
            path_lookup: Dict[str, str] = {}

            for fi in file_infos:
                if fi.type != pafs.FileType.File:
                    continue

                rel_path = fi.path[len(store_path) :].lstrip("/")
                filename = rel_path.split("/")[-1]

                if filename in _METADATA_FILENAMES:
                    with self._filesystem.open_input_file(fi.path) as f:
                        metadata_bytes[rel_path] = bytes(f.read())
                else:
                    path_lookup[rel_path] = fi.path

            is_zarr_v3 = "zarr.json" in metadata_bytes or any(
                k.endswith("/zarr.json") for k in metadata_bytes
            )

            self._store_metadata[store_path] = (metadata_bytes, path_lookup, is_zarr_v3)

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        import zarr

        read_tasks = []
        for store_path, (
            metadata_bytes,
            path_lookup,
            is_zarr_v3,
        ) in self._store_metadata.items():
            store_dict = {
                k: zarr.core.buffer.cpu.Buffer.from_bytes(v)
                for k, v in metadata_bytes.items()
            }
            store = zarr.open(zarr.storage.MemoryStore(store_dict=store_dict), mode="r")

            for array_name, arr in _get_arrays(store):
                # Get dimension separator for zarr v2 (default ".")
                dim_sep = getattr(arr.metadata, "dimension_separator", ".")
                for chunk_idx in _get_chunk_indices(arr):
                    chunk_rel_path = _get_chunk_path(
                        array_name, chunk_idx, is_zarr_v3, dim_sep
                    )
                    chunk_abs_path = path_lookup.get(chunk_rel_path)

                    if chunk_abs_path is None:
                        continue  # Chunk file missing (sparse array or empty chunk)

                    chunk_shape = tuple(
                        min(c, s - i * c)
                        for i, c, s in zip(chunk_idx, arr.chunks, arr.shape)
                    )
                    chunk_size = int(np.prod(chunk_shape) * arr.dtype.itemsize)

                    read_tasks.append(
                        ReadTask(
                            _create_read_fn(
                                chunk_abs_path,
                                chunk_rel_path,
                                array_name,
                                chunk_idx,
                                arr.chunks,
                                arr.shape,
                                metadata_bytes,
                                self._filesystem,
                            ),
                            BlockMetadata(
                                num_rows=1,
                                size_bytes=chunk_size,
                                input_files=[store_path],
                                exec_stats=None,
                            ),
                        )
                    )

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        import zarr

        total = 0
        for metadata_bytes, _, _ in self._store_metadata.values():
            store_dict = {
                k: zarr.core.buffer.cpu.Buffer.from_bytes(v)
                for k, v in metadata_bytes.items()
            }
            store = zarr.open(zarr.storage.MemoryStore(store_dict=store_dict), mode="r")

            for _, arr in _get_arrays(store):
                total += arr.nbytes

        return total


def _get_chunk_path(
    array_name: str,
    chunk_idx: Tuple[int, ...],
    is_zarr_v3: bool,
    dimension_separator: str = ".",
) -> str:
    """Compute the chunk file path for a given array and chunk index."""
    if is_zarr_v3:
        # zarr v3: {array_name}/c/{dim0}/{dim1}/...
        chunk_part = "c/" + "/".join(str(i) for i in chunk_idx)
    else:
        # zarr v2: {array_name}/{dim0}{sep}{dim1}... (separator from .zarray metadata)
        chunk_part = dimension_separator.join(str(i) for i in chunk_idx)

    if array_name:
        return f"{array_name}/{chunk_part}"
    return chunk_part


def _get_arrays(
    store: Union["zarr.Array", "zarr.Group"]
) -> List[Tuple[str, "zarr.Array"]]:
    """Recursively get all arrays from a zarr store."""
    import zarr

    if isinstance(store, zarr.Array):
        return [("", store)]

    arrays = []
    for name, item in store.members():
        if isinstance(item, zarr.Array):
            arrays.append((name, item))
        else:
            for sub_name, arr in _get_arrays(item):
                arrays.append((f"{name}/{sub_name}" if sub_name else name, arr))
    return arrays


def _get_chunk_indices(arr: "zarr.Array") -> List[Tuple[int, ...]]:
    """Get all chunk indices for an array."""
    chunks_per_dim = [(s + c - 1) // c for s, c in zip(arr.shape, arr.chunks)]
    return list(itertools.product(*[range(n) for n in chunks_per_dim]))


def _create_read_fn(
    chunk_abs_path: str,
    chunk_rel_path: str,
    array_name: str,
    chunk_idx: Tuple[int, ...],
    chunks: Tuple[int, ...],
    shape: Tuple[int, ...],
    metadata_bytes: Dict[str, bytes],
    filesystem: "pafs.FileSystem",
) -> Callable[[], Iterable[Block]]:
    def read_fn() -> Iterable[Block]:
        import zarr

        with filesystem.open_input_file(chunk_abs_path) as f:
            chunk_bytes = bytes(f.read())

        all_bytes = {**metadata_bytes, chunk_rel_path: chunk_bytes}
        store_dict = {
            k: zarr.core.buffer.cpu.Buffer.from_bytes(v) for k, v in all_bytes.items()
        }
        store = zarr.open(zarr.storage.MemoryStore(store_dict=store_dict), mode="r")

        arr = store[array_name] if array_name else store
        slices = tuple(
            slice(i * c, min((i + 1) * c, s))
            for i, c, s in zip(chunk_idx, chunks, shape)
        )
        chunk_data = arr[slices]

        yield pa.table(
            {
                "array_name": [array_name],
                "data": [chunk_data.tobytes()],
                "shape": [list(chunk_data.shape)],
                "dtype": [str(chunk_data.dtype)],
                "chunk_index": [list(chunk_idx)],
            }
        )

    return read_fn
