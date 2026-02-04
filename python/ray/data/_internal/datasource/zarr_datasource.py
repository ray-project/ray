"""Zarr datasource for Ray Data."""

import itertools
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    import pyarrow.fs
    import zarr

    from ray.data.context import DataContext


class ZarrDatasource(Datasource):
    """Datasource for reading Zarr arrays.

    Each Zarr chunk becomes one block in the Dataset. Supports both single
    arrays and groups containing multiple arrays.
    """

    def __init__(
        self,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    ):
        _check_import(self, module="zarr", package="zarr")
        self._paths, self._filesystem = _resolve_paths_and_filesystem(paths, filesystem)

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        import zarr

        read_tasks = []
        for path in self._paths:
            zarr_path = _get_zarr_path(path, self._filesystem)
            store = zarr.open(zarr_path, mode="r")
            for array_name, arr in _get_arrays(store):
                for chunk_idx in _get_chunk_indices(arr):
                    # Estimate size of this chunk
                    chunk_shape = tuple(
                        min(c, s - i * c)
                        for i, c, s in zip(chunk_idx, arr.chunks, arr.shape)
                    )
                    chunk_size = int(np.prod(chunk_shape) * arr.dtype.itemsize)
                    metadata = BlockMetadata(
                        num_rows=1,
                        size_bytes=chunk_size,
                        input_files=[path],
                        exec_stats=None,
                    )
                    read_tasks.append(
                        ReadTask(
                            _create_read_fn(zarr_path, array_name, chunk_idx),
                            metadata,
                        )
                    )
        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        import zarr

        total = 0
        for path in self._paths:
            zarr_path = _get_zarr_path(path, self._filesystem)
            store = zarr.open(zarr_path, mode="r")
            for _, arr in _get_arrays(store):
                total += arr.nbytes
        return total


def _get_zarr_path(path: str, filesystem: Optional["pyarrow.fs.FileSystem"]) -> str:
    """Build a zarr-compatible path from the resolved path and filesystem.

    The `_resolve_paths_and_filesystem` function strips URI schemes from paths.
    Since zarr uses fsspec for remote storage and expects full URIs, we
    reconstruct the URI from the filesystem type.
    """
    if filesystem is None:
        return path
    fs_type = filesystem.type_name
    if fs_type == "s3":
        return f"s3://{path}"
    elif fs_type == "gcs":
        return f"gs://{path}"
    return path


def _get_arrays(
    store: Union["zarr.Array", "zarr.Group"],
) -> List[Tuple[str, "zarr.Array"]]:
    """Recursively get all arrays from a Zarr store."""
    import zarr

    if isinstance(store, zarr.Array):
        return [("", store)]
    arrays = []
    for name, arr in store.arrays():
        arrays.append((name, arr))
    for name, group in store.groups():
        for sub_name, arr in _get_arrays(group):
            arrays.append((f"{name}/{sub_name}" if sub_name else name, arr))
    return arrays


def _get_chunk_indices(arr: "zarr.Array") -> List[Tuple[int, ...]]:
    """Get all chunk indices for an array."""
    chunks_per_dim = [(s + c - 1) // c for s, c in zip(arr.shape, arr.chunks)]
    return list(itertools.product(*[range(n) for n in chunks_per_dim]))


def _create_read_fn(zarr_path: str, array_name: str, chunk_idx: Tuple[int, ...]):
    """Create a read function for a specific chunk."""

    def read_fn() -> Iterable[Block]:
        import zarr

        store = zarr.open(zarr_path, mode="r")
        arr = store[array_name] if array_name else store
        slices = tuple(
            slice(i * c, min((i + 1) * c, s))
            for i, c, s in zip(chunk_idx, arr.chunks, arr.shape)
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
