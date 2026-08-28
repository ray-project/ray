from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator, Optional

import numpy as np

from ray.data._internal.util import _check_import, iterate_with_retry
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import (
    FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD,
    PATHS_PER_FILE_SIZE_FETCH_TASK,
    FileBasedDatasource,
    _add_partitions,
    _shuffle_file_metadata,
    _unwrap_s3_serialization_workaround,
    _wrap_s3_serialization_workaround,
)
from ray.data.datasource.file_meta_provider import _fetch_metadata_parallel
from ray.data.datasource.partitioning import PathPartitionParser

if TYPE_CHECKING:
    from ray.data.context import DataContext


@dataclass(frozen=True)
class _HDF5FileMetadata:
    path: str
    shape: tuple[int, ...]
    dtype: str
    itemsize: Optional[int]
    is_string: bool
    is_vlen_array: bool
    chunk_rows: Optional[int]

    @property
    def num_rows(self) -> int:
        return 1 if not self.shape else self.shape[0]

    @property
    def row_size_bytes(self) -> Optional[int]:
        if self.itemsize is None:
            return None
        trailing_items = math.prod(self.shape[1:]) if self.shape else 1
        return trailing_items * self.itemsize


@dataclass(frozen=True)
class _HDF5Segment:
    metadata: _HDF5FileMetadata
    start: int
    stop: int

    @property
    def num_rows(self) -> int:
        return self.stop - self.start


def _to_arrow_nested_list(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        if value.dtype != object:
            return value
        return [_to_arrow_nested_list(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def _read_segment(hdf5_dataset, segment: _HDF5Segment) -> np.ndarray:
    if segment.metadata.is_string:
        hdf5_dataset = hdf5_dataset.asstr()
    if not segment.metadata.shape:
        value = np.asarray(hdf5_dataset[()])
        if segment.metadata.is_vlen_array:
            data = np.empty(1, dtype=object)
            data[0] = value
        else:
            data = value.reshape(1)
    else:
        data = np.asarray(hdf5_dataset[segment.start : segment.stop])
    if not data.dtype.isnative:
        data = data.astype(data.dtype.newbyteorder("="), copy=False)
    return data


def _read_segments(
    filesystem, dataset: str, segments: list[_HDF5Segment]
) -> Iterator[tuple[_HDF5Segment, np.ndarray]]:
    import h5py

    segment_index = 0
    while segment_index < len(segments):
        path = segments[segment_index].metadata.path
        with (
            filesystem.open_input_file(path) as file_obj,
            h5py.File(file_obj, "r") as file,
        ):
            hdf5_dataset = file[dataset]
            while (
                segment_index < len(segments)
                and segments[segment_index].metadata.path == path
            ):
                segment = segments[segment_index]
                yield segment, _read_segment(hdf5_dataset, segment)
                segment_index += 1


def _inspect_hdf5_file(filesystem, dataset_path: str, path: str) -> _HDF5FileMetadata:
    import h5py

    with filesystem.open_input_file(path) as file_obj:
        with h5py.File(file_obj, "r") as file:
            link = file.get(dataset_path, getlink=True)
            if link is None:
                raise ValueError(
                    f"Dataset {dataset_path!r} was not found in HDF5 file {path!r}."
                )
            if isinstance(link, h5py.ExternalLink):
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses an "
                    "external link, which is not supported."
                )
            dataset = file[dataset_path]
            if not isinstance(dataset, h5py.Dataset):
                raise ValueError(
                    f"HDF5 path {dataset_path!r} in file {path!r} is a "
                    "group, not a dataset."
                )
            if dataset.external:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses "
                    "external storage, which is not supported."
                )
            if dataset.is_virtual:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} is a virtual "
                    "dataset, which is not supported."
                )
            if dataset.dtype.fields is not None:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses a "
                    "compound dtype, which is not supported. Select a dataset "
                    "with a scalar dtype or split the fields into separate datasets."
                )
            if h5py.check_ref_dtype(dataset.dtype) is not None:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses an object "
                    "or region reference dtype, which is not supported."
                )
            if dataset.shape is None:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} has a null dataspace, which is "
                    f"not supported in file {path!r}."
                )

            string_info = h5py.check_string_dtype(dataset.dtype)
            is_string = string_info is not None
            vlen_dtype = h5py.check_vlen_dtype(dataset.dtype)
            is_vlen_array = vlen_dtype is not None and not is_string
            vlen_base_dtype = np.dtype(vlen_dtype) if is_vlen_array else None
            if vlen_base_dtype is not None and vlen_base_dtype.fields is not None:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses a "
                    "variable-length compound dtype, which is not supported."
                )
            if vlen_base_dtype is not None and vlen_base_dtype.subdtype is not None:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses a "
                    "variable-length subarray dtype, which is not supported."
                )
            if vlen_base_dtype is not None and not vlen_base_dtype.isnative:
                raise ValueError(
                    f"HDF5 dataset {dataset_path!r} in file {path!r} uses a "
                    "variable-length dtype with a non-native byte order, which "
                    "is not supported."
                )
            if is_string:
                canonical_dtype = "str"
            elif vlen_base_dtype is not None:
                canonical_dtype = f"vlen:{vlen_base_dtype.newbyteorder('=').name}"
            else:
                canonical_dtype = str(np.dtype(dataset.dtype).newbyteorder("="))

            return _HDF5FileMetadata(
                path=path,
                shape=tuple(dataset.shape),
                dtype=canonical_dtype,
                itemsize=(
                    None if is_vlen_array or is_string else dataset.dtype.itemsize
                ),
                is_string=is_string,
                is_vlen_array=is_vlen_array,
                chunk_rows=(dataset.chunks[0] if dataset.chunks else None),
            )


class HDF5Datasource(FileBasedDatasource):
    """Reads a dataset from one or more HDF5 files."""

    _FILE_EXTENSIONS = ["h5", "hdf5", "hdf"]

    def __init__(self, paths, *, dataset: str, **file_based_datasource_kwargs):
        _check_import(self, module="h5py", package="h5py")
        if not isinstance(dataset, str) or not dataset.strip("/"):
            raise ValueError(
                f"dataset must be a non-empty string naming an HDF5 dataset, "
                f"got {dataset!r}"
            )
        self.dataset = dataset.strip("/")
        super().__init__(paths, **file_based_datasource_kwargs)
        self._hdf5_metadata = self._inspect_files()
        self._validate_metadata()

    def _inspect_files(self) -> list[_HDF5FileMetadata]:
        paths = self._paths()
        if (
            len(paths) <= FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD
            or not self.supports_distributed_reads
        ):
            return [
                _inspect_hdf5_file(self._filesystem, self.dataset, path)
                for path in paths
            ]

        filesystem = _wrap_s3_serialization_workaround(self._filesystem)
        dataset = self.dataset

        def inspect_batch(paths: list[str]) -> list[_HDF5FileMetadata]:
            fs = _unwrap_s3_serialization_workaround(filesystem)
            return [_inspect_hdf5_file(fs, dataset, path) for path in paths]

        return list(
            _fetch_metadata_parallel(
                paths, inspect_batch, PATHS_PER_FILE_SIZE_FETCH_TASK
            )
        )

    def estimate_inmemory_data_size(self) -> Optional[int]:
        row_sizes = [metadata.row_size_bytes for metadata in self._hdf5_metadata]
        if any(row_size is None for row_size in row_sizes):
            return None
        return sum(
            metadata.num_rows * row_size
            for metadata, row_size in zip(self._hdf5_metadata, row_sizes)
            if row_size is not None
        )

    def _validate_metadata(self) -> None:
        if not self._hdf5_metadata:
            return
        expected = self._hdf5_metadata[0]
        expected_row_shape = expected.shape[1:] if expected.shape else ()
        for metadata in self._hdf5_metadata[1:]:
            row_shape = metadata.shape[1:] if metadata.shape else ()
            if row_shape != expected_row_shape:
                raise ValueError(
                    "All HDF5 datasets must have the same per-row shape. "
                    f"{expected.path!r} has {expected_row_shape}, but "
                    f"{metadata.path!r} has {row_shape}."
                )
            if metadata.dtype != expected.dtype:
                raise ValueError(
                    "All HDF5 datasets must have the same dtype. "
                    f"{expected.path!r} has {expected.dtype}, but "
                    f"{metadata.path!r} has {metadata.dtype}."
                )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> list[ReadTask]:
        if parallelism <= 0:
            raise ValueError(f"parallelism must be positive, got {parallelism}")

        execution_idx = data_context._execution_idx if data_context is not None else 0
        _, metadata = _shuffle_file_metadata(
            [item.path for item in self._hdf5_metadata],
            self._hdf5_metadata,
            self._shuffle,
            execution_idx,
        )
        target_max_block_size = (
            data_context.target_max_block_size
            if data_context is not None
            else self._data_context.target_max_block_size
        )
        segments_by_task = self._plan_segments(
            parallelism, metadata, target_max_block_size
        )
        filesystem = _wrap_s3_serialization_workaround(self._filesystem)
        retry_match = (
            data_context.retried_io_errors
            if data_context is not None
            else self._data_context.retried_io_errors
        )
        dataset = self.dataset
        partitioning = self._partitioning
        include_paths = self._include_paths

        read_tasks = []
        for segments in segments_by_task:
            num_rows = sum(segment.num_rows for segment in segments)
            row_sizes = [segment.metadata.row_size_bytes for segment in segments]
            size_bytes = (
                None
                if any(row_size is None for row_size in row_sizes)
                else sum(
                    segment.num_rows * row_size
                    for segment, row_size in zip(segments, row_sizes)
                    if row_size is not None
                )
            )

            def read_fn(segments=segments):
                fs = _unwrap_s3_serialization_workaround(filesystem)
                remaining = per_task_row_limit
                bounded_segments = []
                for original_segment in segments:
                    if remaining is not None and remaining <= 0:
                        break
                    segment = original_segment
                    if remaining is not None and segment.num_rows > remaining:
                        segment = _HDF5Segment(
                            segment.metadata,
                            segment.start,
                            segment.start + remaining,
                        )
                    bounded_segments.append(segment)
                    if remaining is not None:
                        remaining -= segment.num_rows

                for segment, data in iterate_with_retry(
                    lambda: _read_segments(fs, dataset, bounded_segments),
                    description="read HDF5 dataset segments",
                    match=retry_match,
                ):
                    if segment.metadata.is_string or segment.metadata.is_vlen_array:
                        import pyarrow as pa

                        batch_data = pa.array(_to_arrow_nested_list(data))
                    else:
                        batch_data = data
                    block = BlockAccessor.batch_to_block({"data": batch_data})
                    if partitioning is not None:
                        partitions = PathPartitionParser(partitioning)(
                            segment.metadata.path
                        )
                        if partitions:
                            block = _add_partitions(block, partitions)
                    if include_paths:
                        block = BlockAccessor.for_block(block).fill_column(
                            "path", segment.metadata.path
                        )
                    yield block

            read_tasks.append(
                ReadTask(
                    read_fn,
                    BlockMetadata(
                        num_rows=num_rows,
                        size_bytes=size_bytes,
                        input_files=tuple(
                            dict.fromkeys(segment.metadata.path for segment in segments)
                        ),
                        exec_stats=None,
                    ),
                    per_task_row_limit=per_task_row_limit,
                )
            )
        return read_tasks

    def _plan_segments(
        self,
        parallelism: int,
        metadata: Optional[list[_HDF5FileMetadata]] = None,
        target_max_block_size: Optional[int] = None,
    ) -> list[list[_HDF5Segment]]:
        metadata = self._hdf5_metadata if metadata is None else metadata
        files = [item for item in metadata if item.num_rows]
        total_rows = sum(metadata.num_rows for metadata in files)
        if total_rows == 0:
            return []

        target_rows = math.ceil(total_rows / min(parallelism, total_rows))
        needs_byte_bounded_segments = target_max_block_size is not None and any(
            metadata.row_size_bytes is not None
            and target_rows * metadata.row_size_bytes > target_max_block_size
            for metadata in files
        )
        if needs_byte_bounded_segments or any(
            metadata.chunk_rows is not None for metadata in files
        ):
            return self._plan_bounded_segments(
                parallelism, files, total_rows, target_max_block_size
            )

        num_tasks = min(parallelism, total_rows)
        task_size, remainder = divmod(total_rows, num_tasks)
        task_sizes = [
            task_size + (task_index < remainder) for task_index in range(num_tasks)
        ]

        tasks: list[list[_HDF5Segment]] = []
        file_index = 0
        file_row = 0
        for size in task_sizes:
            task_segments = []
            remaining = size
            while remaining:
                file_metadata = files[file_index]
                stop = min(file_metadata.num_rows, file_row + remaining)
                task_segments.append(_HDF5Segment(file_metadata, file_row, stop))
                consumed = stop - file_row
                remaining -= consumed
                file_row = stop
                if file_row == file_metadata.num_rows:
                    file_index += 1
                    file_row = 0
            tasks.append(task_segments)
        return tasks

    @staticmethod
    def _plan_bounded_segments(
        parallelism: int,
        files: list[_HDF5FileMetadata],
        total_rows: int,
        target_max_block_size: Optional[int],
    ) -> list[list[_HDF5Segment]]:
        """Plan row ranges, bounding fixed-width segments by bytes.

        Whole HDF5 chunks are preserved when they fit.
        """
        target_segment_rows = math.ceil(total_rows / min(parallelism, total_rows))
        candidate_segments = []
        for metadata in files:
            segment_rows = target_segment_rows
            if (
                target_max_block_size is not None
                and metadata.row_size_bytes is not None
                and metadata.row_size_bytes > 0
            ):
                segment_rows = min(
                    segment_rows,
                    max(1, target_max_block_size // metadata.row_size_bytes),
                )
            chunk_size_bytes = (
                metadata.chunk_rows * metadata.row_size_bytes
                if metadata.chunk_rows is not None
                and metadata.row_size_bytes is not None
                else None
            )
            if metadata.chunk_rows is not None and (
                target_max_block_size is None
                or (
                    chunk_size_bytes is not None
                    and chunk_size_bytes <= target_max_block_size
                )
            ):
                chunks_per_segment = max(
                    1, math.ceil(target_segment_rows / metadata.chunk_rows)
                )
                if target_max_block_size is not None and chunk_size_bytes:
                    chunks_per_segment = min(
                        chunks_per_segment,
                        max(1, target_max_block_size // chunk_size_bytes),
                    )
                segment_rows = chunks_per_segment * metadata.chunk_rows
            for start in range(0, metadata.num_rows, segment_rows):
                candidate_segments.append(
                    _HDF5Segment(
                        metadata, start, min(start + segment_rows, metadata.num_rows)
                    )
                )

        num_tasks = min(parallelism, len(candidate_segments))
        tasks = []
        candidate_index = 0
        rows_remaining = total_rows
        for task_index in range(num_tasks):
            tasks_remaining = num_tasks - task_index
            target_task_rows = math.ceil(rows_remaining / tasks_remaining)
            task_segments = []
            task_rows = 0
            # Keep candidate segments intact and leave one for each remaining task.
            while candidate_index < len(candidate_segments):
                candidates_remaining = len(candidate_segments) - candidate_index
                if task_segments and (
                    task_rows >= target_task_rows
                    or candidates_remaining == tasks_remaining - 1
                ):
                    break
                segment = candidate_segments[candidate_index]
                task_segments.append(segment)
                task_rows += segment.num_rows
                candidate_index += 1
            tasks.append(task_segments)
            rows_remaining -= task_rows
        return tasks
